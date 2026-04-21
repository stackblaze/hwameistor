package rwxpvc

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apisv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

const (
	// RWXProvisionerName is the StorageClass provisioner that marks a PVC
	// as belonging to this controller.
	RWXProvisionerName = "lvm.hwameistor.io/rwx"
	// RWXBackingStorageClassParam is the SC parameter naming the RWO
	// HwameiStor storage class that will back the RWX volume.
	RWXBackingStorageClassParam = "lvm.hwameistor.io/backing-storage-class"
	// RWXSquashParam is the optional SC parameter controlling the NFS
	// squash mount option (e.g. "all_squash", "no_root_squash").
	RWXSquashParam = "lvm.hwameistor.io/nfs-squash"
	// RWXFinalizer is the finalizer added to the user PVC so we can clean
	// up the backing chain before the PVC is fully deleted.
	RWXFinalizer = "hwameistor.io/rwx-pvc"
	// RWXExportAnnotation tells the node-side reactor which user PVC a
	// given LocalVolume is exporting over NFS.
	RWXExportAnnotation = "hwameistor.io/rwx-export"
	// NFSCSIDriver is the csi.k8s.io driver name for the NFS CSI driver.
	NFSCSIDriver = "nfs.csi.k8s.io"
	// BackingPVCSuffix is appended to the user PVC name to form the name
	// of the backing RWO PVC.
	BackingPVCSuffix = "-rwx-backing"
	// MirrorPVPrefix is prepended to the user PVC UID to form the name of
	// the mirror NFS PV.
	MirrorPVPrefix = "hwameistor-rwx-"
	// ExportPathPrefix is the on-node directory under which per-volume
	// export paths are created.
	ExportPathPrefix = "/srv/exports/"

	// requeueAfterBacking is how long we wait before re-checking whether
	// the backing PVC has become Bound so we can annotate its LocalVolume.
	requeueAfterBacking = 5 * time.Second
)

// Reconciler reconciles user-facing RWX PersistentVolumeClaims provisioned
// by lvm.hwameistor.io/rwx into an NFS-exposed HwameiStor chain.
type Reconciler struct {
	Client client.Client
	// Scheme is not currently used for OwnerReferences (cross-namespace
	// and cluster/namespace-scope boundaries make that awkward) but
	// downstream helpers may want it, so we keep it on the struct.
	Scheme *runtime.Scheme
}

// SetupWithManager wires the reconciler into the manager, watching
// PersistentVolumeClaims only. We intentionally do not watch the backing
// resources: they are deterministic from the user PVC so an owner reference
// or periodic resync would just produce duplicate work.
func (r *Reconciler) SetupWithManager(mgr manager.Manager) error {
	c, err := controller.New("rwxpvc-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	return c.Watch(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, &handler.EnqueueRequestForObject{})
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.WithField("pvc", req.NamespacedName.String())

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, req.NamespacedName, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	match, sc, err := r.isRWXPVC(ctx, pvc)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !match {
		// Not ours. If we somehow left a finalizer on a non-matching PVC
		// (e.g. SC changed out from under us), still clean it up.
		if containsFinalizer(pvc, RWXFinalizer) && pvc.DeletionTimestamp == nil {
			return reconcile.Result{}, r.removeFinalizer(ctx, pvc)
		}
		return reconcile.Result{}, nil
	}

	if pvc.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, pvc)
	}

	if !containsFinalizer(pvc, RWXFinalizer) {
		pvc.Finalizers = append(pvc.Finalizers, RWXFinalizer)
		if err := r.Client.Update(ctx, pvc); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}

	cfg, err := r.buildConfig(pvc, sc)
	if err != nil {
		logger.WithError(err).Error("invalid RWX PVC/SC configuration")
		return reconcile.Result{}, err
	}

	if err := r.ensureBackingPVC(ctx, cfg); err != nil {
		return reconcile.Result{}, fmt.Errorf("ensure backing PVC: %w", err)
	}

	svc, err := r.ensureService(ctx, cfg)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("ensure service: %w", err)
	}

	if svc.Spec.ClusterIP == "" {
		// Service hasn't been assigned a ClusterIP yet. Requeue rather
		// than build a PV with an empty server address.
		return reconcile.Result{RequeueAfter: requeueAfterBacking}, nil
	}

	if err := r.ensureMirrorPV(ctx, cfg, svc); err != nil {
		return reconcile.Result{}, fmt.Errorf("ensure mirror PV: %w", err)
	}

	if requeue, err := r.annotateLocalVolume(ctx, cfg); err != nil {
		return reconcile.Result{}, err
	} else if requeue {
		return reconcile.Result{RequeueAfter: requeueAfterBacking}, nil
	}

	return reconcile.Result{}, nil
}

// isRWXPVC decides whether this PVC belongs to us. Both conditions must
// hold: the SC's provisioner must be ours, and the PVC must actually
// request RWX — otherwise a misconfigured RWO PVC would end up behind an
// NFS proxy for no reason.
func (r *Reconciler) isRWXPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, *storagev1.StorageClass, error) {
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return false, nil, nil
	}
	rwx := false
	for _, m := range pvc.Spec.AccessModes {
		if m == corev1.ReadWriteMany {
			rwx = true
			break
		}
	}
	if !rwx {
		return false, nil, nil
	}

	sc := &storagev1.StorageClass{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil, nil
		}
		return false, nil, err
	}
	if sc.Provisioner != RWXProvisionerName {
		return false, nil, nil
	}
	return true, sc, nil
}

func (r *Reconciler) buildConfig(pvc *corev1.PersistentVolumeClaim, sc *storagev1.StorageClass) (rwxConfig, error) {
	backingSC := sc.Parameters[RWXBackingStorageClassParam]
	if backingSC == "" {
		return rwxConfig{}, fmt.Errorf("storageclass %q is missing required parameter %q", sc.Name, RWXBackingStorageClassParam)
	}
	capacity, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok {
		return rwxConfig{}, fmt.Errorf("pvc %s/%s has no storage request", pvc.Namespace, pvc.Name)
	}
	return rwxConfig{
		UserPVCName:         pvc.Name,
		UserPVCNamespace:    pvc.Namespace,
		UserPVCUID:          pvc.UID,
		BackingStorageClass: backingSC,
		Capacity:            capacity,
		Squash:              sc.Parameters[RWXSquashParam],
	}, nil
}

// ensureBackingPVC creates the backing RWO PVC if it does not exist, and
// only grows the capacity on update. We never shrink — that would be
// unsafe — and we never change the storage class once bound.
func (r *Reconciler) ensureBackingPVC(ctx context.Context, cfg rwxConfig) error {
	desired := BuildBackingPVC(cfg)

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return r.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	desiredCap := cfg.Capacity
	currentCap := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if desiredCap.Cmp(currentCap) > 0 {
		if existing.Spec.Resources.Requests == nil {
			existing.Spec.Resources.Requests = corev1.ResourceList{}
		}
		existing.Spec.Resources.Requests[corev1.ResourceStorage] = desiredCap
		return r.Client.Update(ctx, existing)
	}
	return nil
}

// ensureService creates the Service if it doesn't exist and returns the
// up-to-date Service (including any ClusterIP assigned by the apiserver).
// We never mutate an existing Service because its selector/ports matter to
// the external reactor and should only change out-of-band.
func (r *Reconciler) ensureService(ctx context.Context, cfg rwxConfig) (*corev1.Service, error) {
	desired := BuildService(cfg)

	existing := &corev1.Service{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Client.Create(ctx, desired); err != nil {
			return nil, err
		}
		// Re-read to pick up a cluster-assigned ClusterIP.
		if err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err != nil {
		return nil, err
	}
	return existing, nil
}

// ensureMirrorPV creates the NFS PV if missing, and grows its capacity if
// the user expanded their RWX PVC. We intentionally do not mutate the
// CSI source or ClaimRef of an existing PV — that would break binding.
func (r *Reconciler) ensureMirrorPV(ctx context.Context, cfg rwxConfig, svc *corev1.Service) error {
	desired := BuildMirrorPV(cfg, svc)

	existing := &corev1.PersistentVolume{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name}, existing)
	if apierrors.IsNotFound(err) {
		return r.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	desiredCap := cfg.Capacity
	currentCap := existing.Spec.Capacity[corev1.ResourceStorage]
	if desiredCap.Cmp(currentCap) > 0 {
		if existing.Spec.Capacity == nil {
			existing.Spec.Capacity = corev1.ResourceList{}
		}
		existing.Spec.Capacity[corev1.ResourceStorage] = desiredCap
		return r.Client.Update(ctx, existing)
	}
	return nil
}

// annotateLocalVolume finds the LocalVolume that backs the RWO PVC and
// stamps the rwx-export annotation on it. Returns (requeue, err). A
// requeue result means "not an error, just not ready yet".
//
// The LocalVolume's name is the same as the PV name produced by the CSI
// driver (see genLocalVolumeFromRequest in pkg/local-storage/member/csi),
// so we look up the backing PVC -> its PV name -> the LocalVolume with
// that name. As a fallback (e.g. if the naming contract ever changes),
// we also try listing LocalVolumes by PVC reference.
func (r *Reconciler) annotateLocalVolume(ctx context.Context, cfg rwxConfig) (bool, error) {
	backing := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: BackingPVCName(cfg.UserPVCName), Namespace: cfg.UserPVCNamespace}, backing); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if backing.Spec.VolumeName == "" {
		// Not bound yet; caller should requeue.
		return true, nil
	}

	lv := &apisv1alpha1.LocalVolume{}
	getErr := r.Client.Get(ctx, types.NamespacedName{Name: backing.Spec.VolumeName}, lv)
	if apierrors.IsNotFound(getErr) {
		// Fallback: scan by PVC reference. This is best-effort and only
		// matters if the naming convention has drifted.
		list := &apisv1alpha1.LocalVolumeList{}
		if err := r.Client.List(ctx, list); err != nil {
			return false, err
		}
		var found *apisv1alpha1.LocalVolume
		for i := range list.Items {
			item := &list.Items[i]
			if item.Spec.PersistentVolumeClaimName == backing.Name &&
				item.Spec.PersistentVolumeClaimNamespace == backing.Namespace {
				found = item
				break
			}
		}
		if found == nil {
			// Not observable yet; benign — requeue.
			return true, nil
		}
		lv = found
	} else if getErr != nil {
		return false, getErr
	}

	if lv.Annotations[RWXExportAnnotation] == string(cfg.UserPVCUID) {
		return false, nil
	}
	patched := lv.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[RWXExportAnnotation] = string(cfg.UserPVCUID)
	return false, r.Client.Update(ctx, patched)
}

// reconcileDelete tears down the backing chain in reverse order: PV first
// (so no client can bind or stay bound after we delete the Service/PVC),
// then Service, then backing PVC (which triggers normal HwameiStor
// teardown of the LocalVolume), and finally the finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (reconcile.Result, error) {
	if !containsFinalizer(pvc, RWXFinalizer) {
		return reconcile.Result{}, nil
	}

	// 1. mirror PV
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: MirrorPVName(pvc.UID)},
	}
	if err := r.Client.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	// 2. Service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName(pvc.Name),
			Namespace: pvc.Namespace,
		},
	}
	if err := r.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	// 3. backing PVC
	backing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BackingPVCName(pvc.Name),
			Namespace: pvc.Namespace,
		},
	}
	if err := r.Client.Delete(ctx, backing); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	// 4. drop finalizer
	return reconcile.Result{}, r.removeFinalizer(ctx, pvc)
}

func (r *Reconciler) removeFinalizer(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	if !containsFinalizer(pvc, RWXFinalizer) {
		return nil
	}
	out := pvc.Finalizers[:0]
	for _, f := range pvc.Finalizers {
		if f != RWXFinalizer {
			out = append(out, f)
		}
	}
	pvc.Finalizers = out
	return r.Client.Update(ctx, pvc)
}

func containsFinalizer(pvc *corev1.PersistentVolumeClaim, name string) bool {
	for _, f := range pvc.Finalizers {
		if f == name {
			return true
		}
	}
	return false
}
