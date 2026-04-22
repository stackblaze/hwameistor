// Package rwxpvc reconciles user-facing RWX PersistentVolumeClaims
// provisioned by lvm.hwameistor.io/rwx into an NFS-exposed HwameiStor
// chain: backing RWO PVC → selector-less Service → mirror NFS PV.
// The node-side nfs-reactor picks up the chain via an annotation on the
// LocalVolume and serves it through Ganesha.
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
	"github.com/hwameistor/hwameistor/pkg/local-storage/member/rwx"
)

const (
	// RWXProvisionerName, RWXExportAnnotation re-exported from rwx.* for
	// backwards compatibility with callers that imported them directly.
	RWXProvisionerName  = rwx.ProvisionerName
	RWXExportAnnotation = rwx.ExportAnnotation

	RWXBackingStorageClassParam = "lvm.hwameistor.io/backing-storage-class"
	RWXSquashParam              = "lvm.hwameistor.io/nfs-squash"
	RWXFinalizer                = "hwameistor.io/rwx-pvc"
	NFSCSIDriver                = "nfs.csi.k8s.io"
	BackingPVCSuffix            = "-rwx-backing"
	MirrorPVPrefix              = "hwameistor-rwx-"
	ExportPathPrefix            = rwx.ExportRootDefault + "/"

	requeueAfterBacking = 5 * time.Second
)

type Reconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func (r *Reconciler) SetupWithManager(mgr manager.Manager) error {
	c, err := controller.New("rwxpvc-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	return c.Watch(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, &handler.EnqueueRequestForObject{})
}

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
		// Drop a stale finalizer if the SC was changed out from under us.
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

// isRWXPVC: the SC's provisioner must be ours AND the PVC must actually
// request RWX, else a misconfigured RWO PVC would end up behind an NFS
// proxy for no reason.
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

// ensureBackingPVC creates the RWO backing PVC with a selected-node
// annotation. HwameiStor's CSI CreateVolume requires a topology hint,
// which normally comes from WaitForFirstConsumer — but no user pod ever
// consumes the backing PVC directly (the NFS PV does), so WFFC would
// deadlock. We pick any Ready LocalStorageNode ourselves.
func (r *Reconciler) ensureBackingPVC(ctx context.Context, cfg rwxConfig) error {
	desired := BuildBackingPVC(cfg)

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		node, perr := r.pickStorageNode(ctx)
		if perr != nil {
			return perr
		}
		if node != "" {
			if desired.Annotations == nil {
				desired.Annotations = map[string]string{}
			}
			desired.Annotations[rwx.AnnSelectedNode] = node
		}
		return r.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Still Pending with no hint (older reconciler or user-created PVC):
	// stamp it now so provisioning can proceed.
	if existing.Status.Phase != corev1.ClaimBound &&
		existing.Annotations[rwx.AnnSelectedNode] == "" {
		node, perr := r.pickStorageNode(ctx)
		if perr != nil {
			return perr
		}
		if node != "" {
			patch := existing.DeepCopy()
			if patch.Annotations == nil {
				patch.Annotations = map[string]string{}
			}
			patch.Annotations[rwx.AnnSelectedNode] = node
			if err := r.Client.Update(ctx, patch); err != nil {
				return err
			}
		}
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

// pickStorageNode returns the lex-first Ready LocalStorageNode so repeated
// calls are stable.
func (r *Reconciler) pickStorageNode(ctx context.Context) (string, error) {
	list := &apisv1alpha1.LocalStorageNodeList{}
	if err := r.Client.List(ctx, list); err != nil {
		return "", fmt.Errorf("list LocalStorageNodes: %w", err)
	}
	best := ""
	for i := range list.Items {
		n := &list.Items[i]
		if n.Status.State != apisv1alpha1.NodeStateReady {
			continue
		}
		if best == "" || n.Name < best {
			best = n.Name
		}
	}
	return best, nil
}

func (r *Reconciler) ensureService(ctx context.Context, cfg rwxConfig) (*corev1.Service, error) {
	desired := BuildService(cfg)

	existing := &corev1.Service{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Client.Create(ctx, desired); err != nil {
			return nil, err
		}
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

// ensureMirrorPV never mutates the CSI source or ClaimRef of an existing
// PV — that would break binding.
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

// annotateLocalVolume stamps the rwx-export annotation on the LV that
// backs the RWO PVC. The LV's name is the same as the PV name produced
// by the CSI driver (see genLocalVolumeFromRequest in
// pkg/local-storage/member/csi); we fall back to a PVC-ref scan if that
// naming contract ever drifts. Returns (requeue, err); requeue means
// "not an error, just not ready yet".
func (r *Reconciler) annotateLocalVolume(ctx context.Context, cfg rwxConfig) (bool, error) {
	backing := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: BackingPVCName(cfg.UserPVCName), Namespace: cfg.UserPVCNamespace}, backing); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if backing.Spec.VolumeName == "" {
		return true, nil
	}

	lv := &apisv1alpha1.LocalVolume{}
	getErr := r.Client.Get(ctx, types.NamespacedName{Name: backing.Spec.VolumeName}, lv)
	if apierrors.IsNotFound(getErr) {
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

// reconcileDelete tears down in reverse order: mirror PV → Service →
// backing PVC (triggers HwameiStor teardown of the LocalVolume) →
// finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (reconcile.Result, error) {
	if !containsFinalizer(pvc, RWXFinalizer) {
		return reconcile.Result{}, nil
	}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: MirrorPVName(pvc.UID)}}
	if err := r.Client.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: ServiceName(pvc.Name), Namespace: pvc.Namespace}}
	if err := r.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}
	backing := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: BackingPVCName(pvc.Name), Namespace: pvc.Namespace}}
	if err := r.Client.Delete(ctx, backing); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}
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
