package backup

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hwameistorv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

// BackupConfigReconciler owns the cluster-scoped BackupConfig singleton. It
// watches PVCs and the singleton, and reconciles one PVCBackup per matching
// PVC. It also propagates the S3-credentials and restic-password Secrets
// from the controller namespace into each namespace that has a managed PVC.
//
// User-authored PVCBackups (those without the managed-by label) are never
// touched — this controller only acts on resources it owns.
type BackupConfigReconciler struct {
	client    client.Client
	apiReader client.Reader

	// ControllerNamespace is where the user-authored S3 credentials and
	// restic password Secrets live. Managed namespaces receive mirror copies.
	ControllerNamespace string

	// ResticImage is the default image used for mover and restore Jobs.
	ResticImage string

	// ServiceAccountName is the ServiceAccount under which mover and
	// restore Jobs run in the target namespace. Must exist (or be created
	// by the chart) in every namespace a PVCBackup fires in.
	ServiceAccountName string
}

func NewBackupConfigReconciler(
	c client.Client,
	apiReader client.Reader,
	controllerNamespace, resticImage, serviceAccountName string,
) *BackupConfigReconciler {
	return &BackupConfigReconciler{
		client:              c,
		apiReader:           apiReader,
		ControllerNamespace: controllerNamespace,
		ResticImage:         resticImage,
		ServiceAccountName:  serviceAccountName,
	}
}

//+kubebuilder:rbac:groups=hwameistor.io,resources=backupconfigs,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=hwameistor.io,resources=backupconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *BackupConfigReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := crlog.FromContext(ctx).WithValues("pvc", req.NamespacedName)

	bc, ok, err := r.loadSingleton(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !ok {
		log.V(1).Info("no BackupConfig singleton; skipping")
		return reconcile.Result{}, nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err = r.client.Get(ctx, req.NamespacedName, pvc)
	if apierrors.IsNotFound(err) {
		// PVC deleted — remove any managed PVCBackup with this name.
		return r.deleteManagedPVCBackup(ctx, req.Namespace, req.Name)
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	matches, err := r.pvcMatches(ctx, bc, pvc)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !matches {
		return r.deleteManagedPVCBackup(ctx, pvc.Namespace, pvc.Name)
	}

	if err := r.ensurePropagatedSecrets(ctx, bc, pvc.Namespace); err != nil {
		return reconcile.Result{}, fmt.Errorf("propagate secrets: %w", err)
	}

	return r.ensureManagedPVCBackup(ctx, bc, pvc)
}

// LoadSingleton returns the one BackupConfig named "default". Other
// BackupConfigs are ignored. Exported so sibling controllers (Restore)
// can share the lookup without duplicating the singleton rule.
func (r *BackupConfigReconciler) LoadSingleton(ctx context.Context) (*hwameistorv1alpha1.BackupConfig, bool, error) {
	return r.loadSingleton(ctx)
}

func (r *BackupConfigReconciler) loadSingleton(ctx context.Context) (*hwameistorv1alpha1.BackupConfig, bool, error) {
	bc := &hwameistorv1alpha1.BackupConfig{}
	err := r.client.Get(ctx, client.ObjectKey{Name: BackupConfigSingletonName}, bc)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return bc, true, nil
}

// pvcMatches returns true if the PVC should be backed up under the given
// BackupConfig. The rules, in order:
//
//  1. If the PVC carries hwameistor.io/backup=false, skip.
//  2. If the PVC's StorageClass provisioner is not lvm.hwameistor.io, skip —
//     only hwameistor-backed PVCs are supported.
//  3. If BackupConfig.spec.selector is set and does not match the PVC's
//     labels, skip.
//  4. Otherwise match.
func (r *BackupConfigReconciler) pvcMatches(ctx context.Context, bc *hwameistorv1alpha1.BackupConfig, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	if v, ok := pvc.Annotations[BackupEnabledAnnotation]; ok && v == "false" {
		return false, nil
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return false, nil
	}
	sc := &storagev1.StorageClass{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if sc.Provisioner != HwameistorLVMCSIProvisioner {
		return false, nil
	}
	if bc.Spec.Selector != nil {
		sel, err := metav1.LabelSelectorAsSelector(bc.Spec.Selector)
		if err != nil {
			return false, err
		}
		if !sel.Matches(labels.Set(pvc.Labels)) {
			return false, nil
		}
	}
	return true, nil
}

// ensureManagedPVCBackup creates or updates the managed PVCBackup for a PVC.
func (r *BackupConfigReconciler) ensureManagedPVCBackup(ctx context.Context, bc *hwameistorv1alpha1.BackupConfig, pvc *corev1.PersistentVolumeClaim) (reconcile.Result, error) {
	want := r.buildPVCBackup(bc, pvc)

	got := &hwameistorv1alpha1.PVCBackup{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: want.Namespace, Name: want.Name}, got)
	if apierrors.IsNotFound(err) {
		if err := r.client.Create(ctx, want); err != nil {
			return reconcile.Result{}, fmt.Errorf("create PVCBackup: %w", err)
		}
		return reconcile.Result{}, nil
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	// Only reconcile PVCBackups we own. User-authored ones are left alone.
	if got.Labels[BackupManagedByLabel] != BackupManagedByValue {
		return reconcile.Result{}, nil
	}
	if pvcBackupSpecEqual(got.Spec, want.Spec) {
		return reconcile.Result{}, nil
	}
	patched := got.DeepCopy()
	patched.Spec = want.Spec
	if err := r.client.Update(ctx, patched); err != nil {
		return reconcile.Result{}, fmt.Errorf("update PVCBackup: %w", err)
	}
	return reconcile.Result{}, nil
}

func (r *BackupConfigReconciler) deleteManagedPVCBackup(ctx context.Context, namespace, pvcName string) (reconcile.Result, error) {
	got := &hwameistorv1alpha1.PVCBackup{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: pvcName}, got)
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}
	if err != nil {
		return reconcile.Result{}, err
	}
	if got.Labels[BackupManagedByLabel] != BackupManagedByValue {
		// User-authored — never touch it.
		return reconcile.Result{}, nil
	}
	if err := r.client.Delete(ctx, got); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// buildPVCBackup computes the desired PVCBackup for a PVC, merging
// BackupConfig defaults with per-PVC annotation overrides.
func (r *BackupConfigReconciler) buildPVCBackup(bc *hwameistorv1alpha1.BackupConfig, pvc *corev1.PersistentVolumeClaim) *hwameistorv1alpha1.PVCBackup {
	schedule := bc.Spec.Schedule
	if v, ok := pvc.Annotations[BackupScheduleAnnotation]; ok && v != "" {
		schedule = v
	}
	keepSnap := bc.Spec.KeepSnapshotAfterBackup
	if v, ok := pvc.Annotations[BackupKeepSnapshotAnnotation]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			keepSnap = b
		}
	}
	retention := bc.Spec.Retention
	retention.KeepLast = overrideRetention(retention.KeepLast, pvc.Annotations[BackupRetentionLastAnnotation])
	retention.KeepHourly = overrideRetention(retention.KeepHourly, pvc.Annotations[BackupRetentionHourlyAnnotation])
	retention.KeepDaily = overrideRetention(retention.KeepDaily, pvc.Annotations[BackupRetentionDailyAnnotation])
	retention.KeepWeekly = overrideRetention(retention.KeepWeekly, pvc.Annotations[BackupRetentionWeeklyAnnotation])
	retention.KeepMonthly = overrideRetention(retention.KeepMonthly, pvc.Annotations[BackupRetentionMonthlyAnnotation])
	retention.KeepYearly = overrideRetention(retention.KeepYearly, pvc.Annotations[BackupRetentionYearlyAnnotation])

	return &hwameistorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvc.Name,
			Namespace: pvc.Namespace,
			Labels: map[string]string{
				CreatedbyLabelKey:            CreatedbyLabelValue,
				BackupManagedByLabel:         BackupManagedByValue,
				BackupOwnerPVCNamespaceLabel: pvc.Namespace,
				BackupOwnerPVCNameLabel:      pvc.Name,
			},
		},
		Spec: hwameistorv1alpha1.PVCBackupSpec{
			PVCName:                 pvc.Name,
			Schedule:                schedule,
			Retention:               retention,
			KeepSnapshotAfterBackup: keepSnap,
			VolumeSnapshotClassName: bc.Spec.VolumeSnapshotClassName,
			Suspend:                 bc.Spec.Suspend,
		},
	}
}

// overrideRetention returns an annotation-parsed int32, falling back to the
// default when the annotation is empty or malformed.
func overrideRetention(def int32, raw string) int32 {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return def
	}
	return int32(n)
}

func pvcBackupSpecEqual(a, b hwameistorv1alpha1.PVCBackupSpec) bool {
	return a.PVCName == b.PVCName &&
		a.Schedule == b.Schedule &&
		a.Retention == b.Retention &&
		a.KeepSnapshotAfterBackup == b.KeepSnapshotAfterBackup &&
		a.VolumeSnapshotClassName == b.VolumeSnapshotClassName &&
		a.Suspend == b.Suspend &&
		a.Trigger == b.Trigger
}

// ensurePropagatedSecrets copies the two user-authored Secrets (S3 creds,
// restic password) from the controller namespace into the target namespace,
// keeping them in sync on each reconcile.
func (r *BackupConfigReconciler) ensurePropagatedSecrets(ctx context.Context, bc *hwameistorv1alpha1.BackupConfig, targetNS string) error {
	if targetNS == r.ControllerNamespace {
		return nil // No need to mirror into the controller's own namespace.
	}
	if err := r.mirrorSecret(ctx, bc.Spec.S3.CredentialsSecretRef.Name, r.controllerNS(bc.Spec.S3.CredentialsSecretRef.Namespace), targetNS); err != nil {
		return fmt.Errorf("mirror s3 creds secret: %w", err)
	}
	if err := r.mirrorSecret(ctx, bc.Spec.ResticPasswordSecretRef.Name, r.controllerNS(bc.Spec.ResticPasswordSecretRef.Namespace), targetNS); err != nil {
		return fmt.Errorf("mirror restic password secret: %w", err)
	}
	return nil
}

// controllerNS returns the namespace to read a user Secret from: honor the
// explicit SecretReference.Namespace if set, else fall back to the
// controller's own namespace.
func (r *BackupConfigReconciler) controllerNS(declared string) string {
	if declared != "" {
		return declared
	}
	return r.ControllerNamespace
}

func (r *BackupConfigReconciler) mirrorSecret(ctx context.Context, name, sourceNS, targetNS string) error {
	src := &corev1.Secret{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: sourceNS, Name: name}, src); err != nil {
		return err
	}
	want := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: targetNS,
			Labels: map[string]string{
				CreatedbyLabelKey:    CreatedbyLabelValue,
				BackupManagedByLabel: BackupManagedByValue,
			},
		},
		Type: src.Type,
		Data: src.Data,
	}
	got := &corev1.Secret{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: targetNS, Name: name}, got)
	if apierrors.IsNotFound(err) {
		return r.client.Create(ctx, want)
	}
	if err != nil {
		return err
	}
	if got.Labels[BackupManagedByLabel] != BackupManagedByValue {
		// A user-authored Secret with the same name exists in the target
		// namespace; don't clobber it.
		return nil
	}
	if secretDataEqual(got.Data, want.Data) && got.Type == want.Type {
		return nil
	}
	patched := got.DeepCopy()
	patched.Data = want.Data
	patched.Type = want.Type
	return r.client.Update(ctx, patched)
}

func secretDataEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}

// ResolveRuntimeConfig is the callback the PVCBackup reconciler invokes to
// merge cluster-wide BackupConfig settings with the per-PVCBackup spec and
// the resolved PVC's size/storage class.
func (r *BackupConfigReconciler) ResolveRuntimeConfig(ctx context.Context, pb *hwameistorv1alpha1.PVCBackup, pvc *corev1.PersistentVolumeClaim) (RuntimeConfig, bool, error) {
	bc, ok, err := r.loadSingleton(ctx)
	if err != nil || !ok {
		return RuntimeConfig{}, ok, err
	}
	size, hasSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !hasSize {
		return RuntimeConfig{}, false, fmt.Errorf("PVC %s/%s has no storage request", pvc.Namespace, pvc.Name)
	}
	sc := ""
	if pvc.Spec.StorageClassName != nil {
		sc = *pvc.Spec.StorageClassName
	}
	snapClass := pb.Spec.VolumeSnapshotClassName
	if snapClass == "" {
		snapClass = bc.Spec.VolumeSnapshotClassName
	}
	return RuntimeConfig{
		PVCNamespace:             pvc.Namespace,
		PVCName:                  pvc.Name,
		PVCSize:                  size,
		StorageClassName:         sc,
		PVCBackupName:            pb.Name,
		PVCBackupUID:             string(pb.UID),
		VolumeSnapshotClassName:  snapClass,
		Retention:                pb.Spec.Retention,
		KeepSnapshotAfterBackup:  pb.Spec.KeepSnapshotAfterBackup,
		S3Endpoint:               bc.Spec.S3.Endpoint,
		S3Bucket:                 bc.Spec.S3.Bucket,
		S3Region:                 bc.Spec.S3.Region,
		S3InsecureTLS:            bc.Spec.S3.InsecureTLS,
		S3Prefix:                 bc.Spec.S3.Prefix,
		S3CredentialsSecretName:  bc.Spec.S3.CredentialsSecretRef.Name,
		ResticPasswordSecretName: bc.Spec.ResticPasswordSecretRef.Name,
		ResticImage:              r.ResticImage,
		ServiceAccountName:       r.ServiceAccountName,
	}, true, nil
}

// SetupWithManager registers this reconciler with the controller manager.
// Adapted to the older controller-runtime API vendored in hwameistor: we use
// controller.New + source.Kind instead of builder.Builder.Named/For/Watches.
func (r *BackupConfigReconciler) SetupWithManager(mgr manager.Manager) error {
	// Import here to keep the package list clean and avoid polluting the file
	// header with old-style imports outside SetupWithManager.
	return addBackupConfigController(mgr, r)
}

// Compile-time assertion that the scheme includes hwameistor v1alpha1 (so
// this file fails early if wiring drifts).
var _ runtime.Object = &hwameistorv1alpha1.BackupConfig{}

// Compile-time check for reconcile.Reconciler satisfaction.
var _ reconcile.Reconciler = &BackupConfigReconciler{}
