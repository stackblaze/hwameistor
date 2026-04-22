package backup

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hwameistorv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

// RestoreReconciler walks a Restore CR through:
//
//	Pending -> provision target PVC -> wait for Bound -> Running (restore Job)
//	-> Succeeded | Failed
//
// The target PVC is deliberately NOT owned by the Restore CR — if the
// Restore is deleted, the restored data should remain. The restore Job is
// owned by the Restore CR so deleting the CR cascades the Job away.
type RestoreReconciler struct {
	client    client.Client
	apiReader client.Reader

	// LoadBackupConfig returns the cluster-wide BackupConfig singleton,
	// injected so this controller does not duplicate loadSingleton logic.
	LoadBackupConfig func(ctx context.Context) (*hwameistorv1alpha1.BackupConfig, bool, error)

	ControllerNamespace string
	ResticImage         string
	ServiceAccountName  string
}

func NewRestoreReconciler(
	c client.Client,
	apiReader client.Reader,
	loadBC func(ctx context.Context) (*hwameistorv1alpha1.BackupConfig, bool, error),
	controllerNamespace, resticImage, serviceAccountName string,
) *RestoreReconciler {
	return &RestoreReconciler{
		client:              c,
		apiReader:           apiReader,
		LoadBackupConfig:    loadBC,
		ControllerNamespace: controllerNamespace,
		ResticImage:         resticImage,
		ServiceAccountName:  serviceAccountName,
	}
}

//+kubebuilder:rbac:groups=hwameistor.io,resources=restores,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=hwameistor.io,resources=restores/status,verbs=get;update;patch

func (r *RestoreReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := crlog.FromContext(ctx).WithValues("restore", req.NamespacedName)

	rs := &hwameistorv1alpha1.Restore{}
	if err := r.client.Get(ctx, req.NamespacedName, rs); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if rs.DeletionTimestamp != nil {
		return reconcile.Result{}, nil
	}

	// Terminal phases never re-enter the flow.
	if rs.Status.Phase == hwameistorv1alpha1.RestorePhaseSucceeded || rs.Status.Phase == hwameistorv1alpha1.RestorePhaseFailed {
		return reconcile.Result{}, nil
	}

	bc, ok, err := r.LoadBackupConfig(ctx)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("load BackupConfig: %w", err)
	}
	if !ok {
		log.Info("no BackupConfig yet; cannot restore")
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}

	if err := r.ensureTargetPVC(ctx, rs); err != nil {
		return reconcile.Result{}, fmt.Errorf("ensure target PVC: %w", err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err = r.client.Get(ctx, client.ObjectKey{Namespace: rs.Namespace, Name: rs.Spec.TargetPVCName}, pvc)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("get target PVC: %w", err)
	}
	// Do NOT wait for Bound: hwameistor's default StorageClass uses
	// WaitForFirstConsumer, so the target PVC stays Pending until the
	// restore Job's Pod references it. Creating the Job here is the
	// trigger for provisioning.

	// Ensure the restore Job exists and watch for completion.
	cfg := RestoreConfig{
		RestoreName:              rs.Name,
		RestoreNamespace:         rs.Namespace,
		RestoreUID:               string(rs.UID),
		TargetPVCName:            rs.Spec.TargetPVCName,
		SourceNamespace:          rs.Spec.SourceNamespace,
		SourcePVCName:            rs.Spec.SourcePVCName,
		SnapshotID:               rs.Spec.SnapshotID,
		S3Endpoint:               bc.Spec.S3.Endpoint,
		S3Bucket:                 bc.Spec.S3.Bucket,
		S3Region:                 bc.Spec.S3.Region,
		S3InsecureTLS:            bc.Spec.S3.InsecureTLS,
		S3Prefix:                 bc.Spec.S3.Prefix,
		S3CredentialsSecretName:  bc.Spec.S3.CredentialsSecretRef.Name,
		ResticPasswordSecretName: bc.Spec.ResticPasswordSecretRef.Name,
		ResticImage:              r.ResticImage,
		ServiceAccountName:       r.ServiceAccountName,
	}
	job := RestoreJob(cfg)
	got := &batchv1.Job{}
	err = r.client.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: job.Name}, got)
	if apierrors.IsNotFound(err) {
		if err := r.client.Create(ctx, job); err != nil {
			return reconcile.Result{}, fmt.Errorf("create restore Job: %w", err)
		}
		return r.setRestoreStatus(ctx, rs, hwameistorv1alpha1.RestorePhaseRunning, "restore Job created", withJobName(job.Name))
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	switch {
	case got.Status.Succeeded > 0:
		return r.setRestoreStatus(ctx, rs, hwameistorv1alpha1.RestorePhaseSucceeded, "restore complete", withJobName(got.Name), withCompletion())
	case got.Status.Failed > 0:
		return r.setRestoreStatus(ctx, rs, hwameistorv1alpha1.RestorePhaseFailed, "restore Job failed", withJobName(got.Name), withCompletion())
	default:
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}
}

// ensureTargetPVC creates the target PVC if missing. The target PVC is NOT
// owned by the Restore CR — deleting the Restore must not delete the
// restored data.
func (r *RestoreReconciler) ensureTargetPVC(ctx context.Context, rs *hwameistorv1alpha1.Restore) error {
	got := &corev1.PersistentVolumeClaim{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: rs.Namespace, Name: rs.Spec.TargetPVCName}, got)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	sc := rs.Spec.StorageClassName
	want := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rs.Spec.TargetPVCName,
			Namespace: rs.Namespace,
			Labels: map[string]string{
				CreatedbyLabelKey:    CreatedbyLabelValue,
				BackupManagedByLabel: BackupManagedByValue,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &sc,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: rs.Spec.Size,
				},
			},
		},
	}
	return r.client.Create(ctx, want)
}

type restoreStatusOpt func(*hwameistorv1alpha1.RestoreStatus)

func withJobName(name string) restoreStatusOpt {
	return func(s *hwameistorv1alpha1.RestoreStatus) {
		s.JobName = name
		if s.StartTime == nil {
			now := metav1.Now()
			s.StartTime = &now
		}
	}
}

func withCompletion() restoreStatusOpt {
	return func(s *hwameistorv1alpha1.RestoreStatus) {
		now := metav1.Now()
		s.CompletionTime = &now
	}
}

func (r *RestoreReconciler) setRestoreStatus(
	ctx context.Context,
	rs *hwameistorv1alpha1.Restore,
	phase hwameistorv1alpha1.RestorePhase,
	msg string,
	opts ...restoreStatusOpt,
) (reconcile.Result, error) {
	patched := rs.DeepCopy()
	patched.Status.Phase = phase
	patched.Status.Message = msg
	for _, o := range opts {
		o(&patched.Status)
	}
	if err := r.client.Status().Patch(ctx, patched, client.MergeFrom(rs)); err != nil {
		return reconcile.Result{}, err
	}
	// Pending/Running phases requeue so we keep watching until terminal.
	if phase == hwameistorv1alpha1.RestorePhasePending || phase == hwameistorv1alpha1.RestorePhaseRunning {
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return reconcile.Result{}, nil
}

// SetupWithManager registers this reconciler with the controller manager.
func (r *RestoreReconciler) SetupWithManager(mgr manager.Manager) error {
	return addRestoreController(mgr, r)
}

// Compile-time check for reconcile.Reconciler satisfaction.
var _ reconcile.Reconciler = &RestoreReconciler{}
