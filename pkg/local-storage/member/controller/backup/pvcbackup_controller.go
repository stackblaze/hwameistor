package backup

import (
	"context"
	"fmt"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/robfig/cron/v3"
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

// PVCBackupReconciler owns a PVCBackup through its backup lifecycle. One
// reconciler handles all PVCBackups in the cluster; per-PVCBackup state
// lives entirely in the CR's .status subresource so restarts are safe.
//
// Phases walked on a single run:
//
//	Idle -> Snapshotting -> Cloning -> Moving -> Cleanup -> Synced
//
// Any error at any phase transitions the CR to Failed and keeps the
// schedule — the next fire time will attempt a fresh run.
type PVCBackupReconciler struct {
	client    client.Client
	apiReader client.Reader

	// ResolveRuntimeConfig returns the merged runtime config for a PVCBackup:
	// S3/credentials/image pulled from the cluster-wide BackupConfig singleton,
	// merged with the PVCBackup's per-CR fields and the resolved source PVC's
	// storage class and size. Returning (false, nil) means "no BackupConfig
	// yet — do not attempt backups".
	//
	// Injected by the manager wiring so the BackupConfig controller owns the
	// BackupConfig lookup + cross-namespace secret propagation.
	ResolveRuntimeConfig func(ctx context.Context, pvcBackup *hwameistorv1alpha1.PVCBackup, pvc *corev1.PersistentVolumeClaim) (RuntimeConfig, bool, error)
}

func NewPVCBackupReconciler(
	c client.Client,
	apiReader client.Reader,
	resolve func(ctx context.Context, pvcBackup *hwameistorv1alpha1.PVCBackup, pvc *corev1.PersistentVolumeClaim) (RuntimeConfig, bool, error),
) *PVCBackupReconciler {
	return &PVCBackupReconciler{
		client:               c,
		apiReader:            apiReader,
		ResolveRuntimeConfig: resolve,
	}
}

//+kubebuilder:rbac:groups=hwameistor.io,resources=pvcbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hwameistor.io,resources=pvcbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hwameistor.io,resources=pvcbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=pods/log,verbs=get
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete

func (r *PVCBackupReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := crlog.FromContext(ctx).WithValues("pvcbackup", req.NamespacedName)

	pb := &hwameistorv1alpha1.PVCBackup{}
	if err := r.client.Get(ctx, req.NamespacedName, pb); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if pb.DeletionTimestamp != nil {
		// Children carry owner refs with Controller=true so garbage
		// collection handles cleanup. Nothing to do here.
		return reconcile.Result{}, nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: pb.Namespace, Name: pb.Spec.PVCName}, pvc)
	if apierrors.IsNotFound(err) {
		return r.setFailed(ctx, pb, fmt.Sprintf("source PVC %q not found", pb.Spec.PVCName))
	}
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("get source PVC: %w", err)
	}

	cfg, ok, err := r.ResolveRuntimeConfig(ctx, pb, pvc)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("resolve runtime config: %w", err)
	}
	if !ok {
		log.Info("no BackupConfig yet; skipping")
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}

	// Active run — advance the state machine.
	if pb.Status.Phase != "" &&
		pb.Status.Phase != hwameistorv1alpha1.PVCBackupPhaseIdle &&
		pb.Status.Phase != hwameistorv1alpha1.PVCBackupPhaseSynced &&
		pb.Status.Phase != hwameistorv1alpha1.PVCBackupPhaseFailed {
		return r.advance(ctx, pb, cfg)
	}

	// Idle / Synced / Failed / empty — decide whether to start a new run.
	return r.maybeStartRun(ctx, pb, cfg)
}

// maybeStartRun checks the cron schedule and manual trigger and, if a run
// is due, transitions the CR to Snapshotting and creates the VolumeSnapshot.
func (r *PVCBackupReconciler) maybeStartRun(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
	cfg RuntimeConfig,
) (reconcile.Result, error) {
	if pb.Spec.Suspend {
		return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
			s.Phase = hwameistorv1alpha1.PVCBackupPhaseIdle
			s.Message = "suspended"
		})
	}

	now := time.Now()
	manualTrigger := pb.Spec.Trigger != "" && pb.Spec.Trigger != pb.Status.LastHandledTrigger
	nextTime := nextFireTime(pb, now)

	due := manualTrigger || (!nextTime.IsZero() && !now.Before(nextTime))
	if !due {
		// Requeue at the next fire time (or in 1h if no schedule — keeps
		// the controller alive to pick up spec changes).
		wait := time.Hour
		if !nextTime.IsZero() {
			wait = time.Until(nextTime)
			if wait < time.Second {
				wait = time.Second
			}
		}
		return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
			if s.Phase == "" {
				s.Phase = hwameistorv1alpha1.PVCBackupPhaseIdle
			}
			if !nextTime.IsZero() {
				t := metav1.NewTime(nextTime)
				s.NextSyncTime = &t
			} else {
				s.NextSyncTime = nil
			}
		}, withRequeueAfter(wait))
	}

	// Due to fire — transition to Snapshotting and create the child
	// VolumeSnapshot. The runID survives restarts via status.currentSnapshotName.
	runID := now.UTC().Format("20060102150405")
	snap := Snapshot(cfg, runID)
	if err := r.client.Create(ctx, snap); err != nil && !apierrors.IsAlreadyExists(err) {
		return reconcile.Result{}, fmt.Errorf("create VolumeSnapshot: %w", err)
	}

	return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
		s.Phase = hwameistorv1alpha1.PVCBackupPhaseSnapshotting
		s.Message = ""
		s.CurrentSnapshotName = snap.Name
		s.CurrentTempPVCName = TempPVCName(cfg.PVCName, runID)
		s.CurrentMoverJobName = MoverJobName(cfg.PVCName, runID)
		t := metav1.NewTime(now)
		s.CurrentRunStartTime = &t
		if manualTrigger {
			s.LastHandledTrigger = pb.Spec.Trigger
		}
	})
}

// advance walks one step of the state machine based on the current phase
// and the observed state of child objects.
func (r *PVCBackupReconciler) advance(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
	cfg RuntimeConfig,
) (reconcile.Result, error) {
	switch pb.Status.Phase {
	case hwameistorv1alpha1.PVCBackupPhaseSnapshotting:
		return r.advanceSnapshotting(ctx, pb, cfg)
	case hwameistorv1alpha1.PVCBackupPhaseCloning:
		return r.advanceCloning(ctx, pb, cfg)
	case hwameistorv1alpha1.PVCBackupPhaseMoving:
		return r.advanceMoving(ctx, pb)
	case hwameistorv1alpha1.PVCBackupPhaseCleanup:
		return r.advanceCleanup(ctx, pb)
	}
	return reconcile.Result{}, nil
}

func (r *PVCBackupReconciler) advanceSnapshotting(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
	cfg RuntimeConfig,
) (reconcile.Result, error) {
	snap := &snapshotv1.VolumeSnapshot{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: pb.Namespace, Name: pb.Status.CurrentSnapshotName}, snap)
	if apierrors.IsNotFound(err) {
		return r.setFailed(ctx, pb, "VolumeSnapshot disappeared mid-run")
	}
	if err != nil {
		return reconcile.Result{}, err
	}
	if snap.Status == nil || snap.Status.ReadyToUse == nil || !*snap.Status.ReadyToUse {
		// still provisioning — poll.
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Snapshot ready; derive the runID from the stored temp PVC name to
	// ensure deterministic naming across reconciles.
	runID := extractRunIDFromTempPVC(pb.Spec.PVCName, pb.Status.CurrentTempPVCName)
	if runID == "" {
		return r.setFailed(ctx, pb, "could not derive run id from status")
	}
	tempPVC := TempPVC(cfg, runID)
	if err := r.client.Create(ctx, tempPVC); err != nil && !apierrors.IsAlreadyExists(err) {
		return reconcile.Result{}, fmt.Errorf("create temp PVC: %w", err)
	}
	return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
		s.Phase = hwameistorv1alpha1.PVCBackupPhaseCloning
	})
}

func (r *PVCBackupReconciler) advanceCloning(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
	cfg RuntimeConfig,
) (reconcile.Result, error) {
	tempPVC := &corev1.PersistentVolumeClaim{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: pb.Namespace, Name: pb.Status.CurrentTempPVCName}, tempPVC)
	if apierrors.IsNotFound(err) {
		return r.setFailed(ctx, pb, "temp PVC disappeared mid-run")
	}
	if err != nil {
		return reconcile.Result{}, err
	}
	// Do NOT wait for the temp PVC to reach Bound here. Hwameistor's default
	// StorageClass uses WaitForFirstConsumer, so the PVC will stay Pending
	// until a Pod references it. The mover Job we're about to create is
	// exactly that first consumer — creating it triggers the scheduler to
	// pick a node and hwameistor to provision the underlying LV.

	runID := extractRunIDFromTempPVC(pb.Spec.PVCName, pb.Status.CurrentTempPVCName)
	if runID == "" {
		return r.setFailed(ctx, pb, "could not derive run id from status")
	}
	job := MoverJob(cfg, runID)
	if err := r.client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return reconcile.Result{}, fmt.Errorf("create mover job: %w", err)
	}
	return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
		s.Phase = hwameistorv1alpha1.PVCBackupPhaseMoving
	})
}

func (r *PVCBackupReconciler) advanceMoving(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
) (reconcile.Result, error) {
	job := &batchv1.Job{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: pb.Namespace, Name: pb.Status.CurrentMoverJobName}, job)
	if apierrors.IsNotFound(err) {
		return r.setFailed(ctx, pb, "mover Job disappeared mid-run")
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	switch {
	case job.Status.Succeeded > 0:
		return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
			s.Phase = hwameistorv1alpha1.PVCBackupPhaseCleanup
			s.LatestMoverStatus = &hwameistorv1alpha1.MoverStatus{
				JobName:   job.Name,
				Succeeded: true,
			}
		})
	case job.Status.Failed > 0:
		return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
			s.Phase = hwameistorv1alpha1.PVCBackupPhaseCleanup
			s.LatestMoverStatus = &hwameistorv1alpha1.MoverStatus{
				JobName:   job.Name,
				Succeeded: false,
			}
			s.Message = "mover Job failed"
		})
	default:
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}
}

func (r *PVCBackupReconciler) advanceCleanup(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
) (reconcile.Result, error) {
	// Always delete the temp PVC. Delete the mover Job so a subsequent run
	// can reuse the deterministic name for Cleanup diagnostics — the user
	// can still see status.latestMoverStatus.
	if name := pb.Status.CurrentTempPVCName; name != "" {
		tempPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: pb.Namespace, Name: name},
		}
		if err := r.client.Delete(ctx, tempPVC); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("delete temp PVC: %w", err)
		}
	}
	if name := pb.Status.CurrentMoverJobName; name != "" {
		// Use propagation=Background so the Job's Pod is GC'd; otherwise
		// the Pod lingers and blocks re-running with the same deterministic name.
		bg := metav1.DeletePropagationBackground
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: pb.Namespace, Name: name},
		}
		if err := r.client.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("delete mover Job: %w", err)
		}
	}
	if !pb.Spec.KeepSnapshotAfterBackup {
		if name := pb.Status.CurrentSnapshotName; name != "" {
			snap := &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{Namespace: pb.Namespace, Name: name},
			}
			if err := r.client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
				return reconcile.Result{}, fmt.Errorf("delete VolumeSnapshot: %w", err)
			}
		}
	}

	succeeded := pb.Status.LatestMoverStatus != nil && pb.Status.LatestMoverStatus.Succeeded
	now := metav1.Now()
	return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
		if succeeded {
			s.Phase = hwameistorv1alpha1.PVCBackupPhaseSynced
			s.LastSyncTime = &now
			if s.CurrentRunStartTime != nil {
				d := metav1.Duration{Duration: now.Sub(s.CurrentRunStartTime.Time)}
				s.LastSyncDuration = &d
			}
			s.Message = ""
		} else {
			s.Phase = hwameistorv1alpha1.PVCBackupPhaseFailed
		}
		s.CurrentRunStartTime = nil
		s.CurrentSnapshotName = ""
		s.CurrentTempPVCName = ""
		s.CurrentMoverJobName = ""
	})
}

// setFailed transitions the PVCBackup to Failed with a message and clears
// any in-flight child tracking. Child resources may still exist; the next
// reconcile will retry the run and recreate them.
func (r *PVCBackupReconciler) setFailed(ctx context.Context, pb *hwameistorv1alpha1.PVCBackup, msg string) (reconcile.Result, error) {
	return r.updateStatus(ctx, pb, func(s *hwameistorv1alpha1.PVCBackupStatus) {
		s.Phase = hwameistorv1alpha1.PVCBackupPhaseFailed
		s.Message = msg
		s.CurrentRunStartTime = nil
		s.CurrentSnapshotName = ""
		s.CurrentTempPVCName = ""
		s.CurrentMoverJobName = ""
	})
}

type updateOpt func(*reconcile.Result)

func withRequeueAfter(d time.Duration) updateOpt {
	return func(r *reconcile.Result) { r.RequeueAfter = d }
}

// updateStatus applies mutate to a copy of .status, patches, and returns a
// Result configured by the optional updateOpts.
func (r *PVCBackupReconciler) updateStatus(
	ctx context.Context,
	pb *hwameistorv1alpha1.PVCBackup,
	mutate func(*hwameistorv1alpha1.PVCBackupStatus),
	opts ...updateOpt,
) (reconcile.Result, error) {
	patched := pb.DeepCopy()
	mutate(&patched.Status)
	patched.Status.ObservedGeneration = pb.Generation
	if err := r.client.Status().Patch(ctx, patched, client.MergeFrom(pb)); err != nil {
		return reconcile.Result{}, err
	}
	res := reconcile.Result{}
	for _, o := range opts {
		o(&res)
	}
	return res, nil
}

// nextFireTime parses .spec.schedule and returns the next fire time after
// the latest of lastSyncTime / currentRunStartTime. Zero time means no
// schedule.
func nextFireTime(pb *hwameistorv1alpha1.PVCBackup, now time.Time) time.Time {
	if pb.Spec.Schedule == "" {
		return time.Time{}
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(pb.Spec.Schedule)
	if err != nil {
		return time.Time{}
	}
	anchor := pb.CreationTimestamp.Time
	if pb.Status.LastSyncTime != nil && pb.Status.LastSyncTime.After(anchor) {
		anchor = pb.Status.LastSyncTime.Time
	}
	next := schedule.Next(anchor)
	// Step forward until strictly after now to avoid re-firing the same slot.
	for !next.After(now) {
		next = schedule.Next(next)
	}
	return next
}

// extractRunIDFromTempPVC recovers the run id from a deterministic temp PVC
// name. Returns "" if the name is malformed.
func extractRunIDFromTempPVC(pvcName, tempPVCName string) string {
	prefix := pvcName + BackupJobTempPVCInfix
	if len(tempPVCName) <= len(prefix) {
		return ""
	}
	if tempPVCName[:len(prefix)] != prefix {
		return ""
	}
	return tempPVCName[len(prefix):]
}

// SetupWithManager registers this reconciler with the controller manager.
func (r *PVCBackupReconciler) SetupWithManager(mgr manager.Manager) error {
	return addPVCBackupController(mgr, r)
}

// Compile-time check for reconcile.Reconciler satisfaction.
var _ reconcile.Reconciler = &PVCBackupReconciler{}
