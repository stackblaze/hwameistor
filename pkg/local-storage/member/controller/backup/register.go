package backup

import (
	"context"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	hwameistorv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

// SetupBackupReconcilers wires the three backup/restore reconcilers:
// BackupConfig (PVC -> PVCBackup + Secret propagation), PVCBackup (runs the
// actual backup state machine), and Restore (one-shot restore flow).
//
// controllerNamespace is the namespace where the user-authored S3 creds and
// restic password Secrets live; the BackupConfig controller mirrors them
// into every namespace that has a managed PVC.
func SetupBackupReconcilers(
	mgr manager.Manager,
	c client.Client,
	apiReader client.Reader,
	controllerNamespace, resticImage, moverServiceAccountName string,
) error {
	bc := NewBackupConfigReconciler(
		c, apiReader, controllerNamespace, resticImage, moverServiceAccountName,
	)
	if err := bc.SetupWithManager(mgr); err != nil {
		return err
	}

	pb := NewPVCBackupReconciler(c, apiReader, bc.ResolveRuntimeConfig)
	if err := pb.SetupWithManager(mgr); err != nil {
		return err
	}

	rs := NewRestoreReconciler(
		c, apiReader, bc.LoadSingleton,
		controllerNamespace, resticImage, moverServiceAccountName,
	)
	return rs.SetupWithManager(mgr)
}

// addBackupConfigController registers the BackupConfig reconciler using the
// old controller-runtime API (controller.New + source.Kind). We watch PVCs
// as the primary resource (the reconciler keys on PVC identity), and also
// watch BackupConfig so edits to the singleton re-enqueue every PVC.
func addBackupConfigController(mgr manager.Manager, r *BackupConfigReconciler) error {
	c, err := controller.New("hwameistor-backup-config", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	if err := c.Watch(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	// Re-enqueue every PVC when the singleton BackupConfig changes.
	pvcFanout := handler.EnqueueRequestsFromMapFunc(func(_ client.Object) []reconcile.Request {
		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := r.client.List(context.Background(), pvcs); err != nil {
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(pvcs.Items))
		for _, p := range pvcs.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: p.Namespace, Name: p.Name}})
		}
		return reqs
	})
	return c.Watch(&source.Kind{Type: &hwameistorv1alpha1.BackupConfig{}}, pvcFanout)
}

// addPVCBackupController registers the PVCBackup reconciler. Watches the
// primary PVCBackup resource plus the three owned child kinds (VolumeSnapshot,
// PVC, Job) so state changes on children re-enqueue the parent.
func addPVCBackupController(mgr manager.Manager, r *PVCBackupReconciler) error {
	c, err := controller.New("hwameistor-backup-pvcbackup", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	if err := c.Watch(&source.Kind{Type: &hwameistorv1alpha1.PVCBackup{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	ownerHandler := &handler.EnqueueRequestForOwner{OwnerType: &hwameistorv1alpha1.PVCBackup{}, IsController: true}
	if err := c.Watch(&source.Kind{Type: &snapshotv1.VolumeSnapshot{}}, ownerHandler); err != nil {
		return err
	}
	if err := c.Watch(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, ownerHandler); err != nil {
		return err
	}
	return c.Watch(&source.Kind{Type: &batchv1.Job{}}, ownerHandler)
}

// addRestoreController registers the Restore reconciler. Watches the primary
// Restore resource plus the restore Job as an owned child.
func addRestoreController(mgr manager.Manager, r *RestoreReconciler) error {
	c, err := controller.New("hwameistor-backup-restore", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	if err := c.Watch(&source.Kind{Type: &hwameistorv1alpha1.Restore{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	ownerHandler := &handler.EnqueueRequestForOwner{OwnerType: &hwameistorv1alpha1.Restore{}, IsController: true}
	return c.Watch(&source.Kind{Type: &batchv1.Job{}}, ownerHandler)
}
