package nfsreactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apisv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

// ExportAnnotation is the LocalVolume annotation whose value is the user PVC
// UID. Non-empty means "export this LV over NFS". Empty / missing means skip.
const ExportAnnotation = "hwameistor.io/rwx-export"

// Options bundles the Reactor's configuration.
type Options struct {
	Client         client.Client
	NodeName       string
	PodIP          string
	PodNamespace   string
	DbusAddr       string
	ExportRoot     string
	ResyncInterval time.Duration
}

// Reactor is the per-node NFS export reconciler.
type Reactor struct {
	opts    Options
	state   *State
	mounts  MountClient
	ganesha GaneshaClient
	slices  EndpointSliceClient
}

// New builds a Reactor with the real mount/ganesha/slice clients.
func New(opts Options) *Reactor {
	return &Reactor{
		opts:    opts,
		state:   NewState(),
		mounts:  NewRealMountClient(),
		ganesha: NewGaneshaDBusClient(opts.DbusAddr),
		slices:  NewEndpointSliceClient(opts.Client, opts.NodeName, opts.PodIP),
	}
}

// NewForTest builds a Reactor with injected fakes. Used by tests only.
func NewForTest(opts Options, m MountClient, g GaneshaClient, s EndpointSliceClient) *Reactor {
	return &Reactor{opts: opts, state: NewState(), mounts: m, ganesha: g, slices: s}
}

// SetupWithManager wires the Reactor into a controller-runtime Manager.
// It watches LocalVolume and LocalVolumeReplica (changes on this node's
// replicas must re-enqueue the owning LV), and adds a periodic resync as a
// Runnable on the manager.
func (r *Reactor) SetupWithManager(mgr manager.Manager) error {
	// Best-effort state rehydrate from /proc/mounts. Ignored on platforms
	// where /proc/mounts isn't available (e.g. test runs).
	if err := r.state.RebuildFromMounts(r.opts.ExportRoot); err != nil {
		log.WithError(err).Debug("nfsreactor: could not rebuild state from /proc/mounts (ok if running off-node)")
	}

	c, err := controller.New("nfs-reactor", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("build controller: %w", err)
	}
	if err := c.Watch(&source.Kind{Type: &apisv1alpha1.LocalVolume{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return fmt.Errorf("watch LocalVolume: %w", err)
	}
	replicaToVolume := handler.EnqueueRequestsFromMapFunc(func(a client.Object) []reconcile.Request {
		lvr, ok := a.(*apisv1alpha1.LocalVolumeReplica)
		if !ok || lvr.Spec.VolumeName == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: lvr.Spec.VolumeName}}}
	})
	if err := c.Watch(&source.Kind{Type: &apisv1alpha1.LocalVolumeReplica{}}, replicaToVolume); err != nil {
		return fmt.Errorf("watch LocalVolumeReplica: %w", err)
	}

	// Register the periodic full resync as a Runnable.
	if err := mgr.Add(manager.RunnableFunc(r.runResync)); err != nil {
		return fmt.Errorf("add resync runnable: %w", err)
	}
	return nil
}

// runResync lists all LocalVolumes every ResyncInterval and enqueues a
// reconcile for each by calling Reconcile directly. This compensates for
// missed events and catches drift (e.g. manual umount on the node).
func (r *Reactor) runResync(ctx context.Context) error {
	interval := r.opts.ResyncInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// Fire once at startup so we converge quickly.
	r.resyncOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.resyncOnce(ctx)
		}
	}
}

func (r *Reactor) resyncOnce(ctx context.Context) {
	lvs := &apisv1alpha1.LocalVolumeList{}
	if err := r.opts.Client.List(ctx, lvs); err != nil {
		log.WithError(err).Warn("nfsreactor: resync list failed")
		return
	}
	for i := range lvs.Items {
		lv := &lvs.Items[i]
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: lv.Name}}); err != nil {
			log.WithError(err).WithField("lv", lv.Name).Warn("nfsreactor: resync reconcile failed")
		}
	}
}

// Reconcile brings the on-node state for one LocalVolume in line with its
// annotation + replica placement. Safe to call repeatedly; idempotent.
func (r *Reactor) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	lg := log.WithField("lv", req.Name)

	lv := &apisv1alpha1.LocalVolume{}
	err := r.opts.Client.Get(ctx, types.NamespacedName{Name: req.Name}, lv)
	if apierrors.IsNotFound(err) {
		// LV gone — tear down anything we were serving for it.
		if prior := r.state.GetByLV(req.Name); prior != nil {
			lg.Info("LocalVolume deleted, tearing down export")
			if terr := r.ensureAbsent(ctx, prior); terr != nil {
				return reconcile.Result{}, terr
			}
		}
		return reconcile.Result{}, nil
	}
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("get LocalVolume %s: %w", req.Name, err)
	}

	uid := lv.Annotations[ExportAnnotation]
	eligible := uid != "" && r.isLocallyReady(lv)

	if !eligible {
		if prior := r.state.GetByLV(lv.Name); prior != nil {
			lg.WithField("uid", prior.UID).Info("LV no longer eligible on this node, removing export")
			if terr := r.ensureAbsent(ctx, prior); terr != nil {
				return reconcile.Result{}, terr
			}
		}
		return reconcile.Result{}, nil
	}

	// Eligible. Build target served-state, diff against in-memory state.
	device, err := r.resolveDevicePath(ctx, lv)
	if err != nil {
		return reconcile.Result{}, err
	}
	// HwameiStor's LVM driver doesn't record the SC fsType on the LV unless
	// NodeStageVolume runs — which it never does for RWX backing volumes.
	// Default to xfs to match the StorageClass we ship (and what HwameiStor
	// itself defaults to).
	fsType := lv.Status.PublishedFSType
	if fsType == "" {
		fsType = "xfs"
	}
	serviceName := serviceNameFor(lv)
	serviceNS := lv.Spec.PersistentVolumeClaimNamespace
	if serviceNS == "" {
		// Fall back to our own namespace so we don't crash; this is really
		// a misconfiguration the controller should set.
		serviceNS = r.opts.PodNamespace
	}
	mountPath := filepath.Join(r.opts.ExportRoot, uid)

	existing := r.state.Get(uid)
	if existing != nil && existing.ExportID != 0 &&
		existing.Device == device && existing.MountPath == mountPath &&
		existing.ServiceName == serviceName && existing.ServiceNS == serviceNS {
		// Everything already matches. Make sure the EndpointSlice is present
		// (cheap) and return.
		if err := r.slices.Put(ctx, serviceName, serviceNS); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	// Mount (no-op if already mounted).
	if err := r.mounts.Mount(device, mountPath, fsType); err != nil {
		return reconcile.Result{}, err
	}

	// Assign or reuse export ID.
	exportID := uint16(0)
	if existing != nil && existing.ExportID != 0 {
		exportID = existing.ExportID
	} else {
		exportID = r.state.AllocateID()
	}

	cfg := RenderExportConfig(exportID, mountPath, uid)
	// Ganesha's DBus AddExport takes a path to a config *file*, not inline
	// config text. Write the export block under <ExportRoot>/<uid>.conf so
	// both the reactor and ganesha containers can see it on the shared
	// emptyDir. Make sure ExportRoot exists first — it normally does (mount
	// created it), but during unit tests the fake MountClient doesn't.
	cfgPath := filepath.Join(r.opts.ExportRoot, uid+".conf")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		return reconcile.Result{}, fmt.Errorf("mkdir ExportRoot: %w", err)
	}
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		return reconcile.Result{}, fmt.Errorf("write ganesha export config: %w", err)
	}
	// Ganesha rejects AddExport for a pseudo-path/export-id that already
	// exists. On reactor restart our in-memory state is empty, so the
	// export can still be registered in Ganesha. Remove first (no-op if
	// absent), then re-add. RemoveExport errors are informational at this
	// point — if the export truly isn't there, the subsequent AddExport
	// is what matters.
	if err := r.ganesha.RemoveExport(exportID); err != nil {
		lg.WithError(err).WithField("exportID", exportID).Debug("ganesha RemoveExport (pre-add) returned — likely not present, will add")
	}
	if err := r.ganesha.AddExport(exportID, cfgPath, cfg); err != nil {
		return reconcile.Result{}, fmt.Errorf("ganesha AddExport: %w", err)
	}

	if err := r.slices.Put(ctx, serviceName, serviceNS); err != nil {
		return reconcile.Result{}, err
	}

	r.state.Put(&Served{
		UID:         uid,
		LVName:      lv.Name,
		ExportID:    exportID,
		Device:      device,
		MountPath:   mountPath,
		ServiceName: serviceName,
		ServiceNS:   serviceNS,
	})
	lg.WithFields(log.Fields{
		"uid":      uid,
		"exportID": exportID,
		"device":   device,
		"mount":    mountPath,
		"service":  serviceName,
	}).Info("nfsreactor: export published")
	return reconcile.Result{}, nil
}

// ensureAbsent removes the EndpointSlice, unmounts, rmdirs, and clears state
// for a previously-served entry. Best-effort on each step: an error in one
// sub-step is returned but we still try the rest so a retry can converge.
func (r *Reactor) ensureAbsent(ctx context.Context, prior *Served) error {
	var firstErr error
	if prior.ExportID != 0 {
		if err := r.ganesha.RemoveExport(prior.ExportID); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("ganesha RemoveExport: %w", err)
		}
	}
	if prior.MountPath != "" {
		if err := r.mounts.Unmount(prior.MountPath); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(prior.MountPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
			// Not fatal — empty dir removal is nice-to-have.
			log.WithError(err).WithField("path", prior.MountPath).Debug("nfsreactor: rmdir failed")
		}
	}
	if prior.ServiceName != "" && prior.ServiceNS != "" {
		if err := r.slices.Delete(ctx, prior.ServiceName, prior.ServiceNS); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.state.Delete(prior.UID)
	return firstErr
}

// isLocallyReady returns true iff the LV has a replica config entry for our
// node and the overall LV is in VolumeStateReady. HwameiStor exposes
// per-replica state only on LocalVolumeReplica CRs; the LV config's
// VolumeReplica struct has no state field (verified against
// pkg/apis/hwameistor/v1alpha1/localvolume_types.go).
func (r *Reactor) isLocallyReady(lv *apisv1alpha1.LocalVolume) bool {
	if lv.Spec.Config == nil {
		return false
	}
	if !lv.Spec.Config.ExistReplicaOnNode(r.opts.NodeName) {
		return false
	}
	// LV.Status.State tracks the whole volume; require Ready to avoid
	// trying to mount a half-constructed replica.
	return lv.Status.State == apisv1alpha1.VolumeStateReady
}

// resolveDevicePath returns the absolute /dev/<pool>/<lv> path for the
// replica on this node. Preferred source is the LocalVolumeReplica's
// Status.DevicePath (already canonical, e.g. /dev/LocalStorage_PoolHDD/pvc-xxx).
// If the per-replica CR hasn't populated it yet, we fall back to building
// it from the LV's PoolName + name.
func (r *Reactor) resolveDevicePath(ctx context.Context, lv *apisv1alpha1.LocalVolume) (string, error) {
	lvrs := &apisv1alpha1.LocalVolumeReplicaList{}
	if err := r.opts.Client.List(ctx, lvrs); err != nil {
		return "", fmt.Errorf("list LocalVolumeReplicas: %w", err)
	}
	for i := range lvrs.Items {
		lvr := &lvrs.Items[i]
		if lvr.Spec.VolumeName != lv.Name {
			continue
		}
		if lvr.Spec.NodeName != r.opts.NodeName {
			continue
		}
		if lvr.Status.DevicePath != "" {
			return lvr.Status.DevicePath, nil
		}
		if lvr.Status.StoragePath != "" {
			return lvr.Status.StoragePath, nil
		}
	}
	// Fallback: /dev/<PoolName>/<LV-name>. This matches the format used by
	// HwameiStor's LVM executor (see executor_lvm.go).
	if lv.Spec.PoolName == "" {
		return "", fmt.Errorf("no replica device path available for LV %s and Spec.PoolName is empty", lv.Name)
	}
	return filepath.ToSlash(filepath.Join("/dev", lv.Spec.PoolName, lv.Name)), nil
}

// serviceNameFor returns the tenant Service name that fronts this LV's NFS
// exports. HwameiStor records the backing PVC name on the LV spec; the
// external controller that creates the RWX Service uses the same name so
// EndpointSlices can bind by label selector.
func serviceNameFor(lv *apisv1alpha1.LocalVolume) string {
	return lv.Spec.PersistentVolumeClaimName
}
