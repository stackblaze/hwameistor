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
	"github.com/hwameistor/hwameistor/pkg/local-storage/member/rwx"
)

// ExportAnnotation re-exports the shared constant so internal call sites
// here keep working; new code should import pkg/local-storage/member/rwx
// directly.
const ExportAnnotation = rwx.ExportAnnotation

type Options struct {
	Client         client.Client
	NodeName       string
	PodIP          string
	PodNamespace   string
	DbusAddr       string
	ExportRoot     string
	ResyncInterval time.Duration
}

type Reactor struct {
	opts    Options
	state   *State
	mounts  MountClient
	ganesha GaneshaClient
	slices  EndpointSliceClient
	drbd    DRBDClient
}

func New(opts Options) *Reactor {
	return &Reactor{
		opts:    opts,
		state:   NewState(),
		mounts:  NewRealMountClient(),
		ganesha: NewGaneshaDBusClient(opts.DbusAddr),
		slices:  NewEndpointSliceClient(opts.Client, opts.NodeName, opts.PodIP),
		drbd:    NewDRBDClient(),
	}
}

// NewForTest injects fakes.
func NewForTest(opts Options, m MountClient, g GaneshaClient, s EndpointSliceClient, d DRBDClient) *Reactor {
	return &Reactor{opts: opts, state: NewState(), mounts: m, ganesha: g, slices: s, drbd: d}
}

func (r *Reactor) SetupWithManager(mgr manager.Manager) error {
	c, err := controller.New("nfs-reactor", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("build controller: %w", err)
	}
	if err := c.Watch(&source.Kind{Type: &apisv1alpha1.LocalVolume{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return fmt.Errorf("watch LocalVolume: %w", err)
	}
	// Replicas belong to an LV; re-enqueue the owner.
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

	if err := mgr.Add(manager.RunnableFunc(r.runResync)); err != nil {
		return fmt.Errorf("add resync runnable: %w", err)
	}
	return nil
}

// runResync picks up missed events and catches out-of-band drift (manual
// umount, Ganesha restart, etc).
func (r *Reactor) runResync(ctx context.Context) error {
	interval := r.opts.ResyncInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
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

// Reconcile brings on-node state (mount, Ganesha export, EndpointSlice) in
// line with the LV's annotation and replica placement. Idempotent.
func (r *Reactor) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	lg := log.WithField("lv", req.Name)

	lv := &apisv1alpha1.LocalVolume{}
	err := r.opts.Client.Get(ctx, types.NamespacedName{Name: req.Name}, lv)
	if apierrors.IsNotFound(err) {
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
	ready, err := r.isLocallyReady(lv)
	if err != nil {
		return reconcile.Result{}, err
	}
	eligible := uid != "" && ready

	if !eligible {
		if prior := r.state.GetByLV(lv.Name); prior != nil {
			lg.WithField("uid", prior.UID).Info("LV no longer eligible on this node, removing export")
			if terr := r.ensureAbsent(ctx, prior); terr != nil {
				return reconcile.Result{}, terr
			}
		}
		return reconcile.Result{}, nil
	}

	device, err := r.resolveDevicePath(ctx, lv)
	if err != nil {
		return reconcile.Result{}, err
	}
	// Status.PublishedFSType is only set when NodeStageVolume runs, which
	// never happens for RWX backing volumes — no pod mounts them directly.
	fsType := lv.Status.PublishedFSType
	if fsType == "" {
		fsType = "xfs"
	}
	serviceName := serviceNameFor(lv)
	serviceNS := lv.Spec.PersistentVolumeClaimNamespace
	if serviceNS == "" {
		serviceNS = r.opts.PodNamespace
	}
	mountPath := filepath.Join(r.opts.ExportRoot, uid)

	existing := r.state.Get(uid)
	if existing != nil && existing.ExportID != 0 &&
		existing.Device == device && existing.MountPath == mountPath &&
		existing.ServiceName == serviceName && existing.ServiceNS == serviceNS {
		if err := r.slices.Put(ctx, serviceName, serviceNS); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if err := r.mounts.Mount(device, mountPath, fsType); err != nil {
		return reconcile.Result{}, err
	}

	exportID := r.state.AllocateID(uid)
	cfg := RenderExportConfig(exportID, mountPath, uid)
	// AddExport needs a config file path, not inline text. Write alongside
	// the mountpoint so ganesha (same emptyDir) can read it.
	cfgPath := filepath.Join(r.opts.ExportRoot, uid+".conf")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		return reconcile.Result{}, fmt.Errorf("mkdir ExportRoot: %w", err)
	}
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		return reconcile.Result{}, fmt.Errorf("write ganesha export config: %w", err)
	}
	// Don't pre-RemoveExport: Ganesha v6.5 segfaults if you remove an
	// unknown id, and AddExport already tolerates "already added" replies.
	// See https://github.com/nfs-ganesha/nfs-ganesha/issues/166.
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

// ensureAbsent is best-effort: on any sub-step failure we still attempt
// the rest so a subsequent retry can converge.
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
		if err := os.Remove(prior.MountPath); err != nil && !os.IsNotExist(err) {
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

// isLocallyReady decides whether this node should serve the export for
// the given LV.
//
// Single-replica (non-HA): if the replica is on this node and the LV is
// Ready, we serve.
//
// Multi-replica (HA, DRBD): we serve iff we're the DRBD Primary. Both
// nodes satisfy "replica is on me" but only the Primary can open the
// device r/w. If nobody is Primary yet (fresh volume, never opened),
// the lex-first replica node elects itself so DRBD has someone to
// auto-promote. Other nodes will see themselves Secondary on the next
// reconcile and back off.
//
// HwameiStor's LocalVolume.Spec.Config.Replicas[i].Primary field is a
// static scheduling hint set at creation time — not the live DRBD
// role — so we ask DRBD directly via drbdsetup.
func (r *Reactor) isLocallyReady(lv *apisv1alpha1.LocalVolume) (bool, error) {
	if lv.Status.State != apisv1alpha1.VolumeStateReady {
		return false, nil
	}
	if lv.Spec.Config == nil {
		return false, nil
	}
	if !lv.Spec.Config.ExistReplicaOnNode(r.opts.NodeName) {
		return false, nil
	}
	if !lv.IsHighAvailability() {
		return true, nil
	}
	if r.drbd == nil {
		return false, fmt.Errorf("HA LV %s: reactor has no DRBDClient", lv.Name)
	}
	primary, err := r.drbd.IsPrimary(lv.Name)
	if err != nil {
		return false, err
	}
	if primary {
		return true, nil
	}
	// Not Primary yet. If DRBD hasn't promoted anyone, the lex-first
	// replica node attempts to serve so the mount triggers auto-promote.
	// Peers will see Secondary on next reconcile.
	if r.shouldElectSelf(lv) {
		return true, nil
	}
	return false, nil
}

// shouldElectSelf returns true on the lex-first replica hostname,
// used only to break the bootstrap tie on a fresh HA volume.
func (r *Reactor) shouldElectSelf(lv *apisv1alpha1.LocalVolume) bool {
	first := ""
	for _, rep := range lv.Spec.Config.Replicas {
		if first == "" || rep.Hostname < first {
			first = rep.Hostname
		}
	}
	return first == r.opts.NodeName
}

// resolveDevicePath returns the /dev path of this node's replica.
// Falls back to /dev/<PoolName>/<LV-name> if the LocalVolumeReplica CR
// hasn't published the canonical path yet.
func (r *Reactor) resolveDevicePath(ctx context.Context, lv *apisv1alpha1.LocalVolume) (string, error) {
	lvrs := &apisv1alpha1.LocalVolumeReplicaList{}
	if err := r.opts.Client.List(ctx, lvrs); err != nil {
		return "", fmt.Errorf("list LocalVolumeReplicas: %w", err)
	}
	for i := range lvrs.Items {
		lvr := &lvrs.Items[i]
		if lvr.Spec.VolumeName != lv.Name || lvr.Spec.NodeName != r.opts.NodeName {
			continue
		}
		if lvr.Status.DevicePath != "" {
			return lvr.Status.DevicePath, nil
		}
		if lvr.Status.StoragePath != "" {
			return lvr.Status.StoragePath, nil
		}
	}
	if lv.Spec.PoolName == "" {
		return "", fmt.Errorf("no replica device path available for LV %s", lv.Name)
	}
	return filepath.ToSlash(filepath.Join("/dev", lv.Spec.PoolName, lv.Name)), nil
}

func serviceNameFor(lv *apisv1alpha1.LocalVolume) string {
	return lv.Spec.PersistentVolumeClaimName
}
