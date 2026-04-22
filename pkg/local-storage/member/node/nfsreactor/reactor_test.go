package nfsreactor

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apisv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

// --- fakes ---

type fakeMount struct {
	mu      sync.Mutex
	mounted map[string]string // target -> device
}

func newFakeMount() *fakeMount { return &fakeMount{mounted: map[string]string{}} }

func (f *fakeMount) IsMounted(target string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.mounted[filepath.Clean(target)]
	return ok, nil
}
func (f *fakeMount) Mount(device, target, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounted[filepath.Clean(target)] = device
	return nil
}
func (f *fakeMount) Unmount(target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.mounted, filepath.Clean(target))
	return nil
}
func (f *fakeMount) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mounted)
}

type fakeGanesha struct {
	mu    sync.Mutex
	adds  int
	rems  int
	alive map[uint16]string // exportID -> path
}

func newFakeGanesha() *fakeGanesha { return &fakeGanesha{alive: map[uint16]string{}} }

func (f *fakeGanesha) AddExport(id uint16, path, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds++
	f.alive[id] = path
	return nil
}
func (f *fakeGanesha) RemoveExport(id uint16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rems++
	delete(f.alive, id)
	return nil
}
func (f *fakeGanesha) liveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.alive)
}

type fakeSlices struct {
	mu      sync.Mutex
	puts    int
	deletes int
	alive   map[string]bool // "<ns>/<svc>"
}

func newFakeSlices() *fakeSlices { return &fakeSlices{alive: map[string]bool{}} }

func (f *fakeSlices) Put(_ context.Context, svc, ns string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	f.alive[ns+"/"+svc] = true
	return nil
}
func (f *fakeSlices) Delete(_ context.Context, svc, ns string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	delete(f.alive, ns+"/"+svc)
	return nil
}
func (f *fakeSlices) liveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.alive)
}

// fakeDRBD: every resource is Primary unless explicitly added to
// secondary[]; non-HA tests don't touch it since isLocallyReady
// short-circuits on Convertible==false.
type fakeDRBD struct {
	mu        sync.Mutex
	secondary map[string]bool
	err       error
}

func newFakeDRBD() *fakeDRBD { return &fakeDRBD{secondary: map[string]bool{}} }

func (f *fakeDRBD) IsPrimary(resource string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false, f.err
	}
	return !f.secondary[resource], nil
}

func (f *fakeDRBD) setSecondary(resource string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.secondary[resource] = true
}

// --- helpers ---

const (
	testNode = "node-a"
	testUID  = "pvc-uid-1234"
	testLV   = "pvc-aaaa"
	testPool = "LocalStorage_PoolHDD"
	testNS   = "tenant-x"
	testPVC  = "my-rwx"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := apisv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1: %v", err)
	}
	if err := discoveryv1.AddToScheme(s); err != nil {
		t.Fatalf("discoveryv1: %v", err)
	}
	return s
}

func makeLV(withAnnotation, withLocalReplica, ready bool) *apisv1alpha1.LocalVolume {
	lv := &apisv1alpha1.LocalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: testLV},
		Spec: apisv1alpha1.LocalVolumeSpec{
			PoolName:                       testPool,
			PersistentVolumeClaimName:      testPVC,
			PersistentVolumeClaimNamespace: testNS,
			Config:                         &apisv1alpha1.VolumeConfig{},
		},
	}
	if withAnnotation {
		lv.Annotations = map[string]string{ExportAnnotation: testUID}
	}
	if withLocalReplica {
		lv.Spec.Config.Replicas = []apisv1alpha1.VolumeReplica{
			{ID: 1, Hostname: testNode, IP: "10.0.0.1"},
		}
	} else {
		lv.Spec.Config.Replicas = []apisv1alpha1.VolumeReplica{
			{ID: 1, Hostname: "other-node", IP: "10.0.0.2"},
		}
	}
	if ready {
		lv.Status.State = apisv1alpha1.VolumeStateReady
		lv.Status.PublishedFSType = "ext4"
	}
	return lv
}

func makeLVR() *apisv1alpha1.LocalVolumeReplica {
	return &apisv1alpha1.LocalVolumeReplica{
		ObjectMeta: metav1.ObjectMeta{Name: testLV + "-" + testNode},
		Spec: apisv1alpha1.LocalVolumeReplicaSpec{
			VolumeName: testLV,
			NodeName:   testNode,
			PoolName:   testPool,
		},
		Status: apisv1alpha1.LocalVolumeReplicaStatus{
			State:      apisv1alpha1.VolumeReplicaStateReady,
			DevicePath: "/dev/" + testPool + "/" + testLV,
		},
	}
}

func buildReactor(t *testing.T, objs ...client.Object) (*Reactor, *fakeMount, *fakeGanesha, *fakeSlices, *fakeDRBD) {
	t.Helper()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	fm, fg, fs, fd := newFakeMount(), newFakeGanesha(), newFakeSlices(), newFakeDRBD()
	// ExportRoot must be a writable dir — the reactor writes per-export
	// Ganesha config files there. Using t.TempDir keeps tests hermetic and
	// cross-platform.
	r := NewForTest(Options{
		Client:       c,
		NodeName:     testNode,
		PodIP:        "10.1.2.3",
		PodNamespace: "hwameistor",
		ExportRoot:   t.TempDir(),
	}, fm, fg, fs, fd)
	return r, fm, fg, fs, fd
}

func req() reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: testLV}}
}

// --- tests ---

func TestReconcile_AddsExportWhenEligible(t *testing.T) {
	r, fm, fg, fs, _ := buildReactor(t, makeLV(true, true, true), makeLVR())
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count() != 1 {
		t.Fatalf("want 1 mount, got %d", fm.count())
	}
	if fg.liveCount() != 1 {
		t.Fatalf("want 1 live export, got %d", fg.liveCount())
	}
	if fs.liveCount() != 1 {
		t.Fatalf("want 1 endpointslice, got %d", fs.liveCount())
	}
	if srv := r.state.Get(testUID); srv == nil || srv.ExportID == 0 {
		t.Fatalf("expected state entry with non-zero ExportID, got %+v", srv)
	}
}

func TestReconcile_NoOpWhenMissingAnnotation(t *testing.T) {
	r, fm, fg, fs, _ := buildReactor(t, makeLV(false, true, true), makeLVR())
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count()+fg.liveCount()+fs.liveCount() != 0 {
		t.Fatalf("expected no side effects, got m=%d g=%d s=%d", fm.count(), fg.liveCount(), fs.liveCount())
	}
}

func TestReconcile_NoOpWhenReplicaOnDifferentNode(t *testing.T) {
	r, fm, fg, fs, _ := buildReactor(t, makeLV(true, false, true))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count()+fg.liveCount()+fs.liveCount() != 0 {
		t.Fatalf("expected no side effects, got m=%d g=%d s=%d", fm.count(), fg.liveCount(), fs.liveCount())
	}
}

func TestReconcile_RemovesExportWhenLVDeleted(t *testing.T) {
	r, fm, fg, fs, _ := buildReactor(t, makeLV(true, true, true), makeLVR())
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, req()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Now delete the LV from the fake client and reconcile again.
	lv := &apisv1alpha1.LocalVolume{}
	if err := r.opts.Client.Get(ctx, types.NamespacedName{Name: testLV}, lv); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := r.opts.Client.Delete(ctx, lv); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := r.Reconcile(ctx, req()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if fm.count() != 0 || fg.liveCount() != 0 || fs.liveCount() != 0 {
		t.Fatalf("expected teardown, got m=%d g=%d s=%d", fm.count(), fg.liveCount(), fs.liveCount())
	}
	if srv := r.state.Get(testUID); srv != nil {
		t.Fatalf("expected state cleared, got %+v", srv)
	}
}

func TestReconcile_Idempotent(t *testing.T) {
	r, fm, fg, fs, _ := buildReactor(t, makeLV(true, true, true), makeLVR())
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, req()); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if fm.count() != 1 {
		t.Fatalf("mount called multiple times: %d", fm.count())
	}
	if fg.adds != 1 {
		t.Fatalf("AddExport should be called once, got %d", fg.adds)
	}
	// EndpointSlice Put is cheap and may be called each round; that's fine.
	if fs.liveCount() != 1 {
		t.Fatalf("want 1 endpointslice, got %d", fs.liveCount())
	}
}

// --- v2 (HA / convertible) tests ---

// makeHALV builds an eligible-by-spec HA (replicaNumber=2) LV with a
// local replica. DRBD role is decided by fakeDRBD per-test. The peer
// replica is added at a lex-LATER hostname so testNode is the election
// winner by default (simplifies bootstrap tests).
func makeHALV() *apisv1alpha1.LocalVolume {
	lv := makeLV(true, true, true)
	lv.Spec.ReplicaNumber = 2
	lv.Spec.Config.Replicas = append(lv.Spec.Config.Replicas,
		apisv1alpha1.VolumeReplica{ID: 2, Hostname: "peer-z", IP: "10.0.0.9"},
	)
	return lv
}

func TestReconcile_HA_PublishesWhenPrimary(t *testing.T) {
	// fakeDRBD defaults every resource to Primary → this node serves.
	r, fm, fg, fs, _ := buildReactor(t, makeHALV(), makeLVR())
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count() != 1 {
		t.Fatalf("HA Primary should mount: got %d", fm.count())
	}
	if fg.liveCount() != 1 {
		t.Fatalf("HA Primary should AddExport: got %d", fg.liveCount())
	}
	if fs.liveCount() != 1 {
		t.Fatalf("HA Primary should Put EndpointSlice: got %d", fs.liveCount())
	}
}

func TestReconcile_HA_SkipsWhenSecondaryAndNotElected(t *testing.T) {
	// testNode is "node-a" (default); give the peer a lex-EARLIER name
	// so election goes to the peer, not us. Then mark testLV Secondary:
	// we should truly back off.
	lv := makeLV(true, true, true)
	lv.Spec.ReplicaNumber = 2
	lv.Spec.Config.Replicas = append(lv.Spec.Config.Replicas,
		apisv1alpha1.VolumeReplica{ID: 2, Hostname: "aaa-peer", IP: "10.0.0.9"},
	)
	r, fm, fg, fs, fd := buildReactor(t, lv, makeLVR())
	fd.setSecondary(testLV)

	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count() != 0 {
		t.Fatalf("HA Secondary must NOT mount: got %d", fm.count())
	}
	if fg.liveCount() != 0 {
		t.Fatalf("HA Secondary must NOT AddExport: got %d", fg.liveCount())
	}
	if fs.liveCount() != 0 {
		t.Fatalf("HA Secondary must NOT write EndpointSlice: got %d", fs.liveCount())
	}
}

func TestReconcile_HA_ElectsSelfOnBootstrap(t *testing.T) {
	// Fresh HA volume: DRBD says Secondary on every node. The lex-first
	// replica node attempts to mount — DRBD auto-promotes it on open().
	// testNode="node-a" is lex-first vs "peer-z" (see makeHALV).
	r, fm, fg, fs, fd := buildReactor(t, makeHALV(), makeLVR())
	fd.setSecondary(testLV)

	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count() != 1 {
		t.Fatalf("bootstrap elector should mount: got %d", fm.count())
	}
	if fg.liveCount() != 1 {
		t.Fatalf("bootstrap elector should AddExport: got %d", fg.liveCount())
	}
	if fs.liveCount() != 1 {
		t.Fatalf("bootstrap elector should Put EndpointSlice: got %d", fs.liveCount())
	}
}

// Promotion flow: a peer is election-winner + DRBD Primary, so this
// node stays Secondary and does nothing. Then DRBD auto-promotes this
// node (because the peer died and we opened the device). Next reconcile
// must start serving.
func TestReconcile_HA_TakesOverOnPromotion(t *testing.T) {
	// Make the peer lex-first so this node is NOT the bootstrap elector.
	lv := makeLV(true, true, true)
	lv.Spec.ReplicaNumber = 2
	lv.Spec.Config.Replicas = append(lv.Spec.Config.Replicas,
		apisv1alpha1.VolumeReplica{ID: 2, Hostname: "aaa-peer", IP: "10.0.0.9"},
	)
	r, _, fg, fs, fd := buildReactor(t, lv, makeLVR())
	fd.setSecondary(testLV)

	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if fg.liveCount() != 0 || fs.liveCount() != 0 {
		t.Fatalf("pre-promotion should not have served: exports=%d slices=%d", fg.liveCount(), fs.liveCount())
	}

	// Peer dies → DRBD auto-promotes us on the next open.
	fd.mu.Lock()
	fd.secondary = map[string]bool{}
	fd.mu.Unlock()

	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if fg.liveCount() != 1 || fs.liveCount() != 1 {
		t.Fatalf("post-promotion should serve: exports=%d slices=%d", fg.liveCount(), fs.liveCount())
	}
}

// Demotion flow: this node was Primary and serving; the peer becomes
// Primary (we got demoted). Peer is lex-first so we don't re-elect
// ourselves after the role flip. Next reconcile must tear down.
func TestReconcile_HA_TearsDownOnDemotion(t *testing.T) {
	lv := makeLV(true, true, true)
	lv.Spec.ReplicaNumber = 2
	lv.Spec.Config.Replicas = append(lv.Spec.Config.Replicas,
		apisv1alpha1.VolumeReplica{ID: 2, Hostname: "aaa-peer", IP: "10.0.0.9"},
	)
	r, fm, fg, fs, fd := buildReactor(t, lv, makeLVR())
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, req()); err != nil {
		t.Fatalf("setup reconcile: %v", err)
	}
	if fg.liveCount() != 1 {
		t.Fatalf("setup: want 1 export, got %d", fg.liveCount())
	}

	fd.setSecondary(testLV)

	if _, err := r.Reconcile(ctx, req()); err != nil {
		t.Fatalf("demotion reconcile: %v", err)
	}
	if fm.count() != 0 {
		t.Fatalf("demotion should unmount: got %d mounts", fm.count())
	}
	if fg.liveCount() != 0 {
		t.Fatalf("demotion should RemoveExport: got %d live", fg.liveCount())
	}
	if fs.liveCount() != 0 {
		t.Fatalf("demotion should delete EndpointSlice: got %d", fs.liveCount())
	}
}
