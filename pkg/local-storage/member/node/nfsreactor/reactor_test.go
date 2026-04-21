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

func buildReactor(t *testing.T, objs ...client.Object) (*Reactor, *fakeMount, *fakeGanesha, *fakeSlices) {
	t.Helper()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	fm, fg, fs := newFakeMount(), newFakeGanesha(), newFakeSlices()
	r := NewForTest(Options{
		Client:       c,
		NodeName:     testNode,
		PodIP:        "10.1.2.3",
		PodNamespace: "hwameistor",
		ExportRoot:   "/srv/exports",
	}, fm, fg, fs)
	return r, fm, fg, fs
}

func req() reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: testLV}}
}

// --- tests ---

func TestReconcile_AddsExportWhenEligible(t *testing.T) {
	r, fm, fg, fs := buildReactor(t, makeLV(true, true, true), makeLVR())
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
	r, fm, fg, fs := buildReactor(t, makeLV(false, true, true), makeLVR())
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count()+fg.liveCount()+fs.liveCount() != 0 {
		t.Fatalf("expected no side effects, got m=%d g=%d s=%d", fm.count(), fg.liveCount(), fs.liveCount())
	}
}

func TestReconcile_NoOpWhenReplicaOnDifferentNode(t *testing.T) {
	r, fm, fg, fs := buildReactor(t, makeLV(true, false, true))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fm.count()+fg.liveCount()+fs.liveCount() != 0 {
		t.Fatalf("expected no side effects, got m=%d g=%d s=%d", fm.count(), fg.liveCount(), fs.liveCount())
	}
}

func TestReconcile_RemovesExportWhenLVDeleted(t *testing.T) {
	r, fm, fg, fs := buildReactor(t, makeLV(true, true, true), makeLVR())
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
	r, fm, fg, fs := buildReactor(t, makeLV(true, true, true), makeLVR())
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
