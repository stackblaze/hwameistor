package rwxpvc

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apisv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

const (
	testUserPVCName   = "data-rwx"
	testUserNamespace = "app"
	testUserUID       = "uid-1234"
	testBackingSC     = "hwameistor-hdd"
	testRWXSC         = "hwameistor-rwx"
)

func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := scheme.Scheme
	s.AddKnownTypes(apisv1alpha1.SchemeGroupVersion,
		&apisv1alpha1.LocalVolume{},
		&apisv1alpha1.LocalVolumeList{},
		&apisv1alpha1.LocalStorageNode{},
		&apisv1alpha1.LocalStorageNodeList{},
	)
	return s
}

func newRWXSC() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: testRWXSC},
		Provisioner: RWXProvisionerName,
		Parameters: map[string]string{
			RWXBackingStorageClassParam: testBackingSC,
		},
	}
}

func newUserPVC(accessModes []corev1.PersistentVolumeAccessMode, scName string) *corev1.PersistentVolumeClaim {
	sc := scName
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testUserPVCName,
			Namespace: testUserNamespace,
			UID:       types.UID(testUserUID),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: &sc,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
}

// newReconcilerForTest builds a Reconciler backed by a fake client seeded
// with the supplied objects.
func newReconcilerForTest(t *testing.T, objs ...client.Object) *Reconciler {
	t.Helper()
	s := buildScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &Reconciler{Client: c, Scheme: s}
}

// seedClusterIP writes a ClusterIP onto the Service the reconciler just
// created, since the fake client doesn't auto-allocate one.
func seedClusterIP(t *testing.T, r *Reconciler, name, ns, ip string) {
	t.Helper()
	svc := &corev1.Service{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, svc); err != nil {
		t.Fatalf("get svc: %v", err)
	}
	svc.Spec.ClusterIP = ip
	if err := r.Client.Update(context.Background(), svc); err != nil {
		t.Fatalf("update svc: %v", err)
	}
}

func reconcilePVC(t *testing.T, r *Reconciler) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: testUserPVCName, Namespace: testUserNamespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func TestReconcile_CreatesBackingChain(t *testing.T) {
	sc := newRWXSC()
	pvc := newUserPVC([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, testRWXSC)
	r := newReconcilerForTest(t, sc, pvc)

	// 1st pass: adds the finalizer, requeues.
	res := reconcilePVC(t, r)
	if !res.Requeue {
		t.Fatalf("expected requeue after adding finalizer, got %+v", res)
	}

	// After finalizer add, the backing PVC + Service should not yet exist.
	got := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: testUserPVCName, Namespace: testUserNamespace}, got); err != nil {
		t.Fatalf("get user pvc: %v", err)
	}
	if !containsFinalizer(got, RWXFinalizer) {
		t.Fatalf("finalizer not present: %v", got.Finalizers)
	}

	// 2nd pass: creates backing PVC and Service; Service has no ClusterIP
	// from the fake client so the reconciler will ask for a requeue before
	// building the PV.
	res = reconcilePVC(t, r)
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter waiting for ClusterIP, got %+v", res)
	}

	svcName := ServiceName(testUserPVCName)
	seedClusterIP(t, r, svcName, testUserNamespace, "10.0.0.42")

	// 3rd pass: Service now has a ClusterIP; mirror PV is built. The
	// backing PVC is not yet Bound so annotateLocalVolume requeues.
	res = reconcilePVC(t, r)
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter (backing PVC not bound), got %+v", res)
	}

	// Check backing PVC.
	backing := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: BackingPVCName(testUserPVCName), Namespace: testUserNamespace}, backing); err != nil {
		t.Fatalf("get backing pvc: %v", err)
	}
	if backing.Spec.StorageClassName == nil || *backing.Spec.StorageClassName != testBackingSC {
		t.Errorf("backing PVC SC mismatch: %+v", backing.Spec.StorageClassName)
	}
	if len(backing.Spec.AccessModes) != 1 || backing.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("backing PVC should be RWO, got %v", backing.Spec.AccessModes)
	}

	// Check Service — specifically, no selector.
	svc := &corev1.Service{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: svcName, Namespace: testUserNamespace}, svc); err != nil {
		t.Fatalf("get svc: %v", err)
	}
	if len(svc.Spec.Selector) != 0 {
		t.Errorf("service should have no selector, got %v", svc.Spec.Selector)
	}

	// Check mirror PV.
	pv := &corev1.PersistentVolume{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: MirrorPVName(types.UID(testUserUID))}, pv); err != nil {
		t.Fatalf("get mirror pv: %v", err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != NFSCSIDriver {
		t.Errorf("mirror PV should use nfs.csi.k8s.io driver, got %+v", pv.Spec.CSI)
	}
	// NFSv4 pseudo path — clients mount /<uid>, not the on-disk Path.
	wantHandle := "10.0.0.42#/" + testUserUID + "#"
	if pv.Spec.CSI.VolumeHandle != wantHandle {
		t.Errorf("volumeHandle mismatch: got %q want %q", pv.Spec.CSI.VolumeHandle, wantHandle)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != types.UID(testUserUID) {
		t.Errorf("mirror PV should be pre-bound to user PVC, got ClaimRef=%+v", pv.Spec.ClaimRef)
	}
}

func TestReconcile_NonMatchingSC_NoOp(t *testing.T) {
	otherSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "other"},
		Provisioner: "some.other.provisioner",
	}
	pvc := newUserPVC([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, "other")
	r := newReconcilerForTest(t, otherSC, pvc)

	reconcilePVC(t, r)

	// No backing resources should exist.
	backing := &corev1.PersistentVolumeClaim{}
	err := r.Client.Get(context.Background(), types.NamespacedName{Name: BackingPVCName(testUserPVCName), Namespace: testUserNamespace}, backing)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no backing PVC, err=%v", err)
	}
	svc := &corev1.Service{}
	err = r.Client.Get(context.Background(), types.NamespacedName{Name: ServiceName(testUserPVCName), Namespace: testUserNamespace}, svc)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no Service, err=%v", err)
	}

	// And no finalizer got added.
	got := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: testUserPVCName, Namespace: testUserNamespace}, got); err != nil {
		t.Fatalf("get user pvc: %v", err)
	}
	if containsFinalizer(got, RWXFinalizer) {
		t.Fatalf("finalizer should not be present for non-matching SC")
	}
}

// drive the reconciler until the backing chain is fully created (finalizer,
// backing PVC, Service + ClusterIP, mirror PV).
func driveToSteadyState(t *testing.T, r *Reconciler) {
	t.Helper()
	// pass 1: finalizer
	reconcilePVC(t, r)
	// pass 2: backing PVC + Service created, waits for ClusterIP
	reconcilePVC(t, r)
	seedClusterIP(t, r, ServiceName(testUserPVCName), testUserNamespace, "10.0.0.42")
	// pass 3: mirror PV built
	reconcilePVC(t, r)
}

func TestReconcile_DeletionCleansUp(t *testing.T) {
	sc := newRWXSC()
	pvc := newUserPVC([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, testRWXSC)
	r := newReconcilerForTest(t, sc, pvc)

	driveToSteadyState(t, r)

	// Ask the fake client to delete the PVC. Because the reconciler has
	// installed a finalizer, the fake client preserves the object with a
	// DeletionTimestamp — which is exactly what we want to test.
	got := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: testUserPVCName, Namespace: testUserNamespace}, got); err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if err := r.Client.Delete(context.Background(), got); err != nil {
		t.Fatalf("delete pvc: %v", err)
	}

	reconcilePVC(t, r)

	// Backing PVC, Service, mirror PV should all be gone.
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: BackingPVCName(testUserPVCName), Namespace: testUserNamespace}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Errorf("backing PVC should be deleted, err=%v", err)
	}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: ServiceName(testUserPVCName), Namespace: testUserNamespace}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Errorf("service should be deleted, err=%v", err)
	}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: MirrorPVName(types.UID(testUserUID))}, &corev1.PersistentVolume{}); !apierrors.IsNotFound(err) {
		t.Errorf("mirror PV should be deleted, err=%v", err)
	}

	// Finalizer should be gone — the fake client may or may not garbage
	// collect on its own once finalizers are empty + deletionTimestamp
	// is set, so look up either way.
	final := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: testUserPVCName, Namespace: testUserNamespace}, final); err == nil {
		if containsFinalizer(final, RWXFinalizer) {
			t.Errorf("finalizer still present: %v", final.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Errorf("unexpected error reading user PVC after cleanup: %v", err)
	}
}

func TestReconcile_Idempotent(t *testing.T) {
	sc := newRWXSC()
	pvc := newUserPVC([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, testRWXSC)
	r := newReconcilerForTest(t, sc, pvc)

	driveToSteadyState(t, r)

	// Capture the resourceVersions of the three backing objects.
	backing := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: BackingPVCName(testUserPVCName), Namespace: testUserNamespace}, backing); err != nil {
		t.Fatalf("get backing pvc: %v", err)
	}
	svc := &corev1.Service{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: ServiceName(testUserPVCName), Namespace: testUserNamespace}, svc); err != nil {
		t.Fatalf("get svc: %v", err)
	}
	pv := &corev1.PersistentVolume{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: MirrorPVName(types.UID(testUserUID))}, pv); err != nil {
		t.Fatalf("get mirror pv: %v", err)
	}

	// Run reconcile twice more — should be a no-op on the backing objects.
	reconcilePVC(t, r)
	reconcilePVC(t, r)

	backing2 := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: BackingPVCName(testUserPVCName), Namespace: testUserNamespace}, backing2); err != nil {
		t.Fatalf("get backing pvc (2): %v", err)
	}
	svc2 := &corev1.Service{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: ServiceName(testUserPVCName), Namespace: testUserNamespace}, svc2); err != nil {
		t.Fatalf("get svc (2): %v", err)
	}
	pv2 := &corev1.PersistentVolume{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: MirrorPVName(types.UID(testUserUID))}, pv2); err != nil {
		t.Fatalf("get mirror pv (2): %v", err)
	}

	if backing2.ResourceVersion != backing.ResourceVersion {
		t.Errorf("backing PVC mutated on idempotent reconcile (rv %s -> %s)", backing.ResourceVersion, backing2.ResourceVersion)
	}
	if svc2.ResourceVersion != svc.ResourceVersion {
		t.Errorf("service mutated on idempotent reconcile (rv %s -> %s)", svc.ResourceVersion, svc2.ResourceVersion)
	}
	if pv2.ResourceVersion != pv.ResourceVersion {
		t.Errorf("mirror PV mutated on idempotent reconcile (rv %s -> %s)", pv.ResourceVersion, pv2.ResourceVersion)
	}
}

func TestAnnotateLocalVolume_StampsAnnotation(t *testing.T) {
	sc := newRWXSC()
	pvc := newUserPVC([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, testRWXSC)

	backingName := BackingPVCName(testUserPVCName)
	pvName := "pvc-backing-uuid"
	backing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backingName,
			Namespace: testUserNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}
	lv := &apisv1alpha1.LocalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: apisv1alpha1.LocalVolumeSpec{
			PersistentVolumeClaimName:      backingName,
			PersistentVolumeClaimNamespace: testUserNamespace,
		},
	}
	r := newReconcilerForTest(t, sc, pvc, backing, lv)

	cfg := rwxConfig{
		UserPVCName:      testUserPVCName,
		UserPVCNamespace: testUserNamespace,
		UserPVCUID:       types.UID(testUserUID),
	}
	requeue, err := r.annotateLocalVolume(context.Background(), cfg)
	if err != nil {
		t.Fatalf("annotateLocalVolume: %v", err)
	}
	if requeue {
		t.Fatalf("did not expect requeue when LV exists")
	}

	got := &apisv1alpha1.LocalVolume{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: pvName}, got); err != nil {
		t.Fatalf("get lv: %v", err)
	}
	if got.Annotations[RWXExportAnnotation] != testUserUID {
		t.Errorf("annotation not set, got %q", got.Annotations[RWXExportAnnotation])
	}
}
