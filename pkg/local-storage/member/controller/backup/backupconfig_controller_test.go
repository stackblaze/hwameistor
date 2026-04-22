package backup

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hwameistorv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hwameistorv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBuildPVCBackup_UsesGlobalDefaults(t *testing.T) {
	r := &BackupConfigReconciler{}
	bc := &hwameistorv1alpha1.BackupConfig{
		Spec: hwameistorv1alpha1.BackupConfigSpec{
			Schedule: "0 2 * * *",
			Retention: hwameistorv1alpha1.RetentionSpec{
				KeepDaily:  7,
				KeepWeekly: 4,
			},
			KeepSnapshotAfterBackup: false,
			VolumeSnapshotClassName: "hwameistor-storage-lvm-snapshot",
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
	}
	pb := r.buildPVCBackup(bc, pvc)

	if pb.Spec.Schedule != "0 2 * * *" {
		t.Errorf("schedule not inherited: %q", pb.Spec.Schedule)
	}
	if pb.Spec.Retention.KeepDaily != 7 || pb.Spec.Retention.KeepWeekly != 4 {
		t.Errorf("retention not inherited: %+v", pb.Spec.Retention)
	}
	if pb.Spec.VolumeSnapshotClassName != "hwameistor-storage-lvm-snapshot" {
		t.Errorf("snapshot class not inherited: %q", pb.Spec.VolumeSnapshotClassName)
	}
	if pb.Labels[BackupManagedByLabel] != BackupManagedByValue {
		t.Errorf("missing managed-by label")
	}
	if pb.Spec.PVCName != "data" {
		t.Errorf("pvcName wrong: %q", pb.Spec.PVCName)
	}
}

func TestBuildPVCBackup_AnnotationsOverrideDefaults(t *testing.T) {
	r := &BackupConfigReconciler{}
	bc := &hwameistorv1alpha1.BackupConfig{
		Spec: hwameistorv1alpha1.BackupConfigSpec{
			Schedule:                "0 2 * * *",
			Retention:               hwameistorv1alpha1.RetentionSpec{KeepDaily: 7},
			KeepSnapshotAfterBackup: false,
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "critical",
			Annotations: map[string]string{
				BackupScheduleAnnotation:         "*/15 * * * *",
				BackupRetentionDailyAnnotation:   "30",
				BackupRetentionMonthlyAnnotation: "12",
				BackupKeepSnapshotAnnotation:     "true",
			},
		},
	}
	pb := r.buildPVCBackup(bc, pvc)

	if pb.Spec.Schedule != "*/15 * * * *" {
		t.Errorf("schedule not overridden: %q", pb.Spec.Schedule)
	}
	if pb.Spec.Retention.KeepDaily != 30 {
		t.Errorf("KeepDaily not overridden: %d", pb.Spec.Retention.KeepDaily)
	}
	if pb.Spec.Retention.KeepMonthly != 12 {
		t.Errorf("KeepMonthly not applied: %d", pb.Spec.Retention.KeepMonthly)
	}
	if !pb.Spec.KeepSnapshotAfterBackup {
		t.Errorf("KeepSnapshotAfterBackup not flipped by annotation")
	}
}

func TestBuildPVCBackup_MalformedAnnotationFallsBack(t *testing.T) {
	r := &BackupConfigReconciler{}
	bc := &hwameistorv1alpha1.BackupConfig{
		Spec: hwameistorv1alpha1.BackupConfigSpec{
			Retention: hwameistorv1alpha1.RetentionSpec{KeepDaily: 7},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				BackupRetentionDailyAnnotation: "not-a-number",
			},
		},
	}
	pb := r.buildPVCBackup(bc, pvc)
	if pb.Spec.Retention.KeepDaily != 7 {
		t.Errorf("malformed annotation should fall back to default, got %d", pb.Spec.Retention.KeepDaily)
	}
}

func TestPVCMatches_SkipAnnotation(t *testing.T) {
	r := &BackupConfigReconciler{client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
	bc := &hwameistorv1alpha1.BackupConfig{}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{BackupEnabledAnnotation: "false"},
		},
	}
	ok, err := r.pvcMatches(context.Background(), bc, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("annotation=false should skip")
	}
}

func TestPVCMatches_SkipsNonHwameistorStorageClass(t *testing.T) {
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "ebs"},
		Provisioner: "ebs.csi.aws.com",
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(sc).Build()
	r := &BackupConfigReconciler{client: c}
	bc := &hwameistorv1alpha1.BackupConfig{}
	ebs := "ebs"
	pvc := &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &ebs},
	}
	ok, err := r.pvcMatches(context.Background(), bc, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("non-hwameistor provisioner should not match")
	}
}

func TestPVCMatches_SelectorRespected(t *testing.T) {
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "hwameistor"},
		Provisioner: HwameistorLVMCSIProvisioner,
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(sc).Build()
	r := &BackupConfigReconciler{client: c}
	bc := &hwameistorv1alpha1.BackupConfig{
		Spec: hwameistorv1alpha1.BackupConfigSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"backup": "yes"}},
		},
	}
	scName := "hwameistor"
	unlabeled := &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	labeled := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"backup": "yes"}},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	if ok, err := r.pvcMatches(context.Background(), bc, unlabeled); err != nil || ok {
		t.Errorf("unlabeled should not match; ok=%v err=%v", ok, err)
	}
	if ok, err := r.pvcMatches(context.Background(), bc, labeled); err != nil || !ok {
		t.Errorf("labeled PVC should match; ok=%v err=%v", ok, err)
	}
}

func TestPVCBackupSpecEqual(t *testing.T) {
	a := hwameistorv1alpha1.PVCBackupSpec{
		PVCName:   "data",
		Schedule:  "0 * * * *",
		Retention: hwameistorv1alpha1.RetentionSpec{KeepDaily: 7},
	}
	b := a
	if !pvcBackupSpecEqual(a, b) {
		t.Fatal("identical specs should be equal")
	}
	b.Schedule = "* * * * *"
	if pvcBackupSpecEqual(a, b) {
		t.Fatal("different schedule should differ")
	}
	b = a
	b.Retention.KeepDaily = 8
	if pvcBackupSpecEqual(a, b) {
		t.Fatal("different retention should differ")
	}
}

// unused reference to silence the linter if resource.Quantity gets dropped later.
var _ = resource.MustParse
