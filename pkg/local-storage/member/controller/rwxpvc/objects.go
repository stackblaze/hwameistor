package rwxpvc

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// rwxConfig is the distilled input the builders consume. Kept separate from
// the Kubernetes types so the builders are trivially unit-testable.
type rwxConfig struct {
	UserPVCName         string
	UserPVCNamespace    string
	UserPVCUID          types.UID
	BackingStorageClass string
	Capacity            resource.Quantity
	// Squash comes from SC param lvm.hwameistor.io/nfs-squash. Empty =
	// omit the mount option.
	Squash string
}

func BackingPVCName(userPVCName string) string { return userPVCName + BackingPVCSuffix }

// ServiceName reuses BackingPVCName so the reactor only needs one handle.
func ServiceName(userPVCName string) string { return BackingPVCName(userPVCName) }

func MirrorPVName(userPVCUID types.UID) string { return MirrorPVPrefix + string(userPVCUID) }

// ExportPath is the server-side on-disk path (Ganesha Path attribute).
func ExportPath(userPVCUID types.UID) string { return ExportPathPrefix + string(userPVCUID) }

// ExportPseudoPath is what NFSv4 clients mount (Ganesha Pseudo attribute).
// Must match RenderExportConfig in the reactor package.
func ExportPseudoPath(userPVCUID types.UID) string { return "/" + string(userPVCUID) }

// nfsVolumeHandle: csi-driver-nfs format "<server>#<share>#". `share` is
// the pseudo path, since that's what the client actually mounts.
func nfsVolumeHandle(clusterIP string, uid types.UID) string {
	return fmt.Sprintf("%s#%s#", clusterIP, ExportPseudoPath(uid))
}

func BuildBackingPVC(cfg rwxConfig) *corev1.PersistentVolumeClaim {
	sc := cfg.BackingStorageClass
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BackingPVCName(cfg.UserPVCName),
			Namespace: cfg.UserPVCNamespace,
			Labels: map[string]string{
				"hwameistor.io/rwx-backing-for": cfg.UserPVCName,
			},
			Annotations: map[string]string{
				RWXExportAnnotation: string(cfg.UserPVCUID),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: cfg.Capacity,
				},
			},
		},
	}
}

// BuildService: ClusterIP with no selector — the reactor manages the
// EndpointSlice so it always points at the node hosting the LV replica.
func BuildService(cfg rwxConfig) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName(cfg.UserPVCName),
			Namespace: cfg.UserPVCNamespace,
			Labels: map[string]string{
				"hwameistor.io/rwx-service-for": cfg.UserPVCName,
			},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "nfs", Port: 2049, Protocol: corev1.ProtocolTCP},
				{Name: "mountd", Port: 20048, Protocol: corev1.ProtocolTCP},
				{Name: "rpcbind", Port: 111, Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// BuildMirrorPV is pre-bound (claimRef) so the scheduler can't race us
// into binding to a different PVC.
func BuildMirrorPV(cfg rwxConfig, svc *corev1.Service) *corev1.PersistentVolume {
	clusterIP := ""
	if svc != nil {
		clusterIP = svc.Spec.ClusterIP
	}

	mountOptions := []string{"nfsvers=4.1"}
	if cfg.Squash != "" {
		mountOptions = append(mountOptions, cfg.Squash)
	}

	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: MirrorPVName(cfg.UserPVCUID),
			Labels: map[string]string{
				"hwameistor.io/rwx-mirror-for": cfg.UserPVCName,
			},
			Annotations: map[string]string{
				RWXExportAnnotation: string(cfg.UserPVCUID),
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: cfg.Capacity,
			},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			MountOptions:                  mountOptions,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       NFSCSIDriver,
					VolumeHandle: nfsVolumeHandle(clusterIP, cfg.UserPVCUID),
					VolumeAttributes: map[string]string{
						"server": clusterIP,
						"share":  ExportPseudoPath(cfg.UserPVCUID),
					},
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:       "PersistentVolumeClaim",
				APIVersion: "v1",
				Name:       cfg.UserPVCName,
				Namespace:  cfg.UserPVCNamespace,
				UID:        cfg.UserPVCUID,
			},
		},
	}
}
