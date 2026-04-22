package rwxpvc

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// rwxConfig is the distilled input used by all builders. It is derived from
// the user PVC + its StorageClass inside Reconcile and then passed into the
// pure builders below, so the builders stay trivially unit-testable.
type rwxConfig struct {
	// UserPVCName is the RWX PVC name the user created.
	UserPVCName string
	// UserPVCNamespace is the namespace that PVC lives in. All generated
	// resources share this namespace.
	UserPVCNamespace string
	// UserPVCUID is the UID of the user PVC. Used to name the mirror PV
	// and the export path so they are globally unique.
	UserPVCUID types.UID
	// BackingStorageClass is the HwameiStor SC that should back the RWO PVC.
	BackingStorageClass string
	// Capacity is the requested capacity of the user PVC, copied onto the
	// backing PVC and the mirror PV.
	Capacity resource.Quantity
	// Squash is the optional NFS squash mode from the SC parameter
	// lvm.hwameistor.io/nfs-squash. Empty string means "unset" and the
	// mirror PV will not carry a squash mount option.
	Squash string
}

// BackingPVCName returns the deterministic name of the backing RWO PVC for
// a given user PVC name.
func BackingPVCName(userPVCName string) string {
	return userPVCName + BackingPVCSuffix
}

// ServiceName returns the deterministic name of the NFS Service for a given
// user PVC name. We reuse the backing PVC name so everything the reactor
// needs to look up shares a single handle.
func ServiceName(userPVCName string) string {
	return BackingPVCName(userPVCName)
}

// MirrorPVName returns the deterministic name of the mirror NFS PV for a
// given user PVC UID.
func MirrorPVName(userPVCUID types.UID) string {
	return MirrorPVPrefix + string(userPVCUID)
}

// ExportPath returns the on-disk path inside the reactor pod where the
// backing LV is mounted and Ganesha serves from (the 'Path' attribute in
// the EXPORT{} block). This is the server-side view.
func ExportPath(userPVCUID types.UID) string {
	return ExportPathPrefix + string(userPVCUID)
}

// ExportPseudoPath returns the NFSv4 pseudo path that clients mount.
// This must match the 'Pseudo' attribute Ganesha publishes for the
// export — NOT the real on-disk path. See RenderExportConfig in the
// reactor package where Pseudo is set to "/<uid>".
func ExportPseudoPath(userPVCUID types.UID) string {
	return "/" + string(userPVCUID)
}

// nfsVolumeHandle returns the standard csi-driver-nfs volumeHandle format:
// <server>#<share>#<optional sub dir>. `share` is the NFSv4 pseudo path,
// since that is what clients actually mount — not the real path.
func nfsVolumeHandle(clusterIP string, uid types.UID) string {
	return fmt.Sprintf("%s#%s#", clusterIP, ExportPseudoPath(uid))
}

// BuildBackingPVC builds the RWO PVC that actually stores the data. It is
// sized to match the user PVC and uses the backing storage class declared
// on the RWX SC.
func BuildBackingPVC(cfg rwxConfig) *corev1.PersistentVolumeClaim {
	sc := cfg.BackingStorageClass
	pvc := &corev1.PersistentVolumeClaim{
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
	return pvc
}

// BuildService builds the ClusterIP Service that will expose NFS for the
// backing volume. Note: this Service has no selector on purpose. An
// out-of-cluster reactor manages EndpointSlices for it so it always points
// at whichever node currently hosts the backing LocalVolume replica.
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
			// No selector: EndpointSlices are managed externally.
			Ports: []corev1.ServicePort{
				{
					Name:     "nfs",
					Port:     2049,
					Protocol: corev1.ProtocolTCP,
				},
				{
					Name:     "mountd",
					Port:     20048,
					Protocol: corev1.ProtocolTCP,
				},
				{
					Name:     "rpcbind",
					Port:     111,
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildMirrorPV builds the NFS-backed PersistentVolume that the user's RWX
// PVC will bind to. The PV is pre-bound (claimRef) so the scheduler cannot
// race us into binding to some other PVC.
func BuildMirrorPV(cfg rwxConfig, svc *corev1.Service) *corev1.PersistentVolume {
	clusterIP := ""
	if svc != nil {
		clusterIP = svc.Spec.ClusterIP
	}

	mountOptions := []string{"nfsvers=4.1"}
	if cfg.Squash != "" {
		mountOptions = append(mountOptions, cfg.Squash)
	}

	pv := &corev1.PersistentVolume{
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
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			MountOptions:                  mountOptions,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           NFSCSIDriver,
					VolumeHandle:     nfsVolumeHandle(clusterIP, cfg.UserPVCUID),
					VolumeAttributes: map[string]string{
						"server": clusterIP,
						// NFSv4 pseudo path, not the real on-disk Path,
						// since the client mounts the pseudo-root.
						"share": ExportPseudoPath(cfg.UserPVCUID),
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
	return pv
}
