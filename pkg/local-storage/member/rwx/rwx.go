// Package rwx holds constants shared between the RWX PVC controller
// (pkg/local-storage/member/controller/rwxpvc) and the NFS reactor
// (pkg/local-storage/member/node/nfsreactor). The two components talk to
// each other through annotations and file-system conventions, so the
// strings they agree on live here — to drift one is to break both.
package rwx

// ExportAnnotation on a LocalVolume holds the user PVC UID. The
// controller stamps it after the backing PVC binds; the node-side reactor
// watches for it and serves that LV over NFS.
const ExportAnnotation = "hwameistor.io/rwx-export"

// ProvisionerName is the StorageClass provisioner that marks a PVC as
// RWX. No real CSI provisioner answers for it — the controller watches
// PVCs with this provisioner directly, and the user PVC binds to a
// mirror NFS PV created by the controller.
const ProvisionerName = "lvm.hwameistor.io/rwx"

// ExportRootDefault is the in-pod directory where the reactor mounts
// each backing LV. Also the `Path =` attribute in Ganesha EXPORT blocks.
const ExportRootDefault = "/srv/exports"

// AnnSelectedNode is the Kubernetes annotation the external-provisioner
// uses to tell a CSI driver which node to place a volume on when the
// scheduler made that decision. Duplicated from
// pkg/local-storage/member/csi so callers don't have to pull in the full
// CSI package (and its transitive Linux-only deps) just for this string.
const AnnSelectedNode = "volume.kubernetes.io/selected-node"
