// Package rwxpvc implements a reconciler that watches PersistentVolumeClaims
// and turns RWX PVCs backed by the lvm.hwameistor.io/rwx provisioner into
// an NFS-exposed HwameiStor volume.
//
// For each matching PVC the reconciler provisions a backing chain:
//
//  1. A backing RWO PVC using a regular HwameiStor storage class (specified
//     via the SC parameter lvm.hwameistor.io/backing-storage-class).
//  2. A headless ClusterIP Service without a selector. An out-of-cluster
//     reactor is responsible for managing EndpointSlices for this Service
//     and pointing them at the node currently hosting the backing LocalVolume
//     replica, so we intentionally leave the selector empty here.
//  3. A mirror PersistentVolume using the nfs.csi.k8s.io CSI driver that
//     binds to the user's RWX PVC.
//  4. An annotation hwameistor.io/rwx-export on the backing PVC's bound
//     LocalVolume, carrying the user PVC UID so node-side reactors can
//     configure the NFS export.
//  5. A finalizer hwameistor.io/rwx-pvc on the user PVC so the backing
//     resources can be torn down in reverse order on deletion.
//
// The reconciler is additive: non-matching PVCs are ignored, and the
// wiring file only registers the controller when Enabled is true.
package rwxpvc
