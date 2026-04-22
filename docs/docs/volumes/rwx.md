---
sidebar_position: 12
sidebar_label: "ReadWriteMany (RWX)"
---

# ReadWriteMany (RWX) Volumes

HwameiStor volumes are node-local LVM logical volumes, so the CSI driver only
provides `ReadWriteOnce` natively. To expose `ReadWriteMany` to workloads
that need it, HwameiStor can layer NFS (via
[NFS-Ganesha](https://github.com/nfs-ganesha/nfs-ganesha)) on top of a
conventional RWO HwameiStor volume.

This page describes how the feature works, how to enable it, and what
trade-offs to expect.

## How it works

When a user creates a PVC that uses the RWX StorageClass
(`provisioner: lvm.hwameistor.io/rwx`) and requests `ReadWriteMany`,
HwameiStor provisions:

1. A **backing RWO PVC** bound to a normal HwameiStor StorageClass — this is
   where the actual data lives. HwameiStor provisions it on one node as
   usual.
2. A **selector-less ClusterIP Service** that the per-node NFS server
   DaemonSet points EndpointSlices at.
3. A static **NFS PersistentVolume** that references the Service and is
   bound to the user's RWX PVC. The PV uses the
   [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs)
   driver (`nfs.csi.k8s.io`), which must be installed in the cluster.

A privileged **`hwameistor-nfs-server` DaemonSet** runs one pod per storage
node. When a backing LocalVolume lands on a node, the `nfs-reactor` sidecar
on that node mounts the LV into the pod and asks Ganesha (via DBus) to add
an export. An EndpointSlice entry is written so the PVC's Service routes to
that pod.

Consumer pods on any node mount the NFS PV and get RWX semantics. The
backing PVC is never mounted directly by anything other than the Ganesha
pod.

## Prerequisites

- HwameiStor installed and working for RWO.
- [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs/blob/master/docs/install-csi-driver.md)
  installed. HwameiStor does not bundle it.
- A HwameiStor StorageClass you are willing to use as the RWO backing store
  (any standard `hwameistor-storage-lvm` class works).
- Cluster pod-security policy allows privileged pods in the HwameiStor
  namespace — the NFS server DaemonSet needs `privileged: true` to mount
  LVs inside the pod.

## Enabling the feature

Set `rwx.enabled=true` in the Helm values:

```yaml
rwx:
  enabled: true
  storageClass:
    enabled: true
    backingStorageClassName: hwameistor-storage-lvm
    reclaimPolicy: Delete
    allowVolumeExpansion: true
```

Upgrading with this value turned on:

- Adds the `hwameistor-nfs-server` DaemonSet (one pod per storage node)
- Creates the sample RWX StorageClass `hwameistor-storage-lvm-rwx`
- Passes `--enable-rwx=true` to the local-storage DaemonSet, activating the
  RWX reconciler
- Adds the minimum extra RBAC (PVC create/delete, Service create/delete)

Existing RWO workloads are not affected. If `rwx.enabled=false`, nothing
related to RWX is deployed — the reconciler's registration short-circuits
at program start.

## Example

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 5Gi
  storageClassName: hwameistor-storage-lvm-rwx
```

Two pods in the same namespace can mount `shared-data` simultaneously, on
different nodes.

## Availability and failure modes

- **No HA in v1.** The backing LocalVolume is non-replicated. If the node
  hosting it dies, the export is unavailable until the node recovers. This
  is the same failure profile as today's RWO.
- **Single Ganesha per node.** All exports on a node share one Ganesha
  process. If Ganesha crashes, Kubelet restarts the container and the
  reactor re-adds the exports — clients see a brief (~30s) stall under an
  NFSv4 hard mount.
- **No VolumeAttachment races.** The backing RWO PVC goes through the
  normal CSI path. Kubernetes guarantees single-primary attachment for that
  RWO volume via `VolumeAttachment`, so the multi-writer semantics live
  entirely at the NFS protocol layer — not at the block layer.

A v2 follow-up is planned to add failover by reacting to DRBD promotion
events and moving exports to the new primary node.

## Security considerations

- The DaemonSet pods are **privileged**. They bind-mount `/dev` from the
  host and run `mount(2)` inside the container. Clusters enforcing Pod
  Security Admission `restricted` on the HwameiStor namespace must switch
  that namespace to `privileged`.
- NFS exports use `SecType = sys` (no authentication). Any pod that can
  reach the Service ClusterIP can mount the export. **NetworkPolicy is the
  right tool if you need restriction.**
- The default squash is `root_squash`. Override per-StorageClass with the
  `lvm.hwameistor.io/nfs-squash` parameter.

## Snapshots

RWX PVCs can be snapshotted by taking a `VolumeSnapshot` of the **backing
RWO PVC** (the PVC named `<your-pvc>-rwx-backing`) using the normal
HwameiStor snapshot workflow (see
[Volume Snapshot](volume_snapshot.md)). The RWX layer is NFS; HwameiStor's
thin-pool snapshot path runs on the underlying LV, so snapshots work the
same way they do for RWO thin volumes.

## Limitations

- No HA / failover in v1.
- No per-export access control beyond NetworkPolicy.
- Requires `csi-driver-nfs` pre-installed.
- Only NFSv4 is enabled (NFSv3 is disabled in the Ganesha config).
- The RWX StorageClass must set the
  `lvm.hwameistor.io/backing-storage-class` parameter. Without it, PVCs
  stay Pending with an error event.
