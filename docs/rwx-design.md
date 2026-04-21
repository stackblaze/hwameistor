---
sidebar_label: "RWX Design"
---

# RWX (ReadWriteMany) Support — Design

Status: **DRAFT — v1 implementation in progress.**

This document describes layering NFS (via NFS-Ganesha) on top of HwameiStor
RWO LocalVolumes to provide `ReadWriteMany` semantics. The shape is inspired
by Piraeus Operator's `linstor-csi-nfs-server` DaemonSet pattern
([piraeus-operator/pkg/resources/cluster/nfs-server/nfs-server-daemon-set.yaml](../../../piraeus-operator/pkg/resources/cluster/nfs-server/nfs-server-daemon-set.yaml))
but simplified for HwameiStor's node-local LVM model.

## Non-goals for v1

- **HA / DRBD failover.** v1 serves each RWX PVC from exactly one node (the
  node hosting the backing LocalVolume). If the node dies, the export is
  unavailable until the node recovers — same availability profile as RWO
  today. HA mode is a v2 follow-up that reacts to DRBD promotion events.
- **New CRDs.** Everything uses existing HwameiStor resources
  (`LocalVolume`, `PersistentVolumeClaim`) plus standard Kubernetes
  primitives (`Service`, `PersistentVolume`, `EndpointSlice`).
- **Modifying the CSI driver.** HwameiStor's CSI driver keeps advertising
  `SINGLE_NODE_WRITER` only. The user's RWX PVC binds to an NFS PV
  (`nfs.csi.k8s.io`), not to HwameiStor's CSI driver.
- **Bundling `csi-driver-nfs`.** It's a prerequisite, not a dependency —
  same call TopoLVM and Piraeus made.

## How it works

```
┌─────────────────────────────────────────────────────────────────┐
│ user creates PVC with                                           │
│   accessModes: [ReadWriteMany]                                  │
│   storageClassName: hwameistor-storage-lvm-rwx                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ rwx-pvc-controller (new, runs in local-storage controller mgr)  │
│   - filters to PVCs whose SC has                                │
│     provisioner: lvm.hwameistor.io/rwx                          │
│   - adds finalizer                                              │
│   - creates:                                                    │
│       backing PVC  (normal HwameiStor SC, RWO)                  │
│       ClusterIP Service (no selector)                           │
│       NFS PV bound to the user's PVC                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ existing HwameiStor CSI + scheduler                             │
│   provisions the backing PVC → LocalVolume on some node N       │
│   (controller annotates the LocalVolume with                    │
│    hwameistor.io/rwx-export=<user-pvc-uid>)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ nfs-reactor sidecar  (runs in DaemonSet pod on every storage    │
│                       node, privileged)                         │
│   watches LocalVolumes on this node                             │
│   for each with the rwx-export annotation whose replica is      │
│   local:                                                        │
│     - mount /dev/<vg>/<lv> → /srv/exports/<uid>                 │
│     - DBus AddExport to ganesha container (live reload)         │
│     - write EndpointSlice pointing this node's pod IP into      │
│       the user PVC's Service                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ consumer pods on any node                                       │
│   mount the NFS PV via nfs.csi.k8s.io                           │
│   traffic routes through Service ClusterIP → EndpointSlice      │
│   → hosting node's ganesha pod                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. `rwx-pvc-controller`

New package `pkg/local-storage/member/controller/rwxpvc/`. One reconciler.

**Trigger:** `PersistentVolumeClaim` changes. Filter:
- `pvc.Spec.StorageClassName` resolves to a StorageClass with
  `provisioner = "lvm.hwameistor.io/rwx"`
- `pvc.Spec.AccessModes` contains `ReadWriteMany`

**On reconcile (create/update):**
1. Add finalizer `hwameistor.io/rwx-pvc`
2. Ensure backing PVC exists (RWO, storage class from SC param
   `lvm.hwameistor.io/backing-storage-class`)
3. Ensure ClusterIP Service exists — **no selector**; the reactor writes
   EndpointSlices for it
4. Ensure mirror NFS PV exists; `spec.csi.driver = nfs.csi.k8s.io`,
   `volumeHandle = <svc-ip>#/srv/exports/<uid>#`, bound to the user PVC
5. Once the backing PVC is bound and its LocalVolume is known, patch the
   LocalVolume with annotation
   `hwameistor.io/rwx-export = <user-pvc-uid>` — this is the signal the
   reactor watches

**On delete:**
1. Delete mirror PV, Service, backing PVC (in order)
2. Remove finalizer

Wired into the local-storage controller manager via
`pkg/local-storage/controller/add_rwxpvc.go`, gated on a CLI flag
`--enable-rwx` (default `false`).

### 2. `nfs-reactor` binary

New binary `cmd/local-storage-nfs-reactor/main.go`, new package
`pkg/local-storage/member/node/nfsreactor/`.

Runs as a container in a new DaemonSet. Per node:

1. `NODE_NAME` from downward API
2. Watch `LocalVolume` objects (existing CRD). Filter to those with:
   - annotation `hwameistor.io/rwx-export != ""`
   - a replica `spec.replicas[].node == $NODE_NAME` in the `Ready` state
3. For each eligible LV:
   - Resolve device path (existing helper returns
     `/dev/<vg>/<lvname>` from the LocalVolume's VG + internal name)
   - Ensure `/srv/exports/<uid>` exists and the device is mounted (ext4/xfs
     per LV's fsType)
   - Call Ganesha DBus `AddExport` on socket at `/run/dbus/system_bus_socket`
     pointing at `/srv/exports/<uid>`
   - Write an `EndpointSlice` named `<svc-name>-<node>` that selects the
     user PVC's Service, with one endpoint: this pod's IP
4. For previously-eligible LVs that are no longer on this node:
   - Ganesha DBus `RemoveExport`
   - Unmount `/srv/exports/<uid>`
   - Delete the EndpointSlice entry
5. Self-heal: the reconcile loop is idempotent, so a restart reconstructs
   all state from LocalVolumes + mount table

### 3. Ganesha container

Upstream image `ghcr.io/kubernetes-sigs/nfs-ganesha:V6.5` (same as TopoLVM).
Minimal config:

```
NFS_Core_Param { NFS_Protocols = 4; }
NFSV4 { Grace_Period = 30; }
# No static exports — reactor adds them via DBus.
```

DBus socket is shared between containers via `emptyDir` at `/run/dbus`.
Ganesha runs PID 1 in its container; no systemd.

### 4. Helm chart additions

All additive, feature-flagged on `rwx.enabled` (default `false`):

- `helm/hwameistor/templates/local-storage-nfs-server.yaml` — DaemonSet
  with two containers (`nfs-reactor`, `ganesha`), privileged, hostPath
  `/dev` rw, `/sys` ro, `/run` rw, same nodeAffinity as existing
  `hwameistor-local-storage` DaemonSet
- `helm/hwameistor/templates/local-storage-nfs-rbac.yaml` — ServiceAccount,
  ClusterRole (PVCs, PVs, Services, EndpointSlices, LocalVolumes), binding
- `helm/hwameistor/templates/storageclass-rwx.yaml` — sample RWX
  StorageClass (only rendered if `rwx.enabled=true`)
- `helm/hwameistor/values.yaml` — new `rwx:` section (image repos, enabled
  flag, backing storage class name)

### 5. Wire-up (one-line changes)

- `cmd/local-storage/storage.go` — new flag `--enable-rwx`, pass through to
  the controller registration
- `pkg/local-storage/controller/add_rwxpvc.go` — register the new
  reconciler in `AddToManagerFuncs` (gated on the flag via a package-level
  `Enabled` bool set by main)

## Files changed

**New:**
```
pkg/local-storage/member/controller/rwxpvc/reconciler.go
pkg/local-storage/member/controller/rwxpvc/objects.go        # helpers that build Service, PV, backing PVC
pkg/local-storage/member/controller/rwxpvc/reconciler_test.go
pkg/local-storage/member/node/nfsreactor/reactor.go
pkg/local-storage/member/node/nfsreactor/mount.go            # mount/unmount helpers
pkg/local-storage/member/node/nfsreactor/ganesha.go          # DBus AddExport/RemoveExport
pkg/local-storage/member/node/nfsreactor/endpointslice.go    # EndpointSlice mgmt
pkg/local-storage/member/node/nfsreactor/reactor_test.go
pkg/local-storage/controller/add_rwxpvc.go
cmd/local-storage-nfs-reactor/main.go
helm/hwameistor/templates/local-storage-nfs-server.yaml
helm/hwameistor/templates/local-storage-nfs-rbac.yaml
helm/hwameistor/templates/storageclass-rwx.yaml
docs/docs/volumes/rwx.md
```

**Modified (minimally):**
```
cmd/local-storage/storage.go     # +1 flag, +1 line to propagate to controller pkg
helm/hwameistor/values.yaml      # +rwx: section
```

## Security impact

- DaemonSet pods are **privileged: true** with `hostPath:/dev` rw. Needed
  because Ganesha mounts per-PVC LVs inside its own mount namespace.
- Clusters enforcing `PodSecurity: restricted` in HwameiStor's namespace
  must switch to `privileged` for this DaemonSet. Document in user-facing
  docs.
- NFS exports use `SecType = sys` (no auth). Any pod that can reach the
  Service ClusterIP can mount. NetworkPolicy is the correct control.
- Default squash: `root_squash`. Override via StorageClass param
  `lvm.hwameistor.io/nfs-squash` if needed.

## Constraints / known limitations for v1

- **No HA.** Node loss = export unavailable until node returns.
- **Single Ganesha process per node.** All exports on that node share it.
  A crash takes down all exports on that node for ~30s.
- **No per-export access control.** NetworkPolicy only.
- **Requires `csi-driver-nfs`** installed in the cluster (provides
  `nfs.csi.k8s.io`).
- **Snapshots.** RWX PVC snapshots work by snapshotting the backing RWO
  PVC — same semantics as TopoLVM. v1 controller does **not** automate
  this; users snapshot the backing PVC directly.

## Test plan

### Unit tests

- `rwxpvc/reconciler_test.go` — reconciler idempotency: create once, create
  again, verify no duplicate objects; delete cleanup in correct order
- `nfsreactor/reactor_test.go` — eligibility filter, add/remove lifecycle
  with fake mount + DBus + EndpointSlice client

### e2e (deferred to follow-up PR)

- basic: RWX PVC, two pods on different nodes, shared write
- delete: PVC deletion tears down Service, PV, backing PVC, export, slice
- restart: kill the reactor pod; verify exports reconstruct on restart

## Rollout

- v1 ships with `rwx.enabled: false` by default
- Operators who opt in must install `csi-driver-nfs` themselves
- Feature-flagged, so enabling on an existing cluster is a pure Helm upgrade
  with zero impact on existing RWO workloads
