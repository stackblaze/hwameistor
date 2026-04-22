---
sidebar_label: "RWX v2 Design (HA)"
---

# RWX v2 — DRBD-replicated ReadWriteMany

Status: **DRAFT — code pending, live-verified on central 2026-04-21.**

## Context from v1

v1 layers NFS-Ganesha on top of an RWO HwameiStor LocalVolume. For a
non-HA LV there's exactly one replica, so "which node serves the NFS
export" is trivial: it's the node that has the replica. The reactor's
eligibility predicate is:

```go
// v1 isLocallyReady
return lv.Spec.Config.ExistReplicaOnNode(r.opts.NodeName) &&
       lv.Status.State == apisv1alpha1.VolumeStateReady
```

## The v1 bug that v2 fixes

When the backing storage class is HA (`replicaNumber: 2, convertible: true`),
HwameiStor creates **two DRBD-replicated replicas**. Both replica nodes
satisfy the v1 predicate. Both reactors try to mount the device. DRBD
auto-promotes whichever opens it first to Primary; the other stays
Secondary and refuses writes. The losing reactor loops forever with:

```
mkfs xfs /dev/LocalStorage_PoolNVMe-HA/pvc-...: exit status 1:
mkfs.xfs: cannot open ...: Read-only file system
```

Meanwhile the Primary's reactor is serving the export correctly. Data
plane works, logs are noisy, and if DRBD fails over nothing re-mounts on
the new Primary because both reactors are stuck in their respective
steady states.

## Verified facts (from central, 2026-04-21)

Test: 2-replica convertible LV, no consumer pod. 3 r/w nodes all run the
v1 reactor DaemonSet.

| Thing I checked | Result | What it means |
|---|---|---|
| `spec.config.replicas[i].primary` | Static, set once at volume creation, **not** the live DRBD role | Can't use this field as a promotion signal. |
| `status.publishedNodeName` | Empty on the HA LV — only set when the CSI driver publishes to a **pod**. RWX's reactor is a DaemonSet sidecar, not a pod consumer. | Can't use this field either for pure-RWX HA. |
| `drbdadm status <resource>` on each node | Exactly one node shows `role:Primary ... open:yes`; others show `role:Secondary` | Ground truth of live promotion lives in DRBD itself, not in the HwameiStor API surface. |
| `drbdsetup role <resource>` | Returns `Primary` / `Secondary` / `Unknown` | Simple programmatic probe, fast, already available on nodes that have DRBD. |
| Failover | Not yet tested end-to-end — needs a consuming pod to move, DRBD auto-promotes on new mount | Will verify after code lands. |

## v2 eligibility

The reactor has two modes now, selected at runtime per LV:

```go
// v2 isLocallyReady
if lv.Status.State != apisv1alpha1.VolumeStateReady {
    return false
}
if lv.Spec.Convertible {
    // HA: only the DRBD Primary serves. Ask DRBD directly — the
    // HwameiStor API does not expose live Primary/Secondary role for
    // a convertible LV without a consuming Pod.
    return r.drbd.IsPrimary(lv.Name)
}
// Non-HA (v1 behaviour): any node with a Ready replica.
return lv.Spec.Config != nil && lv.Spec.Config.ExistReplicaOnNode(r.opts.NodeName)
```

`r.drbd.IsPrimary(resource)` is a new tiny interface, injected for tests:

```go
type DRBDClient interface {
    IsPrimary(resource string) (bool, error)
}
```

Real implementation shells out to `drbdsetup role <resource>` and checks
for `Primary`. Exit code 10 (no such resource) → not a Primary,
everything else → error up. Idempotent, fast, no state.

## Failover model

1. Pod using the RWX PVC gets drained / its node dies.
2. `hwameistor-failover-assistant` (or the user label `hwameistor.io/failover=start`) reschedules any stateful consumer pods.
3. Something opens the DRBD device read-write on a surviving node.
4. DRBD auto-promotes that node to Primary; the old Primary (if still alive and still connected) goes Secondary.
5. Reactor on the new Primary: next reconcile pass sees `IsPrimary=true`, mounts the device, adds the Ganesha export, writes itself into the tenant Service's EndpointSlice.
6. Reactor on the old Primary: next reconcile pass sees `IsPrimary=false`, removes the export + EndpointSlice entry, unmounts.
7. NFSv4 clients re-resolve the Service → new endpoint → hard-mount retry succeeds after a few seconds.

Note: for v2 we rely on DRBD's **auto-promote** default. We never issue
`drbdadm primary --force`. The reactor is a consumer, not a promoter.

## Container requirement

The reactor container must have `drbdsetup` available on `PATH`. Two
options:
1. **Install drbd-utils in the reactor image** — adds ~1 MB, one apt
   line. Preferred.
2. **nsenter into host PID 1** — already privileged, already hostPID.
   Avoids image bloat but couples to host version.

Go with (1): `apt-get install -y drbd-utils` in `build/local-storage-nfs-reactor/Dockerfile`. drbd-utils is self-contained (no kernel module dependency).

## What v2 does NOT change

- **CSI driver**: still advertises `SINGLE_NODE_WRITER` only. Unchanged.
- **Non-HA RWX**: same code path as v1. Zero behavioural change for
  existing `hwameistor-storage-lvm-rwx` PVCs.
- **CRDs**: no schema changes. Only reads existing fields
  (`lv.Spec.Convertible`, `lv.Spec.Config.ExistReplicaOnNode`).
- **RBAC**: no new resources. The reactor already has `localvolumes` +
  `endpointslices` + `services`.
- **Helm chart structure**: one new sample StorageClass for HA RWX; the
  existing v1 chart pieces (DaemonSet, RBAC, feature flag) stay
  untouched.
- **rwx-pvc-controller**: no changes. It builds the backing chain the
  same way; `Convertible` is inherited from the backing SC.
- **Scheduler / DRBD / LocalVolume controllers**: untouched.

## Opt-in for users

One new sample StorageClass, shipped in Helm, off by default:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hwameistor-storage-lvm-rwx-ha
provisioner: lvm.hwameistor.io/rwx
parameters:
  lvm.hwameistor.io/backing-storage-class: hwameistor-storage-lvm-hdd-ha  # the HA class
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

Users who want HA RWX create PVCs against `hwameistor-storage-lvm-rwx-ha`.
Existing `hwameistor-storage-lvm-rwx` keeps working as non-HA.

## New Helm values

```yaml
rwx:
  enabled: true
  ha:
    # Creates a sample HA RWX StorageClass. Requires an HA backing SC
    # (convertible + replicaNumber >= 2) to exist already.
    storageClass:
      enabled: false  # off by default — user must explicitly opt in
      backingStorageClassName: hwameistor-storage-lvm-hdd-ha
```

## Files changed

**New:**
- `pkg/local-storage/member/node/nfsreactor/drbd.go` — the `DRBDClient` interface + `drbdsetup role` shell-out, ~60 lines
- `pkg/local-storage/member/node/nfsreactor/drbd_test.go` — fake DRBDClient + a couple unit tests, ~50 lines
- `helm/hwameistor/templates/storageclass-rwx-ha.yaml` — sample HA RWX SC, ~15 lines

**Modified:**
- `pkg/local-storage/member/node/nfsreactor/reactor.go` — eligibility switch on `Convertible`, wires `DRBDClient` into the Reactor struct, ~15 lines
- `pkg/local-storage/member/node/nfsreactor/reactor_test.go` — new test cases: HA eligible when Primary, HA NOT eligible when Secondary, non-HA behavior unchanged, ~40 lines
- `build/local-storage-nfs-reactor/Dockerfile` — one RUN line to install drbd-utils
- `helm/hwameistor/values.yaml` — add `rwx.ha.storageClass.*`

Net: ~200 lines added, 0 deleted, no existing behavior changes.

## Rollout

- v2 ships with `rwx.ha.storageClass.enabled=false`. Existing central
  deployment unaffected.
- Operators who want HA RWX opt in via helm upgrade. Requires DRBD
  kernel module + userspace on every storage node.
- Image tag bumps as part of v2 include the `drbd-utils` package so the
  reactor can call `drbdsetup role`.

## Open questions

1. **DRBD connection loss during failover.** If the old-Primary node is
   partitioned but not dead, it keeps thinking it's Primary for a few
   seconds. Reactor there will keep serving stale data until DRBD
   declares the peer unreachable and demotes. Is that acceptable for v2?
   (Cross-ref: DRBD's `fence-peer` handler. Out of scope for v2
   reactor; DRBD itself has to be configured right. HwameiStor's
   default DRBD config in `drbd.go` doesn't set a fence handler. Flag as
   v2.1 follow-up.)

2. **Multiple EndpointSlices briefly present during role swap.** If
   reactors on two nodes both think they're Primary for a beat, two
   EndpointSlices (`<svc>-<nodeA>`, `<svc>-<nodeB>`) exist. NFS clients
   see two endpoints, may pick the wrong one. The losing reactor's next
   reconcile removes its slice. Short window, clients retry. Acceptable?

3. **Reactor on a node with no DRBD.** Non-HA deployments shouldn't
   require the `drbd-utils` package to even load. The code path short-
   circuits on `lv.Spec.Convertible == false` before calling
   `DRBDClient.IsPrimary`, so `drbdsetup` never runs. But the binary
   still has to exist on `PATH` at startup. Solution: only call
   `DRBDClient.IsPrimary` behind the Convertible check (we do), and
   have the real client lazy-init `drbdsetup` discovery. If drbd-utils
   is missing and no HA LVs exist, the reactor runs fine. If an HA LV
   shows up and drbd-utils is missing, we emit an error on that LV's
   reconcile and leave the export unserved — loud and obvious, not
   silently broken.
