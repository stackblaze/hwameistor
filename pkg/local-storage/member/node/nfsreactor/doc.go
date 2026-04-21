// Package nfsreactor implements a per-node reconciler that bridges HwameiStor
// LocalVolume CRs and a co-located NFS-Ganesha daemon.
//
// For each LocalVolume annotated with "hwameistor.io/rwx-export=<user-pvc-uid>"
// that has a Ready replica on this node, the reactor:
//
//  1. Mounts /dev/<pool>/<lv-name> at <ExportRoot>/<uid>.
//  2. Calls Ganesha AddExport over the system DBus to publish the export.
//  3. Writes an EndpointSlice named "<service>-<node>" that selects the
//     backing tenant Service and points at this pod's IP on NFS port 2049.
//
// On annotation removal, replica relocation, or LocalVolume deletion the
// reactor reverses those operations (RemoveExport, unmount, rmdir, drop
// EndpointSlice). Reconciliation is idempotent; on restart state is
// reconstructed from /proc/mounts under ExportRoot.
package nfsreactor
