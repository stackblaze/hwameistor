package nfsreactor

import (
	"sync"
)

// Served records what this node currently has mounted and exported for a
// given user PVC UID (the value of the hwameistor.io/rwx-export annotation).
type Served struct {
	UID         string
	LVName      string
	ExportID    uint16
	Device      string
	MountPath   string
	ServiceName string
	ServiceNS   string
}

// State is the reactor's in-memory view of what it's currently serving.
// It is the single source of truth for idempotency inside the process; the
// filesystem and Ganesha are treated as side-effects that Reconcile brings
// back into line with State.
type State struct {
	mu     sync.Mutex
	served map[string]*Served // key = user-PVC-UID (annotation value)
}

// NewState creates an empty state.
func NewState() *State {
	return &State{served: map[string]*Served{}}
}

// Get returns the current served entry for a UID or nil.
func (s *State) Get(uid string) *Served {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.served[uid]; ok {
		cp := *v
		return &cp
	}
	return nil
}

// GetByLV returns the served entry for a given LocalVolume name, or nil.
// Used only on the tear-down path when the LV's annotation was removed or
// the LV object itself was deleted, so a linear scan is fine: a node
// rarely serves more than a handful of exports.
func (s *State) GetByLV(lvName string) *Served {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.served {
		if v.LVName == lvName {
			cp := *v
			return &cp
		}
	}
	return nil
}

// Put stores or overwrites a served entry.
func (s *State) Put(srv *Served) {
	if srv == nil || srv.UID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *srv
	s.served[cp.UID] = &cp
}

// Delete removes a UID from the state.
func (s *State) Delete(uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.served, uid)
}

// AllocateID returns a non-zero uint16 export ID deterministically derived
// from the UID. Stable across reactor restarts so Ganesha keeps the same
// id for the same pseudo-path — which avoids a Ganesha v6.5 segfault in
// the RemoveExport-then-AddExport path (see
// https://github.com/nfs-ganesha/nfs-ganesha/issues/166).
//
// fnv-1a over the UID, clamped to [1, 65535]. Collisions are unlikely but
// not impossible; on collision Ganesha rejects the second AddExport with
// "Duplicate export id", which AddExport's error handler treats as
// already-added. If this ever becomes a real problem, switch to a
// persisted UID→ID mapping.
func (s *State) AllocateID(uid string) uint16 {
	const (
		fnvOffset uint64 = 14695981039346656037
		fnvPrime  uint64 = 1099511628211
	)
	h := fnvOffset
	for i := 0; i < len(uid); i++ {
		h ^= uint64(uid[i])
		h *= fnvPrime
	}
	// 0..65534 → 1..65535 (never 0, which Ganesha reserves).
	return uint16(h%65535) + 1
}
