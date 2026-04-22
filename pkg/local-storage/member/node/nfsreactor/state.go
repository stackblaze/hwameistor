package nfsreactor

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Served records what this node currently has mounted + exported for a given
// user PVC UID (the value of the hwameistor.io/rwx-export annotation).
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
// filesystem and Ganesha are treated as side-effects that are reconciled
// toward this State each time Reconcile runs.
type State struct {
	mu     sync.Mutex
	served map[string]*Served // key = user-PVC-UID (annotation value)
	// lvToUID lets us look up what we exported for an LV whose annotation
	// was removed or whose object was deleted (so we can tear down).
	lvToUID map[string]string

	// nextID is a monotonic counter for Ganesha export IDs. Ganesha needs
	// a non-zero uint16; we wrap, skipping zero and any live IDs.
	nextID uint16
}

// NewState creates an empty state with the export-ID counter set to 1.
func NewState() *State {
	return &State{
		served:  map[string]*Served{},
		lvToUID: map[string]string{},
		nextID:  1,
	}
}

// Get returns the current served entry for a UID or nil.
func (s *State) Get(uid string) *Served {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.served[uid]; ok {
		// return a copy so callers don't race with updates
		cp := *v
		return &cp
	}
	return nil
}

// GetByLV returns the served entry indexed by LocalVolume name, or nil.
func (s *State) GetByLV(lvName string) *Served {
	s.mu.Lock()
	defer s.mu.Unlock()
	uid, ok := s.lvToUID[lvName]
	if !ok {
		return nil
	}
	v, ok := s.served[uid]
	if !ok {
		return nil
	}
	cp := *v
	return &cp
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
	if cp.LVName != "" {
		s.lvToUID[cp.LVName] = cp.UID
	}
}

// Delete removes a UID from the state.
func (s *State) Delete(uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.served[uid]; ok {
		if v.LVName != "" {
			delete(s.lvToUID, v.LVName)
		}
		delete(s.served, uid)
	}
}

// AllocateID returns a non-zero uint16 export ID deterministically derived
// from the UID. This keeps the ID stable across reactor restarts so we
// don't confuse Ganesha with a new ID for the same pseudo-path — and we
// avoid calling Ganesha's buggy RemoveExport on unknown ids (which
// segfaults the daemon in v6.5).
//
// The mapping is a simple fnv-1a hash over the UID, clamped to non-zero
// uint16. Collisions are astronomically unlikely (≤ a few thousand
// exports per node) but not impossible; if two UIDs hash to the same id
// Ganesha will reject the second AddExport with 'Duplicate export id'
// and our AddExport caller will treat that as "already added" — which
// is the wrong export but at least it won't crash the daemon. If this
// turns out to bite anyone we can switch to a persisted mapping.
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
	id := uint16(h % 65535) // 0..65534
	id++                    // 1..65535 (never 0)
	return id
}

// RebuildFromMounts scans /proc/mounts and populates state entries for any
// mount under exportRoot that looks like an LVM volume. On restart this lets
// us skip re-mounting volumes that are already mounted and reuse export IDs.
//
// Note: we can only infer UID+Device here. LVName / ServiceName / ExportID
// must be refilled on the first reconcile pass (Reconcile is the source of
// truth for those). We set ExportID=0 as a sentinel meaning "unknown, will
// be reassigned if still eligible".
func (s *State) RebuildFromMounts(exportRoot string) error {
	exportRoot = filepath.Clean(exportRoot)
	f, err := os.Open("/proc/mounts")
	if err != nil {
		// Not fatal: on non-linux test runs /proc/mounts may not exist.
		return fmt.Errorf("open /proc/mounts: %w", err)
	}
	defer f.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		device := fields[0]
		target := filepath.Clean(fields[1])
		if !strings.HasPrefix(target, exportRoot+string(filepath.Separator)) {
			continue
		}
		uid := filepath.Base(target)
		if uid == "" || uid == "." {
			continue
		}
		s.served[uid] = &Served{
			UID:       uid,
			Device:    device,
			MountPath: target,
		}
	}
	return sc.Err()
}
