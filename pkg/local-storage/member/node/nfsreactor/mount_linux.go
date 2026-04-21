//go:build linux

package nfsreactor

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// realMount is the production MountClient using syscall.Mount / Unmount.
type realMount struct{}

// NewRealMountClient returns a MountClient backed by the real kernel.
func NewRealMountClient() MountClient {
	return &realMount{}
}

// IsMounted returns true if target appears as a mount point in /proc/mounts.
func (m *realMount) IsMounted(target string) (bool, error) {
	target = filepath.Clean(target)
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false, fmt.Errorf("open /proc/mounts: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		if filepath.Clean(fields[1]) == target {
			return true, nil
		}
	}
	return false, sc.Err()
}

// Mount mkdir -p's target then mounts device there. Does NOT format — HwameiStor
// already formats the LV at provision time. If the target is already mounted
// this returns nil.
func (m *realMount) Mount(device, target, fsType string) error {
	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", target, err)
	}
	already, err := m.IsMounted(target)
	if err != nil {
		return err
	}
	if already {
		return nil
	}
	if fsType == "" {
		fsType = "ext4"
	}
	if err := syscall.Mount(device, target, fsType, 0, ""); err != nil {
		return fmt.Errorf("mount %s -> %s (%s): %w", device, target, fsType, err)
	}
	return nil
}

// Unmount unmounts target. Returns nil if the target is not currently mounted.
func (m *realMount) Unmount(target string) error {
	mounted, err := m.IsMounted(target)
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}
	if err := syscall.Unmount(target, 0); err != nil {
		return fmt.Errorf("unmount %s: %w", target, err)
	}
	return nil
}
