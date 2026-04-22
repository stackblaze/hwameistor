//go:build linux

package nfsreactor

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type realMount struct{}

func NewRealMountClient() MountClient { return &realMount{} }

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

// Mount mkdir -p's target, formats the device if it has no fs (HwameiStor
// only formats on NodeStageVolume, which never runs for RWX backing
// volumes since no pod mounts them directly), then mounts.
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
		fsType = "xfs"
	}

	existing, err := probeFSType(device)
	if err != nil {
		return fmt.Errorf("probe %s: %w", device, err)
	}
	if existing == "" {
		if err := mkfs(device, fsType); err != nil {
			return err
		}
	} else if existing != fsType {
		return fmt.Errorf("device %s has fsType %q but reactor expects %q; refusing to mount", device, existing, fsType)
	}

	if err := syscall.Mount(device, target, fsType, 0, ""); err != nil {
		return fmt.Errorf("mount %s -> %s (%s): %w", device, target, fsType, err)
	}
	return nil
}

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

// probeFSType returns empty string when blkid finds no signature (exit 2);
// any other failure is surfaced.
func probeFSType(device string) (string, error) {
	out, err := exec.Command("blkid", "-o", "value", "-s", "TYPE", device).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 2 {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func mkfs(device, fsType string) error {
	var cmd *exec.Cmd
	switch fsType {
	case "xfs":
		cmd = exec.Command("mkfs.xfs", "-f", device)
	case "ext4":
		cmd = exec.Command("mkfs.ext4", "-F", device)
	default:
		return fmt.Errorf("unsupported fsType %q; expected xfs or ext4", fsType)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs %s %s: %w: %s", fsType, device, err, string(out))
	}
	return nil
}
