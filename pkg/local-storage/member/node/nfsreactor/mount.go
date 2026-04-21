package nfsreactor

// MountClient abstracts mount operations so tests can inject a fake.
// The real implementation calls into syscall.Mount/Unmount and lives in
// mount_linux.go — this file just defines the interface so callers and
// tests compile everywhere.
type MountClient interface {
	IsMounted(target string) (bool, error)
	Mount(device, target, fsType string) error
	Unmount(target string) error
}
