package nfsreactor

// MountClient abstracts mount operations so tests can inject a fake.
// The real Linux implementation lives in mount_linux.go.
type MountClient interface {
	IsMounted(target string) (bool, error)
	Mount(device, target, fsType string) error
	Unmount(target string) error
}
