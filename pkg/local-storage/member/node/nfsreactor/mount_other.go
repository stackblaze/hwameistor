//go:build !linux

package nfsreactor

import "errors"

// NewRealMountClient on non-Linux returns a stub that always errors. The
// reactor only runs in a Linux container in production; this stub exists so
// the package compiles for cross-platform developer tooling (IDE, tests).
func NewRealMountClient() MountClient {
	return &stubMount{}
}

type stubMount struct{}

func (s *stubMount) IsMounted(string) (bool, error) {
	return false, errors.New("nfsreactor: mount operations are only supported on Linux")
}

func (s *stubMount) Mount(string, string, string) error {
	return errors.New("nfsreactor: mount operations are only supported on Linux")
}

func (s *stubMount) Unmount(string) error {
	return errors.New("nfsreactor: mount operations are only supported on Linux")
}
