//go:build !linux

package nfsreactor

import "errors"

// NewRealMountClient on non-Linux is a stub so the package compiles for
// dev tooling and tests. Production is always Linux.
func NewRealMountClient() MountClient { return &stubMount{} }

type stubMount struct{}

var errNotLinux = errors.New("nfsreactor: mount operations are only supported on Linux")

func (s *stubMount) IsMounted(string) (bool, error)      { return false, errNotLinux }
func (s *stubMount) Mount(string, string, string) error  { return errNotLinux }
func (s *stubMount) Unmount(string) error                { return errNotLinux }
