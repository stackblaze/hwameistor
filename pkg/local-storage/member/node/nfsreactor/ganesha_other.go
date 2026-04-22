//go:build !linux

package nfsreactor

import "errors"

// sighupGaneshaReload on non-Linux platforms is a stub so tests compile
// everywhere. Production always runs on Linux.
func sighupGaneshaReload(string) error {
	return errors.New("nfsreactor: SIGHUP is only supported on Linux")
}
