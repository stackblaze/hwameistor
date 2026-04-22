package nfsreactor

import (
	"fmt"
	"os/exec"
	"strings"
)

// DRBDClient answers "is this node the Primary for a DRBD resource?"
// The reactor uses it to decide whether to serve an HA (convertible) LV
// from this node. Mocked in tests.
type DRBDClient interface {
	IsPrimary(resource string) (bool, error)
}

// NewDRBDClient returns a DRBDClient that shells out to `drbdsetup role
// <resource>`. If drbdsetup isn't on PATH (non-HA deployments), the
// client still constructs successfully — IsPrimary fails at call time
// with a loud error, which is only reached if an HA LV actually exists.
func NewDRBDClient() DRBDClient {
	return &realDRBD{}
}

type realDRBD struct{}

// IsPrimary returns true iff `drbdsetup role <resource>` reports "Primary".
// Exit code 10 from drbdsetup (and 20 from drbdadm) means "no such
// resource known to the kernel" — treat as "not Primary here" rather
// than an error, so the reactor on a node that never touched this LV
// just reports not-eligible without stack traces.
func (r *realDRBD) IsPrimary(resource string) (bool, error) {
	out, err := exec.Command("drbdsetup", "role", resource).CombinedOutput()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			switch ee.ExitCode() {
			case 10, 20:
				return false, nil
			}
		}
		return false, fmt.Errorf("drbdsetup role %s: %w: %s", resource, err, string(out))
	}
	return strings.TrimSpace(string(out)) == "Primary", nil
}
