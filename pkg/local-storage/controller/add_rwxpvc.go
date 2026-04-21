package controller

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/hwameistor/hwameistor/pkg/local-storage/member/controller/rwxpvc"
)

// Enabled gates registration of the RWX-PVC reconciler. It is off by
// default so existing deployments are unaffected; an operator or wiring
// layer sets it to true to activate the NFS-backed RWX flow.
var Enabled bool

func init() {
	AddToManagerFuncs = append(AddToManagerFuncs, addRWXPVCController)
}

// addRWXPVCController registers the rwxpvc reconciler with the manager
// only when Enabled is true. Guarding inside the function is simpler than
// a conditional init and lets callers flip the flag at program startup
// before manager.Start is called.
func addRWXPVCController(mgr manager.Manager) error {
	if !Enabled {
		return nil
	}
	r := &rwxpvc.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
	return r.SetupWithManager(mgr)
}
