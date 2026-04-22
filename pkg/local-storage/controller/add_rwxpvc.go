package controller

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/hwameistor/hwameistor/pkg/local-storage/member/controller/rwxpvc"
)

// Enabled gates the RWX-PVC reconciler. Off by default so existing
// deployments are unaffected; main.go flips it per --enable-rwx.
var Enabled bool

func init() {
	AddToManagerFuncs = append(AddToManagerFuncs, addRWXPVCController)
}

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
