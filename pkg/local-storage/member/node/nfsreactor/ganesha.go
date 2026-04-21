package nfsreactor

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	// TODO: add github.com/godbus/dbus/v5 to go.mod when wiring the build.
	// Once added, replace the stub implementation below with real calls:
	//
	//   import "github.com/godbus/dbus/v5"
	//
	//   conn, err := dbus.Dial(addr)
	//   _ = conn.Auth(nil); _ = conn.Hello()
	//   obj := conn.Object("org.ganesha.nfsd", "/org/ganesha/nfsd/ExportMgr")
	//   var status bool; var message string
	//   err := obj.Call("org.ganesha.nfsd.exportmgr.AddExport", 0, path, config).
	//           Store(&status, &message)
)

// DefaultExportConfigTemplate is the Ganesha EXPORT{} block written per-volume.
// First %d: export ID. First %s: absolute path. Second %s: short pseudo path
// (we use the UID).
const DefaultExportConfigTemplate = `EXPORT {
    Export_Id = %d;
    Path = "%s";
    Pseudo = "/%s";
    Access_Type = RW;
    Squash = Root_Squash;
    SecType = sys;
    FSAL { Name = VFS; }
}`

// RenderExportConfig builds the Ganesha EXPORT{} block for a volume.
func RenderExportConfig(exportID uint16, path, pseudo string) string {
	return fmt.Sprintf(DefaultExportConfigTemplate, exportID, path, pseudo)
}

// Ganesha DBus contract:
//
//   Bus name:  org.ganesha.nfsd
//   Object:    /org/ganesha/nfsd/ExportMgr
//   Interface: org.ganesha.nfsd.exportmgr
//
//   AddExport(path string, config string) -> (status bool, message string)
//   RemoveExport(exportID uint16)         -> (status bool, message string)
const (
	ganeshaBusName    = "org.ganesha.nfsd"
	ganeshaObjectPath = "/org/ganesha/nfsd/ExportMgr"
	ganeshaInterface  = "org.ganesha.nfsd.exportmgr"
	ganeshaAddExport  = "org.ganesha.nfsd.exportmgr.AddExport"
	ganeshaRemExport  = "org.ganesha.nfsd.exportmgr.RemoveExport"
)

// GaneshaClient abstracts the Ganesha management DBus so tests can fake it.
type GaneshaClient interface {
	AddExport(exportID uint16, path, config string) error
	RemoveExport(exportID uint16) error
}

// NewGaneshaDBusClient returns a GaneshaClient that talks to Ganesha over the
// system DBus socket at addr (e.g. "unix:/run/dbus/system_bus_socket").
//
// NOTE: the concrete implementation below is a stub until godbus/dbus/v5 is
// added to go.mod. It logs each call and returns nil so the reactor can be
// exercised end-to-end in tests and dry runs. Callers should replace this
// with the real implementation once the dependency is vendored.
func NewGaneshaDBusClient(addr string) GaneshaClient {
	return &stubGanesha{addr: addr}
}

type stubGanesha struct {
	addr string
}

func (g *stubGanesha) AddExport(exportID uint16, path, config string) error {
	log.WithFields(log.Fields{
		"addr":     g.addr,
		"exportID": exportID,
		"path":     path,
		"bus":      ganeshaBusName,
		"object":   ganeshaObjectPath,
		"iface":    ganeshaInterface,
		"method":   ganeshaAddExport,
	}).Warn("ganesha DBus stub: AddExport called (add godbus/dbus/v5 to go.mod to enable)")
	_ = config
	return nil
}

func (g *stubGanesha) RemoveExport(exportID uint16) error {
	log.WithFields(log.Fields{
		"addr":     g.addr,
		"exportID": exportID,
		"method":   ganeshaRemExport,
	}).Warn("ganesha DBus stub: RemoveExport called (add godbus/dbus/v5 to go.mod to enable)")
	return nil
}
