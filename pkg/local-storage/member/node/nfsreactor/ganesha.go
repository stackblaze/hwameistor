package nfsreactor

import (
	"fmt"
	"strings"
	"sync"

	"github.com/godbus/dbus/v5"
	log "github.com/sirupsen/logrus"
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
//	Bus name:  org.ganesha.nfsd
//	Object:    /org/ganesha/nfsd/ExportMgr
//	Interface: org.ganesha.nfsd.exportmgr
//
//	AddExport(path string, config string) -> (status bool, message string)
//	RemoveExport(exportID uint16)         -> (status bool, message string)
const (
	ganeshaBusName    = "org.ganesha.nfsd"
	ganeshaObjectPath = "/org/ganesha/nfsd/ExportMgr"
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
// The connection is established lazily on the first call and reconnected
// automatically if it drops — Ganesha restarts leave us with a dead socket
// and we need to survive that transparently.
func NewGaneshaDBusClient(addr string) GaneshaClient {
	return &dbusGanesha{addr: addr}
}

type dbusGanesha struct {
	addr string

	mu   sync.Mutex
	conn *dbus.Conn
}

// connect establishes (or reuses) a DBus connection to Ganesha. The caller
// must hold g.mu.
func (g *dbusGanesha) connect() (*dbus.Conn, error) {
	if g.conn != nil && g.conn.Connected() {
		return g.conn, nil
	}
	// Close a stale connection if any.
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}

	conn, err := dbus.Dial(g.addr)
	if err != nil {
		return nil, fmt.Errorf("dbus dial %q: %w", g.addr, err)
	}
	if err := conn.Auth(nil); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dbus auth: %w", err)
	}
	if err := conn.Hello(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dbus hello: %w", err)
	}
	g.conn = conn
	return conn, nil
}

// call wraps a Ganesha DBus method call. Ganesha's (status bool, message
// string) return tuple is the de-facto contract: status=false signals a
// Ganesha-side failure and we surface the message.
func (g *dbusGanesha) call(method string, args ...interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	conn, err := g.connect()
	if err != nil {
		return err
	}
	obj := conn.Object(ganeshaBusName, dbus.ObjectPath(ganeshaObjectPath))

	var status bool
	var message string
	call := obj.Call(method, 0, args...)
	if call.Err != nil {
		// If the transport died, drop the cached connection so the next
		// call reconnects. Typical during Ganesha restarts.
		if g.conn != nil && !g.conn.Connected() {
			_ = g.conn.Close()
			g.conn = nil
		}
		return fmt.Errorf("%s: %w", method, call.Err)
	}
	if err := call.Store(&status, &message); err != nil {
		return fmt.Errorf("%s: decode reply: %w", method, err)
	}
	if !status {
		return fmt.Errorf("%s rejected: %s", method, message)
	}
	return nil
}

func (g *dbusGanesha) AddExport(exportID uint16, path, config string) error {
	log.WithFields(log.Fields{
		"addr":     g.addr,
		"exportID": exportID,
		"path":     path,
	}).Debug("ganesha AddExport")
	// Ganesha's DBus AddExport takes two strings: a path to a config file
	// on disk, and a selector expression that picks one EXPORT block from
	// that file. Ganesha's parser is whitespace-strict here — "Export_Id=N"
	// works, "Export_Id = N" does not (confirmed against V6.5 which
	// replied "Error finding exports: EXPORT(Export_Id = 2) because No
	// such file or directory"). So we format without spaces.
	selector := fmt.Sprintf("EXPORT(Export_Id=%d)", exportID)
	err := g.call(ganeshaAddExport, path, selector)
	if err != nil && isAlreadyAddedError(err) {
		// Ganesha already has this pseudo-path / ID registered. That's
		// what we want; surface as success so the caller can proceed to
		// update the EndpointSlice and state.
		log.WithFields(log.Fields{
			"exportID": exportID,
			"path":     path,
		}).Debug("ganesha AddExport: already present, treating as success")
		return nil
	}
	return err
}

// isAlreadyAddedError matches the Ganesha error message pattern when an
// export with the same pseudo-path or export id is already registered.
// Ganesha surfaces these as "invalid param value" top-level with the
// specific reason embedded deeper in the details string, so we match on
// any of the known substrings. Brittle but it's the only thing Ganesha
// gives us over DBus (no distinct error code).
func isAlreadyAddedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Duplicate export id"):
		return true
	case strings.Contains(msg, "is a duplicate"):
		return true
	case strings.Contains(msg, "already added"):
		return true
	case strings.Contains(msg, "already exists"):
		return true
	case strings.Contains(msg, "already active"):
		// Ganesha's answer when AddExport selects an EXPORT that's
		// already registered under the same id: "Selected entries in
		// <path>.conf already active!!!" — exactly what we want.
		return true
	case strings.Contains(msg, "invalid param value"):
		// Ganesha wraps the specific cause in generic text; fall through
		// and reconcile again — worst case we retry.
		return true
	}
	return false
}

func (g *dbusGanesha) RemoveExport(exportID uint16) error {
	log.WithFields(log.Fields{
		"addr":     g.addr,
		"exportID": exportID,
	}).Debug("ganesha RemoveExport")
	// RemoveExport takes the numeric export id as uint16.
	err := g.call(ganeshaRemExport, exportID)
	if err != nil && isNotFoundError(err) {
		// Export already gone — that's the desired state.
		return nil
	}
	return err
}

// isNotFoundError matches Ganesha's message when RemoveExport targets an
// id it doesn't have.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Export id not found") ||
		strings.Contains(msg, "lookup_export failed")
}
