package nfsreactor

import (
	"fmt"
	"strings"
	"sync"

	"github.com/godbus/dbus/v5"
	log "github.com/sirupsen/logrus"
)

// DefaultExportConfigTemplate is written per RWX volume. Squash defaults to
// No_Root_Squash so uid-0 pods (alpine, debian, most workloads) can write.
const DefaultExportConfigTemplate = `EXPORT {
    Export_Id = %d;
    Path = "%s";
    Pseudo = "/%s";
    Access_Type = RW;
    Squash = No_Root_Squash;
    SecType = sys;
    FSAL { Name = VFS; }
}`

func RenderExportConfig(exportID uint16, path, pseudo string) string {
	return fmt.Sprintf(DefaultExportConfigTemplate, exportID, path, pseudo)
}

const (
	ganeshaBusName    = "org.ganesha.nfsd"
	ganeshaObjectPath = "/org/ganesha/nfsd/ExportMgr"
	ganeshaAddExport  = "org.ganesha.nfsd.exportmgr.AddExport"
	ganeshaRemExport  = "org.ganesha.nfsd.exportmgr.RemoveExport"
)

type GaneshaClient interface {
	AddExport(exportID uint16, path, config string) error
	RemoveExport(exportID uint16) error
}

// NewGaneshaDBusClient connects lazily and reconnects transparently across
// Ganesha restarts.
func NewGaneshaDBusClient(addr string) GaneshaClient {
	return &dbusGanesha{addr: addr}
}

type dbusGanesha struct {
	addr string
	mu   sync.Mutex
	conn *dbus.Conn
}

func (g *dbusGanesha) connect() (*dbus.Conn, error) {
	if g.conn != nil && g.conn.Connected() {
		return g.conn, nil
	}
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

// call expects a (bool, string) reply where false signals a Ganesha-side
// failure and the string carries the reason.
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
	c := obj.Call(method, 0, args...)
	if c.Err != nil {
		if g.conn != nil && !g.conn.Connected() {
			_ = g.conn.Close()
			g.conn = nil
		}
		return fmt.Errorf("%s: %w", method, c.Err)
	}
	if err := c.Store(&status, &message); err != nil {
		return fmt.Errorf("%s: decode reply: %w", method, err)
	}
	if !status {
		return fmt.Errorf("%s rejected: %s", method, message)
	}
	return nil
}

func (g *dbusGanesha) AddExport(exportID uint16, path, config string) error {
	log.WithFields(log.Fields{"exportID": exportID, "path": path}).Debug("ganesha AddExport")
	// Ganesha's parser is whitespace-strict — "Export_Id=N" works,
	// "Export_Id = N" is rejected.
	selector := fmt.Sprintf("EXPORT(Export_Id=%d)", exportID)
	err := g.call(ganeshaAddExport, path, selector)
	if err != nil && isAlreadyAddedError(err) {
		return nil
	}
	return err
}

func (g *dbusGanesha) RemoveExport(exportID uint16) error {
	log.WithField("exportID", exportID).Debug("ganesha RemoveExport")
	err := g.call(ganeshaRemExport, exportID)
	if err != nil && isNotFoundError(err) {
		return nil
	}
	return err
}

// isAlreadyAddedError recognises Ganesha's several worded-not-coded replies
// when an export/id is already present. See
// https://github.com/nfs-ganesha/nfs-ganesha/wiki/Dbusinterface — no
// distinct error codes are returned.
func isAlreadyAddedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate export id") ||
		strings.Contains(msg, "is a duplicate") ||
		strings.Contains(msg, "already added") ||
		strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "already active") ||
		strings.Contains(msg, "invalid param value")
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Export id not found") ||
		strings.Contains(msg, "lookup_export failed")
}
