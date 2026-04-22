package nfsreactor

import (
	"fmt"
	"os"
	"path/filepath"

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

// GaneshaClient publishes / revokes exports. The real implementation
// writes per-export .conf files into a directory ganesha's top-level
// config %dir-includes, then sends SIGHUP. Ganesha (>= v2.8) re-parses
// and diffs, picking up adds, removes, and updates without dropping
// live client connections. Tests inject a fake.
type GaneshaClient interface {
	AddExport(exportID uint16, path, config string) error
	RemoveExport(exportID uint16) error
}

// NewGaneshaClient returns a SIGHUP-based client rooted at exportsDir
// (same dir ganesha's config %dir-includes), reading the ganesha pid
// from pidFile.
func NewGaneshaClient(exportsDir, pidFile string) GaneshaClient {
	return &sighupGanesha{exportsDir: exportsDir, pidFile: pidFile}
}

type sighupGanesha struct {
	exportsDir string
	pidFile    string
}

// AddExport writes <exportsDir>/<exportID>.conf and SIGHUPs ganesha.
// The `path` arg is ignored — config is the authoritative content.
func (g *sighupGanesha) AddExport(exportID uint16, path, config string) error {
	if err := os.MkdirAll(g.exportsDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", g.exportsDir, err)
	}
	fp := g.confPath(exportID)
	// Atomic: write tmp + rename, so ganesha can't read a half-written
	// file if a reload races the write.
	tmp := fp + ".tmp"
	if err := os.WriteFile(tmp, []byte(config), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, fp); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, fp, err)
	}
	log.WithFields(log.Fields{"exportID": exportID, "file": fp}).Debug("ganesha AddExport: wrote config")
	return sighupGaneshaReload(g.pidFile)
}

// RemoveExport deletes the config file and SIGHUPs. Ganesha detects the
// missing EXPORT block on reload and purges it.
func (g *sighupGanesha) RemoveExport(exportID uint16) error {
	fp := g.confPath(exportID)
	if err := os.Remove(fp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", fp, err)
	}
	log.WithFields(log.Fields{"exportID": exportID, "file": fp}).Debug("ganesha RemoveExport: removed config")
	return sighupGaneshaReload(g.pidFile)
}

func (g *sighupGanesha) confPath(exportID uint16) string {
	return filepath.Join(g.exportsDir, fmt.Sprintf("%d.conf", exportID))
}
