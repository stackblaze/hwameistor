//go:build linux

package nfsreactor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

// sighupGaneshaReload reads ganesha's pid file and sends SIGHUP.
// Missing pid file is tolerated — ganesha hasn't started yet, our
// config file is staged and will be picked up on boot.
func sighupGaneshaReload(pidFile string) error {
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.WithField("pidFile", pidFile).Debug("ganesha not running yet; config staged for boot")
			return nil
		}
		return fmt.Errorf("read %s: %w", pidFile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return fmt.Errorf("parse %s (%q): %w", pidFile, raw, err)
	}
	// Guard against signalling ourselves. Without shareProcessNamespace=true
	// on the pod, ganesha's pid file (written from its own namespace)
	// contains a PID that's meaningless to us — most often "1", which in
	// the reactor's own namespace is the reactor itself. SIGHUP to self
	// hits Go's default handler and terminates the process.
	if pid <= 1 || pid == os.Getpid() {
		log.WithField("pid", pid).Warn("ganesha pid file points at self/init; pod likely missing shareProcessNamespace=true — skipping SIGHUP")
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
		return fmt.Errorf("sighup pid %d: %w", pid, err)
	}
	log.WithField("pid", pid).Debug("ganesha SIGHUP sent")
	return nil
}
