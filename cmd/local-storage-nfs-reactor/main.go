// Command local-storage-nfs-reactor runs a per-node agent that watches
// LocalVolume CRs annotated for NFS (RWX) export, mounts the backing LVM
// device, instructs a co-located NFS-Ganesha daemon to export it via DBus,
// and maintains an EndpointSlice pointing at this pod for the tenant Service.
//
// Designed to run as a DaemonSet sidecar beside NFS-Ganesha on every storage
// node. Reconciliation is idempotent and safe to restart: state is
// reconstructed from /proc/mounts and the LocalVolume CRs at boot.
package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	apisv1alpha1 "github.com/hwameistor/hwameistor/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/hwameistor/pkg/local-storage/member/node/nfsreactor"
)

var (
	nodeName       = flag.String("nodename", "", "Node name (required)")
	podIP          = flag.String("pod-ip", "", "Pod IP for EndpointSlice (required)")
	podNamespace   = flag.String("pod-namespace", "", "Pod namespace (required)")
	dbusAddr       = flag.String("ganesha-dbus", "unix:/run/dbus/system_bus_socket", "Ganesha DBus system bus socket")
	exportRoot     = flag.String("export-root", "/srv/exports", "Mount root for NFS exports")
	resyncInterval = flag.Duration("resync", 15*time.Second, "Periodic full resync interval")
	logLevel       = flag.Int("v", 4, "log verbosity")
)

var BUILDVERSION, BUILDTIME, GOVERSION string

func setupLogging() {
	var level log.Level
	if *logLevel >= int(log.TraceLevel) {
		level = log.TraceLevel
	} else if *logLevel <= int(log.PanicLevel) {
		level = log.PanicLevel
	} else {
		level = log.Level(*logLevel)
	}
	log.SetLevel(level)
	log.SetFormatter(&log.JSONFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			s := strings.Split(f.Function, ".")
			funcName := s[len(s)-1]
			fileName := path.Base(f.File)
			return funcName, fmt.Sprintf("%s:%d", fileName, f.Line)
		},
	})
	log.SetReportCaller(true)
}

func main() {
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	flag.Parse()
	setupLogging()

	log.WithFields(log.Fields{
		"GitCommit": BUILDVERSION,
		"BuildDate": BUILDTIME,
		"GoVersion": GOVERSION,
	}).Info("local-storage-nfs-reactor starting")

	if *nodeName == "" || *podIP == "" || *podNamespace == "" {
		log.Fatal("--nodename, --pod-ip, --pod-namespace are required")
	}

	cfg, err := config.GetConfig()
	if err != nil {
		log.Fatalf("failed to get kubeconfig: %s", err)
	}

	mgr, err := manager.New(cfg, manager.Options{Namespace: ""})
	if err != nil {
		log.Fatalf("failed to build manager: %s", err)
	}
	if err := apisv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("failed to register hwameistor scheme: %s", err)
	}

	r := nfsreactor.New(nfsreactor.Options{
		Client:         mgr.GetClient(),
		NodeName:       *nodeName,
		PodIP:          *podIP,
		PodNamespace:   *podNamespace,
		DbusAddr:       *dbusAddr,
		ExportRoot:     *exportRoot,
		ResyncInterval: *resyncInterval,
	})
	if err := r.SetupWithManager(mgr); err != nil {
		log.Fatalf("failed to register reactor: %s", err)
	}

	stopCtx := signals.SetupSignalHandler()
	log.Info("starting manager")
	if err := mgr.Start(stopCtx); err != nil {
		log.WithError(err).Error("manager exited with error")
		os.Exit(1)
	}
	log.Info("local-storage-nfs-reactor stopped cleanly")
}
