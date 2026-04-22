// Command local-storage-nfs-reactor is the DaemonSet-side agent that
// bridges HwameiStor LocalVolumes and a co-located NFS-Ganesha daemon.
// See pkg/local-storage/member/node/nfsreactor for details.
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
	"github.com/hwameistor/hwameistor/pkg/local-storage/member/rwx"
)

var (
	nodeName       = flag.String("nodename", "", "Node name (required)")
	podIP          = flag.String("pod-ip", "", "Pod IP for EndpointSlice (required)")
	podNamespace   = flag.String("pod-namespace", "", "Pod namespace (required)")
	exportRoot     = flag.String("export-root", rwx.ExportRootDefault, "Mount root for NFS exports")
	pidFile        = flag.String("ganesha-pid", "/var/run/ganesha/ganesha.pid", "Path to ganesha.nfsd pid file (shared via emptyDir with the ganesha container)")
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
		ExportRoot:     *exportRoot,
		GaneshaPidFile: *pidFile,
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
