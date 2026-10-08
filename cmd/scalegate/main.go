package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scalegate/scalegate/internal/metrics"
	"github.com/scalegate/scalegate/internal/operator"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

func main() {
	// Initialize structured JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Redirect client-go internal logs to our slog JSON logger
	klog.SetSlogLogger(logger)

	slog.Info("Starting Scalegate Operator...")

	var config *rest.Config
	var err error

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			home := os.Getenv("HOME")
			if home != "" {
				config, err = clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
			}
		}
	}
	if err != nil {
		slog.Error("Failed to create k8s config", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Error("Failed to create k8s client", "error", err)
		os.Exit(1)
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		// Fallback for local development
		hostname, _ := os.Hostname()
		podName = hostname
		slog.Info("POD_NAME env var not set, using hostname for leader election identity", "hostname", podName)
	}
	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		podNamespace = "scalegate" // fallback
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "scalegate-leader-lock",
			Namespace: podNamespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Received termination signal, shutting down...")
		cancel()
	}()

	if os.Getenv("ENABLE_METRICS") == "true" {
		port := os.Getenv("METRICS_PORT")
		if port == "" {
			port = "8081"
		}
		slog.Info("Starting metrics server", "port", port)
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			if err := http.ListenAndServe(":"+port, nil); err != nil {
				slog.Error("Metrics server failed", "error", err)
			}
		}()
	}

	if os.Getenv("ENABLE_LEADER_ELECTION") == "false" {
		slog.Info("Leader election disabled. Starting operators immediately...")
		metrics.LeaderStatus.Set(1)
		operator.Run(ctx, clientset)
		return
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				slog.Info("Started leading. Starting operators...")
				metrics.LeaderStatus.Set(1)
				operator.Run(ctx, clientset)
			},
			OnStoppedLeading: func() {
				slog.Info("Stopped leading. Shutting down...")
				metrics.LeaderStatus.Set(0)
				// The context is cancelled, exit
			},
			OnNewLeader: func(identity string) {
				if identity == podName {
					return
				}
				slog.Info("New leader elected", "identity", identity)
			},
		},
	})
}
