package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

const (
	AnnotationDeletionCost = "controller.kubernetes.io/pod-deletion-cost"
	LabelEnabled           = "scalegate.io/enabled"
	AnnotationPort         = "scalegate.io/metrics-port"
	AnnotationMetric       = "scalegate.io/metric-name"
	DefaultPort            = "8080"
	DefaultMetric          = "scalegate_active_tasks"
)

var (
	metricPodsScraped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_pods_scraped_total",
		Help: "The total number of successful pod scrapes",
	})
	metricScrapeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_scrape_errors_total",
		Help: "The total number of failed pod scrapes",
	})
	metricPodsGuarded = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_pods_guarded",
		Help: "The current number of pods actively guarded (deletion cost > 0)",
	})
	metricPodsDiscovered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_pods_discovered",
		Help: "The current number of pods discovered with the scalegate enabled label",
	})
	metricPatchAPIErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_patch_api_errors_total",
		Help: "The total number of Kubernetes API errors when patching pod deletion cost",
	})
	metricLoopDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scalegate_operator_loop_duration_seconds",
		Help:    "How long it takes to process all discovered pods in a single scraper loop",
		Buckets: prometheus.DefBuckets,
	})
	metricLeaderStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_leader_status",
		Help: "Indicates if this replica is the current leader (1) or standby (0)",
	})
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
		metricLeaderStatus.Set(1)
		runOperator(ctx, clientset)
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
				metricLeaderStatus.Set(1)
				runOperator(ctx, clientset)
			},
			OnStoppedLeading: func() {
				slog.Info("Stopped leading. Shutting down...")
				metricLeaderStatus.Set(0)
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

func runOperator(ctx context.Context, clientset *kubernetes.Clientset) {
	watchNamespacesStr := os.Getenv("WATCH_NAMESPACES")

	var listers []corev1listers.PodLister
	var hasSyncedFuncs []cache.InformerSynced

	if watchNamespacesStr != "" {
		namespaces := strings.Split(watchNamespacesStr, ",")
		for _, ns := range namespaces {
			ns = strings.TrimSpace(ns)
			if ns == "" {
				continue
			}
			slog.Info("Starting operator with namespace scope", "namespace", ns)
			factory := informers.NewSharedInformerFactoryWithOptions(
				clientset,
				0,
				informers.WithNamespace(ns),
				informers.WithTweakListOptions(func(options *metav1.ListOptions) {
					options.LabelSelector = fmt.Sprintf("%s=true", LabelEnabled)
				}),
			)
			podInformer := factory.Core().V1().Pods().Informer()
			factory.Start(ctx.Done())
			hasSyncedFuncs = append(hasSyncedFuncs, podInformer.HasSynced)
			listers = append(listers, factory.Core().V1().Pods().Lister())
		}
	} else {
		slog.Info("Starting operator with cluster scope")
		factory := informers.NewSharedInformerFactoryWithOptions(
			clientset,
			0,
			informers.WithTweakListOptions(func(options *metav1.ListOptions) {
				options.LabelSelector = fmt.Sprintf("%s=true", LabelEnabled)
			}),
		)
		podInformer := factory.Core().V1().Pods().Informer()
		factory.Start(ctx.Done())
		hasSyncedFuncs = append(hasSyncedFuncs, podInformer.HasSynced)
		listers = append(listers, factory.Core().V1().Pods().Lister())
	}

	slog.Info("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(ctx.Done(), hasSyncedFuncs...) {
		slog.Error("Failed to sync cache")
		return
	}

	intervalStr := os.Getenv("SCRAPE_INTERVAL_SECONDS")
	interval := 3 * time.Second
	if intervalStr != "" {
		if val, err := strconv.Atoi(intervalStr); err == nil {
			interval = time.Duration(val) * time.Second
		}
	}

	timeoutStr := os.Getenv("SCRAPE_TIMEOUT_SECONDS")
	timeout := 2 * time.Second
	if timeoutStr != "" {
		if val, err := strconv.Atoi(timeoutStr); err == nil {
			timeout = time.Duration(val) * time.Second
		}
	}

	slog.Info("Informer cache synced. Starting scraper loop", "interval", interval.String(), "timeout", timeout.String())
	runScraper(ctx, clientset, listers, interval, timeout)
}

func runScraper(ctx context.Context, clientset *kubernetes.Clientset, podListers []corev1listers.PodLister, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: timeout}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := time.Now()
			var allPods []*corev1.Pod
			for _, lister := range podListers {
				pods, err := lister.List(labels.Everything())
				if err != nil {
					slog.Error("Error listing pods", "error", err)
					continue
				}
				allPods = append(allPods, pods...)
			}

			metricPodsDiscovered.Set(float64(len(allPods)))

			var wg sync.WaitGroup
			var guardedCount int32
			for _, pod := range allPods {
				if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
					continue
				}

				wg.Add(1)
				go func(p *corev1.Pod) {
					defer wg.Done()
					if processPod(ctx, clientset, httpClient, p) {
						atomic.AddInt32(&guardedCount, 1)
					}
				}(pod)
			}
			wg.Wait()
			metricPodsGuarded.Set(float64(guardedCount))
			metricLoopDuration.Observe(time.Since(start).Seconds())
		}
	}
}

func processPod(ctx context.Context, clientset *kubernetes.Clientset, client *http.Client, pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}

	port := DefaultPort
	if val, ok := pod.Annotations[AnnotationPort]; ok {
		port = val
	}

	metricName := DefaultMetric
	if val, ok := pod.Annotations[AnnotationMetric]; ok {
		metricName = val
	}

	url := fmt.Sprintf("http://%s:%s/metrics", pod.Status.PodIP, port)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Error("Failed to create request for pod", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		metricScrapeErrors.Inc()
		slog.Error("Failed to scrape pod", "namespace", pod.Namespace, "pod", pod.Name, "url", url, "error", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		metricScrapeErrors.Inc()
		slog.Error("Non-200 status code scraping pod", "namespace", pod.Namespace, "pod", pod.Name, "statusCode", resp.StatusCode)
		return false
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		metricScrapeErrors.Inc()
		slog.Error("Failed to read response body for pod", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
		return false
	}

	val, found := parsePrometheusMetric(string(bodyBytes), metricName)
	if !found {
		val = 0
	}

	metricPodsScraped.Inc()

	desiredCostStr := "0"
	if val > 0 {
		desiredCostStr = "10000"
	}

	currentCostStr := pod.Annotations[AnnotationDeletionCost]

	if currentCostStr != desiredCostStr {
		slog.Info("Patching Pod deletion cost", "namespace", pod.Namespace, "pod", pod.Name, "oldCost", currentCostStr, "newCost", desiredCostStr, "metric", metricName, "value", val)
		patchCost(ctx, clientset, pod, desiredCostStr)
	}

	return val > 0
}

func parsePrometheusMetric(payload, metricName string) (float64, bool) {
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(strings.NewReader(payload))
	if err != nil {
		return 0, false
	}

	mf, ok := metricFamilies[metricName]
	if !ok || len(mf.Metric) == 0 {
		return 0, false
	}

	m := mf.Metric[0]
	if m.Gauge != nil {
		return m.Gauge.GetValue(), true
	} else if m.Counter != nil {
		return m.Counter.GetValue(), true
	} else if m.Summary != nil {
		return m.Summary.GetSampleSum(), true
	} else if m.Histogram != nil {
		return m.Histogram.GetSampleSum(), true
	}

	return 0, false
}

func patchCost(ctx context.Context, clientset *kubernetes.Clientset, pod *corev1.Pod, costStr string) {
	patchPayload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				AnnotationDeletionCost: costStr,
			},
		},
	}

	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		slog.Error("Failed to marshal patch payload", "error", err)
		return
	}

	_, err = clientset.CoreV1().Pods(pod.Namespace).Patch(
		ctx,
		pod.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		metricPatchAPIErrors.Inc()
		slog.Error("Failed to patch pod", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
	}
}
