package scraper

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/scalegate/scalegate/internal/k8s"
	"github.com/scalegate/scalegate/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

const (
	AnnotationPort   = "scalegate.io/metrics-port"
	AnnotationMetric = "scalegate.io/metric-name"
	DefaultPort      = "8080"
	DefaultMetric    = "scalegate_active_tasks"
)

func Run(ctx context.Context, clientset *kubernetes.Clientset, podListers []corev1listers.PodLister, interval, timeout time.Duration) {
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

			metrics.PodsDiscovered.Set(float64(len(allPods)))

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
			metrics.PodsGuarded.Set(float64(guardedCount))
			metrics.LoopDuration.Observe(time.Since(start).Seconds())
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
		metrics.ScrapeErrors.Inc()
		slog.Error("Failed to scrape pod", "namespace", pod.Namespace, "pod", pod.Name, "url", url, "error", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		metrics.ScrapeErrors.Inc()
		slog.Error("Non-200 status code scraping pod", "namespace", pod.Namespace, "pod", pod.Name, "statusCode", resp.StatusCode)
		return false
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		metrics.ScrapeErrors.Inc()
		slog.Error("Failed to read response body for pod", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
		return false
	}

	val, found := parsePrometheusMetric(string(bodyBytes), metricName)
	if !found {
		val = 0
	}

	metrics.PodsScraped.Inc()

	desiredCostStr := strconv.Itoa(int(val))

	currentCostStr := pod.Annotations[k8s.AnnotationDeletionCost]

	// If missing, K8s treats it as 0. Avoid unnecessary patches.
	if currentCostStr == "" && desiredCostStr == "0" {
		return val > 0
	}

	if currentCostStr != desiredCostStr {
		slog.Info("Patching Pod deletion cost", "namespace", pod.Namespace, "pod", pod.Name, "oldCost", currentCostStr, "newCost", desiredCostStr, "metric", metricName, "value", val)
		k8s.PatchCost(ctx, clientset, pod, desiredCostStr)
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
