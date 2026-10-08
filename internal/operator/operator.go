package operator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/scalegate/scalegate/internal/scraper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const LabelEnabled = "scalegate.io/enabled"

func Run(ctx context.Context, clientset *kubernetes.Clientset) {
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
	scraper.Run(ctx, clientset, listers, interval, timeout)
}
