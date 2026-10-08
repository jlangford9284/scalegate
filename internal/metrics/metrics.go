package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PodsScraped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_pods_scraped_total",
		Help: "The total number of successful pod scrapes",
	})
	ScrapeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_scrape_errors_total",
		Help: "The total number of failed pod scrapes",
	})
	PodsGuarded = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_pods_guarded",
		Help: "The current number of pods actively guarded (deletion cost > 0)",
	})
	PodsDiscovered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_pods_discovered",
		Help: "The current number of pods discovered with the scalegate enabled label",
	})
	PatchAPIErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scalegate_patch_api_errors_total",
		Help: "The total number of Kubernetes API errors when patching pod deletion cost",
	})
	LoopDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scalegate_operator_loop_duration_seconds",
		Help:    "How long it takes to process all discovered pods in a single scraper loop",
		Buckets: prometheus.DefBuckets,
	})
	LeaderStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scalegate_leader_status",
		Help: "Indicates if this replica is the current leader (1) or standby (0)",
	})
)
