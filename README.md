# Scalegate

**Scalegate** is a zero-bloat, highly available Kubernetes Operator that natively protects your active worker pods from premature downscaling. 

By directly scraping your pods' Prometheus metrics and dynamically patching their Kubernetes API `deletion-cost`, Scalegate ensures that pods actively processing background tasks (or handling long-running websocket connections) are the **last** to be terminated by the Horizontal Pod Autoscaler (HPA) when traffic subsides.

No CRDs. No external metric servers. No complex state. Just pure Go efficiency.

---

## Features

- **Stateless Design**: Requires no Custom Resource Definitions (CRDs) or external databases. It communicates directly with the Kubernetes API and your Pods.
- **Direct Metric Scraping**: Bypasses the Kubernetes Metrics Server by directly parsing your app's native `/metrics` endpoint using Prometheus `expfmt`.
- **Multi-Namespace & Least-Privilege**: Watch the entire cluster or securely scope the operator to specific namespaces via comma-separated configuration.
- **High Availability**: Built-in `client-go` Leader Election allows you to run multiple Scalegate replicas safely. If the leader goes down, a standby instantly takes over.
- **Rich Observability**: Ships with its own Prometheus metrics server so you can monitor the operator's health, scrape errors, API latencies, and how many pods are actively guarded.

---

## How It Works

Scalegate runs a configurable background loop that watches for Pods carrying the `scalegate.io/enabled=true` label.

1. It makes a lightweight HTTP request to the Pod's Prometheus `/metrics` endpoint.
2. It parses the payload looking for your designated "active tasks" metric (e.g., `scalegate_active_tasks`).
3. It dynamically patches the Pod's `controller.kubernetes.io/pod-deletion-cost` annotation directly to the integer value of your metric.
4. The Kubernetes HPA natively respects this cost. When scaling down, Kubernetes naturally evicts the most idle Pods (`0` cost) first, completely shielding your active workers and systematically balancing terminations based on live workload size!

---

## Installation

Deploy Scalegate using the included Helm chart:

```bash
helm upgrade --install scalegate ./charts/scalegate -n scalegate --create-namespace
```

### Helm Configuration (`values.yaml`)

| Key | Default | Description |
|-----|---------|-------------|
| `watchNamespaces` | `""` | Comma-separated list of namespaces to watch. Leave empty to watch the entire cluster. |
| `replicaCount` | `2` | Number of operator replicas (uses Leader Election). |
| `leaderElection.enabled` | `true` | Enable HA leader election. Set to false for single-node efficiency. |
| `metrics.enabled` | `true` | Expose operator metrics for Prometheus to scrape. |
| `config.scrapeInterval` | `"3s"` | How often to evaluate Pod metrics. |
| `config.httpTimeout` | `"1500ms"` | Timeout for Pod scraping. |

---

## Potecting Your Pods (Dynamic Overrides)

Scalegate is **completely workload-agnostic**. It dynamically reads the annotations on *each individual pod* as it scrapes them. This means you can have a Node.js app exposing metrics on port `3000` and a Python app exposing metrics on port `8000` in the same cluster—Scalegate handles them all seamlessly without any operator reconfiguration.

To enroll a deployment, add the enablement label and (optionally) override the defaults via annotations:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-worker-app
spec:
  template:
    metadata:
      labels:
        # 1. Enable Scalegate for this Pod (Required)
        scalegate.io/enabled: "true"
      annotations:
        # 2. Override the port Scalegate scrapes (Optional. Default is "8080")
        scalegate.io/metrics-port: "9090"
        # 3. Override the metric name to look for (Optional. Default is "scalegate_active_tasks")
        scalegate.io/metric-name: "sidekiq_active_jobs_total"
```

---

## Operator Metrics

When `metrics.enabled` is `true`, Scalegate exposes standard Go runtime metrics along with these advanced operator-specific metrics on `/metrics` (Port 8081):

| Metric | Type | Description |
|--------|------|-------------|
| `scalegate_pods_scraped_total` | Counter | Total successful pod HTTP scrapes. |
| `scalegate_scrape_errors_total` | Counter | Failed scrapes (network timeouts, 404s, malformed metrics). |
| `scalegate_patch_api_errors_total` | Counter | Kubernetes API rejections when patching deletion costs. |
| `scalegate_operator_loop_duration_seconds` | Histogram | How long the entire scraper loop takes to execute. |
| `scalegate_pods_discovered` | Gauge | Total number of pods found bearing the enable label. |
| `scalegate_pods_guarded` | Gauge | Exact number of pods currently protected (deletion cost > 0). |
| `scalegate_leader_status` | Gauge | Indicates if this operator pod is the active leader (`1`) or standby (`0`). |