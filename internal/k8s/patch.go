package k8s

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/scalegate/scalegate/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const AnnotationDeletionCost = "controller.kubernetes.io/pod-deletion-cost"

func PatchCost(ctx context.Context, clientset *kubernetes.Clientset, pod *corev1.Pod, costStr string) {
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
		metrics.PatchAPIErrors.Inc()
		slog.Error("Failed to patch pod", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
	}
}
