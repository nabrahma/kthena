/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	backendmetrics "github.com/volcano-sh/kthena/pkg/kthena-router/backend/metrics"
	"github.com/volcano-sh/kthena/pkg/kthena-router/backend/vllm"
	routerutils "github.com/volcano-sh/kthena/pkg/kthena-router/utils"
	plugincontext "github.com/volcano-sh/kthena/test/e2e/router/router-plugins/context"
	"github.com/volcano-sh/kthena/test/e2e/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	pluginMockReplicaCount         = 3
	leastRequestMaxWaitingRequests = 1
	leastRequestLoadWaitTimeout    = 60 * time.Second
	gpuCacheUsageBusyMin           = 0.1
	gpuCacheUsageIdleMax           = 0.05
	gpuCacheUsageLoadWaitTimeout   = 90 * time.Second
	gpuCacheUsageLoadConcurrency   = 6
	gpuCacheUsageLoadMaxTokens     = 256
)

func listReadyMockPods(t *testing.T, kube kubernetes.Interface, namespace string) []corev1.Pod {
	t.Helper()
	ready := utils.ListReadyPodsByLabel(t, kube, namespace, "app="+plugincontext.DeploymentName)
	require.NotEmpty(t, ready, "no ready mock pods")
	return ready
}

var pluginMockKVCacheSimArgs = []string{
	"--model=kthena-plugin-mock",
	"--served-model-name=router-plugin-model",
	"--port=8000",
	"--mode=echo",
	"--enable-kvcache=true",
	"--force-dummy-tokenizer=true",
	"--kv-cache-size=8",
	"--block-size=8",
	"--max-num-seqs=2",
	"--time-to-first-token=2s",
	"--inter-token-latency=200ms",
}

// applyPluginMockKVCacheProfile patches the shared plugin mock deployment for gpu-usage e2e
// and restores the baseline LLM-Mock-plugins.yaml spec on cleanup.
func applyPluginMockKVCacheProfile(t *testing.T, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	baseline := utils.LoadYAMLFromFile[appsv1.Deployment](filepath.Join(plugincontext.TestDataDir, "LLM-Mock-plugins.yaml"))
	baseline.Namespace = namespace

	dep, err := kube.AppsV1().Deployments(namespace).Get(ctx, plugincontext.DeploymentName, metav1.GetOptions{})
	require.NoError(t, err, "get plugin mock deployment")

	container := &dep.Spec.Template.Spec.Containers[0]
	container.Args = append([]string(nil), pluginMockKVCacheSimArgs...)
	container.Env = []corev1.EnvVar{{
		Name: "POD_IP",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		},
	}}

	_, err = kube.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
	require.NoError(t, err, "patch plugin mock deployment for kv-cache simulation")
	require.NoError(t, utils.WaitForDeploymentReadyE(ctx, kube, namespace, plugincontext.DeploymentName, 5*time.Minute),
		"wait for kv-cache plugin mock rollout")

	t.Cleanup(func() {
		cleanupCtx := context.Background()
		latest, err := kube.AppsV1().Deployments(namespace).Get(cleanupCtx, plugincontext.DeploymentName, metav1.GetOptions{})
		if err != nil {
			return
		}
		latest.Spec = *baseline.Spec.DeepCopy()
		if _, err := kube.AppsV1().Deployments(namespace).Update(cleanupCtx, latest, metav1.UpdateOptions{}); err != nil {
			return
		}
		_ = utils.WaitForDeploymentReadyE(cleanupCtx, kube, namespace, plugincontext.DeploymentName, 5*time.Minute)
	})
}

func waitForSchedulerPluginInMetrics(t *testing.T, metricsURL, pluginName, pluginType string) {
	t.Helper()
	require.Eventually(t, func() bool {
		metricsData, err := backendmetrics.ParseMetricsURL(metricsURL)
		if err != nil {
			return false
		}
		return utils.GetHistogramCount(metricsData, "kthena_router_scheduler_plugin_duration_seconds", map[string]string{
			"plugin": pluginName,
			"type":   pluginType,
		}) > 0
	}, 30*time.Second, time.Second)
}

type routerPodMetricsSnapshot struct {
	RequestWaitingNum float64
	RequestRunningNum float64
}

func fetchRouterPodMetricsViaDebug(t *testing.T, debugBaseURL string, pod corev1.Pod) (routerPodMetricsSnapshot, bool) {
	t.Helper()
	url := fmt.Sprintf("%s/debug/config_dump/namespaces/%s/pods/%s", debugBaseURL, pod.Namespace, pod.Name)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return routerPodMetricsSnapshot{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return routerPodMetricsSnapshot{}, false
	}
	var parsed struct {
		Metrics *struct {
			RequestWaitingNum float64 `json:"requestWaitingNum"`
			RequestRunningNum float64 `json:"requestRunningNum"`
		} `json:"metrics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil || parsed.Metrics == nil {
		return routerPodMetricsSnapshot{}, false
	}
	return routerPodMetricsSnapshot{
		RequestWaitingNum: parsed.Metrics.RequestWaitingNum,
		RequestRunningNum: parsed.Metrics.RequestRunningNum,
	}, true
}

type mockPodMetricsPortForward struct {
	pod     corev1.Pod
	metrics string
	close   func()
}

func setupMockPodMetricsPortForward(t *testing.T, pod corev1.Pod) mockPodMetricsPortForward {
	t.Helper()
	localPort := utils.AllocateLocalPort(t)
	pf, err := utils.SetupPortForwardToPod(pod.Namespace, pod.Name, localPort, "8000")
	require.NoError(t, err, "port-forward to mock pod %s for /metrics", pod.Name)
	return mockPodMetricsPortForward{
		pod:     pod,
		metrics: fmt.Sprintf("http://127.0.0.1:%s/metrics", localPort),
		close:   func() { pf.Close() },
	}
}

func fetchMockPodKVCacheUsage(metricsURL string) (float64, bool) {
	allMetrics, err := backendmetrics.ParseMetricsURL(metricsURL)
	if err != nil {
		return 0, false
	}
	countMetrics := vllm.NewVllmEngine().GetCountMetricsInfo(allMetrics)
	kvUsage, ok := countMetrics[routerutils.KVCacheUsage]
	return kvUsage, ok
}

func waitForMockPodKVCacheSeparation(t *testing.T, busyPods []corev1.Pod, idlePod corev1.Pod) {
	t.Helper()
	require.NotEmpty(t, busyPods)

	forwards := make([]mockPodMetricsPortForward, 0, len(busyPods)+1)
	for _, pod := range busyPods {
		forwards = append(forwards, setupMockPodMetricsPortForward(t, pod))
	}
	forwards = append(forwards, setupMockPodMetricsPortForward(t, idlePod))
	defer func() {
		for _, forward := range forwards {
			forward.close()
		}
	}()

	require.Eventually(t, func() bool {
		allBusyHot := true
		for _, forward := range forwards[:len(busyPods)] {
			kvUsage, ok := fetchMockPodKVCacheUsage(forward.metrics)
			if !ok || kvUsage < gpuCacheUsageBusyMin {
				allBusyHot = false
				break
			}
		}
		idleForward := forwards[len(forwards)-1]
		idleUsage, okIdle := fetchMockPodKVCacheUsage(idleForward.metrics)
		if !okIdle {
			return false
		}
		idleCool := idleUsage < gpuCacheUsageIdleMax
		if allBusyHot && idleCool {
			for _, forward := range forwards[:len(busyPods)] {
				kvUsage, _ := fetchMockPodKVCacheUsage(forward.metrics)
				t.Logf("gpu-usage load ready: busy %s kv_cache=%.3f (mock /metrics)", forward.pod.Name, kvUsage)
			}
			t.Logf("gpu-usage load ready: idle %s kv_cache=%.3f (mock /metrics)", idleForward.pod.Name, idleUsage)
		}
		return allBusyHot && idleCool
	}, gpuCacheUsageLoadWaitTimeout, 2*time.Second,
		"all busy pods should report kv_cache_usage_perc >= %.2f and idle pod %s should report < %.2f on mock /metrics",
		gpuCacheUsageBusyMin, idlePod.Name, gpuCacheUsageIdleMax)
}

func waitForLeastRequestLoadSeparation(t *testing.T, kube kubernetes.Interface, kthenaNamespace string, busyPods []corev1.Pod, idlePod corev1.Pod, maxWaitingRequests int) {
	t.Helper()
	require.NotEmpty(t, busyPods)
	require.Greater(t, maxWaitingRequests, 0)
	threshold := float64(maxWaitingRequests)

	routerPod := utils.GetRouterPod(t, kube, kthenaNamespace)
	localPort := utils.AllocateLocalPort(t)
	pf, err := utils.SetupPortForwardToPod(routerPod.Namespace, routerPod.Name, localPort, utils.RouterDebugPort)
	require.NoError(t, err, "port-forward to router debug API")
	defer pf.Close()

	debugBaseURL := fmt.Sprintf("http://127.0.0.1:%s", localPort)
	require.Eventually(t, func() bool {
		allBusySaturated := true
		for _, busyPod := range busyPods {
			busy, okBusy := fetchRouterPodMetricsViaDebug(t, debugBaseURL, busyPod)
			if !okBusy || busy.RequestWaitingNum < threshold {
				allBusySaturated = false
				break
			}
		}
		idle, okIdle := fetchRouterPodMetricsViaDebug(t, debugBaseURL, idlePod)
		if !okIdle {
			return false
		}
		idleFree := idle.RequestWaitingNum < threshold
		if allBusySaturated && idleFree {
			for _, busyPod := range busyPods {
				busy, _ := fetchRouterPodMetricsViaDebug(t, debugBaseURL, busyPod)
				t.Logf("least-request load ready: busy %s waiting=%.0f running=%.0f",
					busyPod.Name, busy.RequestWaitingNum, busy.RequestRunningNum)
			}
			t.Logf("least-request load ready: idle %s waiting=%.0f running=%.0f",
				idlePod.Name, idle.RequestWaitingNum, idle.RequestRunningNum)
		}
		return allBusySaturated && idleFree
	}, leastRequestLoadWaitTimeout, 2*time.Second,
		"all busy pods should have request_waiting >= %d and idle pod %s should have request_waiting < %d",
		maxWaitingRequests, idlePod.Name, maxWaitingRequests)
}

const (
	schedulerOnlyPrefixCache = `scheduler:
  pluginConfig:
  - name: prefix-cache
    args:
      blockSizeToHash: 64
      maxBlocksToMatch: 128
      maxHashCacheSize: 50000
      topKMatches: 5
  plugins:
    Filter:
      enabled: []
    Score:
      enabled:
        - name: prefix-cache
          weight: 1`

	schedulerOnlyLeastRequest = `scheduler:
  pluginConfig:
  - name: least-request
    args:
      maxWaitingRequests: 1
  plugins:
    Filter:
      enabled:
        - least-request
    Score:
      enabled:
        - name: least-request
          weight: 1`

	schedulerOnlyLeastLatency = `scheduler:
  pluginConfig:
  - name: least-latency
    args:
      TTFTTPOTWeightFactor: 0.5
  plugins:
    Filter:
      enabled: []
    Score:
      enabled:
        - name: least-latency
          weight: 1`

	schedulerOnlyLoraAffinity = `scheduler:
  pluginConfig: []
  plugins:
    Filter:
      enabled:
        - lora-affinity
    Score:
      enabled:
        - name: random
          weight: 1`

	schedulerOnlyRandom = `scheduler:
  pluginConfig: []
  plugins:
    Filter:
      enabled: []
    Score:
      enabled:
        - name: random
          weight: 1`

	schedulerOnlyGPUCacheUsage = `scheduler:
  pluginConfig: []
  plugins:
    Filter:
      enabled: []
    Score:
      enabled:
        - name: gpu-usage
          weight: 1`
)
