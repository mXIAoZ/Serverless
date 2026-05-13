package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ContainerMetrics is the schema sent to the gateway and returned by GET /metrics.
type ContainerMetrics struct {
	ContainerID     string    `json:"container_id"`
	Timestamp       time.Time `json:"timestamp"`
	InvocationCount int64     `json:"invocation_count"`
	SuccessCount    int64     `json:"success_count"`
	ErrorCount      int64     `json:"error_count"`
	P50LatencyMs    float64   `json:"p50_latency_ms"`
	P95LatencyMs    float64   `json:"p95_latency_ms"`
	P99LatencyMs    float64   `json:"p99_latency_ms"`
	MemoryBytes     int64     `json:"memory_bytes"`
	CPUUsageUs      int64     `json:"cpu_usage_us"`
}

const maxLatencyWindow = 1000

type agent struct {
	mu          sync.Mutex
	invocations int64
	successes   int64
	errors      int64
	latencies   []float64 // rolling window
}

var cgroupWarnOnce sync.Once

func main() {
	containerID := os.Getenv("CONTAINER_ID")
	if containerID == "" {
		containerID = selfContainerID()
	}
	if containerID == "" {
		// fallback: hostname is the short container ID in Docker
		containerID, _ = os.Hostname()
	}
	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "host.docker.internal:8080"
	}

	a := &agent{}

	if gatewayAddr != "" {
		go a.startReporter(gatewayAddr, containerID)
	} else {
		log.Println("[agent] GATEWAY_ADDR not set — metrics reporting disabled")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/invoke", a.handleInvoke)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/metrics", a.handleMetrics)

	log.Println("[agent] listening on :9001")
	if err := http.ListenAndServe(":9001", mux); err != nil {
		log.Fatal(err)
	}
}

// handleInvoke proxies POST /invoke to the runtime server and records metrics.
func (a *agent) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	start := time.Now()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		"http://localhost:9000/invoke", bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	req.Header.Set("X-Function-Name", r.Header.Get("X-Function-Name"))

	resp, err := http.DefaultClient.Do(req)
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

	if err != nil {
		a.record(latencyMs, false)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	a.record(latencyMs, resp.StatusCode < 500)

	// Stream response back verbatim.
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleHealth proxies GET /health to the runtime server with retries.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	var lastErr error
	for i := 0; i < 3; i++ {
		resp, err := http.Get("http://localhost:9000/health")
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			w.WriteHeader(resp.StatusCode)
			w.Write(body)
			return
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[agent] health proxy failed: %v", lastErr)
	http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
}

// handleMetrics returns a JSON snapshot of current metrics.
func (a *agent) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m := a.snapshot(os.Getenv("CONTAINER_ID"))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m)
}

// record adds a single invocation result to the rolling metrics state.
func (a *agent) record(latencyMs float64, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invocations++
	if success {
		a.successes++
	} else {
		a.errors++
	}
	a.latencies = append(a.latencies, latencyMs)
	if len(a.latencies) > maxLatencyWindow {
		a.latencies = a.latencies[len(a.latencies)-maxLatencyWindow:]
	}
}

// snapshot builds a ContainerMetrics value from current state.
func (a *agent) snapshot(containerID string) ContainerMetrics {
	a.mu.Lock()
	invocations := a.invocations
	successes := a.successes
	errors := a.errors
	latCopy := make([]float64, len(a.latencies))
	copy(latCopy, a.latencies)
	a.mu.Unlock()

	sort.Float64s(latCopy)
	return ContainerMetrics{
		ContainerID:     containerID,
		Timestamp:       time.Now().UTC(),
		InvocationCount: invocations,
		SuccessCount:    successes,
		ErrorCount:      errors,
		P50LatencyMs:    percentile(latCopy, 50),
		P95LatencyMs:    percentile(latCopy, 95),
		P99LatencyMs:    percentile(latCopy, 99),
		MemoryBytes:     readMemoryBytes(),
		CPUUsageUs:      readCPUUsageUs(),
	}
}

// startReporter sends metrics to the gateway every 10 seconds.
func (a *agent) startReporter(gatewayAddr, containerID string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	url := "http://" + gatewayAddr + "/containers/" + containerID + "/metrics"
	for range ticker.C {
		m := a.snapshot(containerID)
		data, _ := json.Marshal(m)
		resp, err := http.Post(url, "application/json", bytes.NewReader(data))
		if err != nil {
			log.Printf("[agent] report failed: %v", err)
			continue
		}
		resp.Body.Close()
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}

func readMemoryBytes() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		cgroupWarnOnce.Do(func() { log.Println("[agent] cgroup memory unavailable (non-Linux?)") })
		return 0
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return v
}

func readCPUUsageUs() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "usage_usec ") {
			v, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "usage_usec ")), 10, 64)
			return v
		}
	}
	return 0
}

// selfContainerID reads the container ID from /proc/self/cgroup (cgroup v2).
func selfContainerID() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		// cgroup v2: "0::/system.slice/docker-<id>.scope"
		if idx := strings.Index(line, "docker-"); idx != -1 {
			rest := line[idx+len("docker-"):]
			if end := strings.Index(rest, ".scope"); end != -1 {
				return rest[:end]
			}
		}
		// cgroup v1: "12:devices:/docker/<id>"
		parts := strings.Split(line, "/")
		if len(parts) >= 3 && parts[len(parts)-2] == "docker" {
			id := parts[len(parts)-1]
			if len(id) == 64 {
				return id
			}
		}
	}
	return ""
}
