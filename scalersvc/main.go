package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ContainerMetrics struct {
	ContainerID     string    `json:"container_id" bson:"container_id"`
	Timestamp       time.Time `json:"timestamp" bson:"timestamp"`
	InvocationCount int64     `json:"invocation_count" bson:"invocation_count"`
	SuccessCount    int64     `json:"success_count" bson:"success_count"`
	ErrorCount      int64     `json:"error_count" bson:"error_count"`
	P50LatencyMs    float64   `json:"p50_latency_ms" bson:"p50_latency_ms"`
	P95LatencyMs    float64   `json:"p95_latency_ms" bson:"p95_latency_ms"`
	P99LatencyMs    float64   `json:"p99_latency_ms" bson:"p99_latency_ms"`
	MemoryBytes     int64     `json:"memory_bytes" bson:"memory_bytes"`
	CPUUsageUs      int64     `json:"cpu_usage_us" bson:"cpu_usage_us"`
}

// policy thresholds — all configurable via env vars
type policy struct {
	TargetConcurrency int     `json:"target_concurrency" bson:"target_concurrency"`
	ScaleUpUtilPct    float64 `json:"scale_up_util_pct" bson:"scale_up_util_pct"`
	ScaleUpP99Ms      float64 `json:"scale_up_p99_ms" bson:"scale_up_p99_ms"`
	ScaleUpErrPct     float64 `json:"scale_up_err_pct" bson:"scale_up_err_pct"`
	ScaleDownUtilPct  float64 `json:"scale_down_util_pct" bson:"scale_down_util_pct"`
	ScaleDownP99Ms    float64 `json:"scale_down_p99_ms" bson:"scale_down_p99_ms"`
	IdleMinutes       float64 `json:"idle_minutes" bson:"idle_minutes"`
}

func defaultPolicy() policy {
	return policy{
		TargetConcurrency: envInt("TARGET_CONCURRENCY", 1),
		ScaleUpUtilPct:    envFloat("SCALE_UP_UTIL_PCT", 80),
		ScaleUpP99Ms:      envFloat("SCALE_UP_P99_MS", 500),
		ScaleUpErrPct:     envFloat("SCALE_UP_ERR_PCT", 10),
		ScaleDownUtilPct:  envFloat("SCALE_DOWN_UTIL_PCT", 20),
		ScaleDownP99Ms:    envFloat("SCALE_DOWN_P99_MS", 100),
		IdleMinutes:       envFloat("IDLE_MINUTES", 2),
	}
}

// ScaleDecision records why a scaling action was taken.
type ScaleDecision struct {
	Time     time.Time `json:"time" bson:"time"`
	FuncName string    `json:"func_name" bson:"func_name"`
	Action   string    `json:"action" bson:"action"` // "scale-up" | "scale-down" | "none"
	Reason   string    `json:"reason" bson:"reason"`
	Busy     int       `json:"busy" bson:"busy"`
	Idle     int       `json:"idle" bson:"idle"`
	Desired  int       `json:"desired" bson:"desired"`
}

type ScaleStatus struct {
	FuncName     string                      `json:"func_name" bson:"func_name"`
	Busy         int                         `json:"busy" bson:"busy"`
	Idle         int                         `json:"idle" bson:"idle"`
	Queued       int                         `json:"queued" bson:"queued"`
	Total        int                         `json:"total" bson:"total"`
	MaxReplicas  int                         `json:"max_replicas" bson:"max_replicas"`
	MinReplicas  int                         `json:"min_replicas" bson:"min_replicas"`
	Policy       policy                      `json:"policy" bson:"policy"`
	LastDecision *ScaleDecision              `json:"last_decision,omitempty" bson:"last_decision,omitempty"`
	Metrics      map[string]ContainerMetrics `json:"metrics" bson:"metrics"`
}

type scaler struct {
	gatewayAddr string
	pol         policy
	mu          sync.RWMutex
	metrics     map[string]ContainerMetrics // containerID → latest
	decisions   map[string]*ScaleDecision   // funcName → last decision
	maxReplicas int
	minReplicas int
	store       ScaleStore
}

func newScaler(gatewayAddr string) *scaler {
	store, err := newScaleStoreFromEnv()
	if err != nil {
		log.Fatalf("[scaler] scale store: %v", err)
	}
	metrics, err := store.LoadLatestMetrics(context.Background())
	if err != nil {
		log.Fatalf("[scaler] load metrics: %v", err)
	}
	if metrics == nil {
		metrics = make(map[string]ContainerMetrics)
	}
	decisions, err := store.LoadLatestDecisions(context.Background())
	if err != nil {
		log.Fatalf("[scaler] load decisions: %v", err)
	}
	if decisions == nil {
		decisions = make(map[string]*ScaleDecision)
	}
	s := &scaler{
		gatewayAddr: gatewayAddr,
		pol:         defaultPolicy(),
		metrics:     metrics,
		decisions:   decisions,
		maxReplicas: envInt("MAX_REPLICAS", 5),
		minReplicas: envInt("MIN_REPLICAS", 0),
		store:       store,
	}
	go s.evaluateLoop()
	return s
}

// evaluateLoop runs every 5s and applies scale-up/down policy per function.
func (s *scaler) evaluateLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, name := range s.functionNames() {
			s.evaluate(name)
		}
	}
}

func (s *scaler) evaluate(funcName string) {
	busy, idle := s.stats(funcName)
	queued := s.queueStatus(funcName)
	total := busy + idle

	// aggregate metrics across all containers for this function
	p99, errPct := s.aggregateMetrics(funcName)

	util := 0.0
	if total > 0 {
		util = float64(busy) / float64(total) * 100
	}

	pol := s.pol
	desired := s.desiredReplicas(total, busy, queued, p99, errPct)
	var action, reason string

	switch {
	case desired > total:
		action = "scale-up"
		reason = fmt.Sprintf("desired=%d > total=%d (busy=%d queued=%d target=%d p99=%.1fms err=%.1f%%)", desired, total, busy, queued, pol.TargetConcurrency, p99, errPct)
	case desired < total && idle > 0:
		action = "scale-down"
		reason = fmt.Sprintf("desired=%d < total=%d (busy=%d idle=%d queued=%d util=%.0f%% p99=%.1fms)", desired, total, busy, idle, queued, util, p99)
	default:
		action = "none"
		reason = fmt.Sprintf("desired=%d total=%d queue=%d util=%.0f%% p99=%.1fms err=%.1f%%", desired, total, queued, util, p99, errPct)
	}

	d := &ScaleDecision{
		Time: time.Now(), FuncName: funcName,
		Action: action, Reason: reason,
		Busy: busy, Idle: idle, Desired: desired,
	}
	s.mu.Lock()
	s.decisions[funcName] = d
	s.mu.Unlock()

	status := s.status(funcName, busy, idle, queued, d)
	if err := s.store.SaveDecision(context.Background(), *d); err != nil {
		log.Printf("[scaler] save decision %s: %v", funcName, err)
	}
	if err := s.store.SaveStatus(context.Background(), status); err != nil {
		log.Printf("[scaler] save status %s: %v", funcName, err)
	}

	if action == "none" {
		return
	}
	log.Printf("[scaler] %s %s: %s", action, funcName, reason)

	switch action {
	case "scale-up":
		s.callGateway("POST", "/internal/scale-up/"+funcName, nil)
	case "scale-down":
		s.callGateway("POST", "/internal/scale-down/"+funcName, nil)
	}
}

func (s *scaler) desiredReplicas(total, busy, queued int, p99, errPct float64) int {
	pol := s.pol
	target := pol.TargetConcurrency
	if target <= 0 {
		target = 1
	}

	desired := ceilDiv(busy+queued, target)
	if total > 0 {
		util := float64(busy) / float64(total) * 100
		if (util >= pol.ScaleUpUtilPct || p99 > pol.ScaleUpP99Ms || errPct >= pol.ScaleUpErrPct) && desired <= total {
			desired = total + 1
		}
		if queued == 0 && util >= pol.ScaleDownUtilPct && desired < total {
			desired = total
		}
	}
	if desired < s.minReplicas {
		desired = s.minReplicas
	}
	if desired > s.maxReplicas {
		desired = s.maxReplicas
	}
	return desired
}

func ceilDiv(n, d int) int {
	if n <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

// aggregateMetrics returns the max p99 and overall error rate across runtime
// instances for this function whose metrics were reported in the last 30s.
func (s *scaler) aggregateMetrics(funcName string) (maxP99, errPct float64) {
	cutoff := time.Now().Add(-30 * time.Second)
	instanceIDs := s.containerIDs(funcName)
	if len(instanceIDs) == 0 {
		return 0, 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalInv, totalErr int64
	for _, id := range instanceIDs {
		m, ok := s.metrics[id]
		if !ok || m.Timestamp.Before(cutoff) {
			continue
		}
		if m.P99LatencyMs > maxP99 {
			maxP99 = m.P99LatencyMs
		}
		totalInv += m.InvocationCount
		totalErr += m.ErrorCount
	}
	if totalInv > 0 {
		errPct = float64(totalErr) / float64(totalInv) * 100
	}
	return
}

// --- gateway helpers ---

func (s *scaler) functionNames() []string {
	resp, err := http.Get("http://" + s.gatewayAddr + "/internal/functions")
	if err != nil {
		log.Printf("[scaler] functionNames: %v", err)
		return nil
	}
	defer resp.Body.Close()
	var names []string
	json.NewDecoder(resp.Body).Decode(&names)
	return names
}

func (s *scaler) stats(funcName string) (busy, idle int) {
	resp, err := http.Get("http://" + s.gatewayAddr + "/internal/stats/" + funcName)
	if err != nil {
		log.Printf("[scaler] stats %s: %v", funcName, err)
		return
	}
	defer resp.Body.Close()
	var result struct {
		Busy int `json:"busy"`
		Idle int `json:"idle"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Busy, result.Idle
}

func (s *scaler) containerIDs(funcName string) []string {
	resp, err := http.Get("http://" + s.gatewayAddr + "/internal/containers/" + funcName)
	if err != nil {
		log.Printf("[scaler] containers %s: %v", funcName, err)
		return nil
	}
	defer resp.Body.Close()
	var ids []string
	if err := json.NewDecoder(resp.Body).Decode(&ids); err != nil {
		log.Printf("[scaler] decode containers %s: %v", funcName, err)
		return nil
	}
	return ids
}

func (s *scaler) queueStatus(funcName string) int {
	resp, err := http.Get("http://" + s.gatewayAddr + "/internal/queue/" + funcName)
	if err != nil {
		log.Printf("[scaler] queue %s: %v", funcName, err)
		return 0
	}
	defer resp.Body.Close()
	var result struct {
		Queued int `json:"queued"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[scaler] decode queue %s: %v", funcName, err)
		return 0
	}
	return result.Queued
}

func (s *scaler) callGateway(method, path string, body io.Reader) {
	req, _ := http.NewRequest(method, "http://"+s.gatewayAddr+path, body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[scaler] %s %s: %v", method, path, err)
		return
	}
	resp.Body.Close()
}

func (s *scaler) status(funcName string, busy, idle, queued int, dec *ScaleDecision) ScaleStatus {
	instanceIDs := s.containerIDs(funcName)
	s.mu.RLock()
	snapshot := make(map[string]ContainerMetrics, len(instanceIDs))
	for _, id := range instanceIDs {
		if m, ok := s.metrics[id]; ok {
			snapshot[id] = m
		}
	}
	s.mu.RUnlock()

	return ScaleStatus{
		FuncName:     funcName,
		Busy:         busy,
		Idle:         idle,
		Queued:       queued,
		Total:        busy + idle,
		MaxReplicas:  s.maxReplicas,
		MinReplicas:  s.minReplicas,
		Policy:       s.pol,
		LastDecision: dec,
		Metrics:      snapshot,
	}
}

// --- HTTP handlers ---

func (s *scaler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	containerID := strings.TrimPrefix(r.URL.Path, "/metrics/")
	var m ContainerMetrics
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if m.ContainerID == "" {
		m.ContainerID = containerID
	}
	s.mu.Lock()
	s.metrics[m.ContainerID] = m
	s.mu.Unlock()
	if err := s.store.SaveMetrics(context.Background(), m); err != nil {
		log.Printf("[scaler] save metrics %s: %v", m.ContainerID, err)
	}
	log.Printf("[scaler] recv container=%s inv=%d p99=%.1fms err=%d mem=%dMB",
		m.ContainerID, m.InvocationCount, m.P99LatencyMs, m.ErrorCount, m.MemoryBytes>>20)
	w.WriteHeader(http.StatusNoContent)
}

func (s *scaler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	funcName := strings.TrimPrefix(r.URL.Path, "/scale/")
	busy, idle := s.stats(funcName)
	queued := s.queueStatus(funcName)

	s.mu.RLock()
	dec := s.decisions[funcName]
	s.mu.RUnlock()
	status := s.status(funcName, busy, idle, queued, dec)
	if err := s.store.SaveStatus(context.Background(), status); err != nil {
		log.Printf("[scaler] save status %s: %v", funcName, err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func main() {
	gatewayAddr := os.Getenv("GATEWAY_INTERNAL_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "localhost:8080"
	}
	listenAddr := os.Getenv("SCALER_LISTEN")
	if listenAddr == "" {
		listenAddr = ":9300"
	}

	s := newScaler(gatewayAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/", s.handleMetrics)
	mux.HandleFunc("/scale/", s.handleStatus)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	log.Printf("[scaler] listening on %s (gateway=%s)", listenAddr, gatewayAddr)
	log.Printf("[scaler] policy: target concurrency=%d | scale-up p99>%.0fms OR err>%.0f%% | scale-down queue=0 AND util<%.0f%% AND p99<%.0fms",
		s.pol.TargetConcurrency, s.pol.ScaleUpP99Ms, s.pol.ScaleUpErrPct,
		s.pol.ScaleDownUtilPct, s.pol.ScaleDownP99Ms)

	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
