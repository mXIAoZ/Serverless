package queue

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"serverless/gateway/router"
)

type Manager struct {
	router       *router.Router
	maxInflight  int
	maxQueue     int
	queueTimeout time.Duration
	mu           sync.Mutex
	funcs        map[string]*functionQueue
}

type functionQueue struct {
	sem    chan struct{}
	mu     sync.Mutex
	queued int
}

type Status struct {
	Function    string `json:"function"`
	InFlight    int    `json:"in_flight"`
	Queued      int    `json:"queued"`
	MaxInflight int    `json:"max_inflight"`
	MaxQueue    int    `json:"max_queue"`
}

func New(r *router.Router) *Manager {
	return &Manager{
		router:       r,
		maxInflight:  envInt("MAX_INFLIGHT_PER_FUNCTION", 5),
		maxQueue:     envInt("MAX_QUEUE_PER_FUNCTION", 100),
		queueTimeout: time.Duration(envInt("QUEUE_TIMEOUT_MS", 30000)) * time.Millisecond,
		funcs:        make(map[string]*functionQueue),
	}
}

func (m *Manager) Invoke(w http.ResponseWriter, req *http.Request, name string) {
	fq := m.get(name)

	select {
	case fq.sem <- struct{}{}:
		defer func() { <-fq.sem }()
		m.router.Invoke(w, req, name)
		return
	default:
	}

	fq.mu.Lock()
	if fq.queued >= m.maxQueue {
		fq.mu.Unlock()
		http.Error(w, "queue full", http.StatusTooManyRequests)
		return
	}
	fq.queued++
	fq.mu.Unlock()

	timer := time.NewTimer(m.queueTimeout)
	defer timer.Stop()

	select {
	case fq.sem <- struct{}{}:
		fq.mu.Lock()
		fq.queued--
		fq.mu.Unlock()
		defer func() { <-fq.sem }()
		m.router.Invoke(w, req, name)
	case <-timer.C:
		fq.mu.Lock()
		fq.queued--
		fq.mu.Unlock()
		http.Error(w, "queue timeout", http.StatusServiceUnavailable)
	case <-req.Context().Done():
		fq.mu.Lock()
		fq.queued--
		fq.mu.Unlock()
	}
}

func (m *Manager) StatusHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(req.URL.Path, "/queues/")
	if name == "" {
		http.Error(w, "missing function name", http.StatusBadRequest)
		return
	}

	status := m.Status(name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (m *Manager) Status(name string) Status {
	fq := m.get(name)
	fq.mu.Lock()
	queued := fq.queued
	fq.mu.Unlock()

	return Status{
		Function:    name,
		InFlight:    len(fq.sem),
		Queued:      queued,
		MaxInflight: m.maxInflight,
		MaxQueue:    m.maxQueue,
	}
}

func (m *Manager) get(name string) *functionQueue {
	m.mu.Lock()
	defer m.mu.Unlock()

	if fq, ok := m.funcs[name]; ok {
		return fq
	}
	fq := &functionQueue{sem: make(chan struct{}, m.maxInflight)}
	m.funcs[name] = fq
	return fq
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
