package entrypoints

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"serverless/gateway/queue"
	"serverless/gateway/scheduler"
)

func registerAutoscalingRoutes(mux *http.ServeMux, cfg Config, sched *scheduler.Scheduler, qm *queue.Manager) {
	mux.HandleFunc("/containers/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(req.URL.Path, "/containers/")
		if !strings.HasSuffix(path, "/metrics") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		if cfg.ScalerAddr != "" {
			go forwardMetrics(cfg.ScalerAddr, path, body)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/internal/stats/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/stats/")
		busy, idle := sched.Stats(name)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"busy": busy, "idle": idle})
	})

	mux.HandleFunc("/internal/containers/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/containers/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.ContainerIDs(name))
	})

	mux.HandleFunc("/internal/instances/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/instances/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.Instances(name))
	})

	mux.HandleFunc("/internal/queue/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/queue/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(qm.Status(name))
	})

	mux.HandleFunc("/internal/functions", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.FunctionNames())
	})
}

func forwardMetrics(scalerAddr, path string, body []byte) {
	url := "http://" + scalerAddr + "/metrics/" + strings.TrimSuffix(path, "/metrics")
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[gateway] forward metrics to scaler: %v", err)
		return
	}
	resp.Body.Close()
}
