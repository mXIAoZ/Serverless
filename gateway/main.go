package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"serverless/gateway/queue"
	"serverless/gateway/router"
	"serverless/gateway/scheduler"
)

func main() {
	sched := scheduler.New()
	r := router.New(sched)
	qm := queue.New(r)

	scalerAddr := os.Getenv("SCALER_ADDR") // e.g. "localhost:9300"
	logdaemonAddr := os.Getenv("LOGDAEMON_ADDR")
	if logdaemonAddr == "" {
		logdaemonAddr = "localhost:9200"
	}

	mux := http.NewServeMux()

	// /functions/{name}        POST   — 注册函数
	// /functions/{name}        DELETE — 注销函数
	// /functions/{name}/code   PUT    — 上传 zip 代码
	mux.HandleFunc("/functions/", func(w http.ResponseWriter, req *http.Request) {
		path := strings.TrimPrefix(req.URL.Path, "/functions/")
		if path == "" {
			http.Error(w, "missing function name", http.StatusBadRequest)
			return
		}

		if req.Method == http.MethodPut && strings.HasSuffix(path, "/code") {
			name := strings.TrimSuffix(path, "/code")
			data, err := io.ReadAll(io.LimitReader(req.Body, 50<<20))
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			if err := sched.UploadCode(name, data); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "uploaded", "name": name})
			return
		}

		name := path
		switch req.Method {
		case http.MethodPost:
			var cfg scheduler.FunctionConfig
			if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			cfg.Name = name
			if err := sched.Register(cfg); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "registered", "name": name})

		case http.MethodDelete:
			sched.Deregister(name)
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// POST /invoke/{name}
	mux.HandleFunc("/invoke/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/invoke/")
		if name == "" {
			http.Error(w, "missing function name", http.StatusBadRequest)
			return
		}
		qm.Invoke(w, req, name)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// GET /logs/{name} and /logs/{name}/stream — user-facing log API proxied through gateway.
	mux.HandleFunc("/logs/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(req.URL.Path, "/logs/")
		if path == "" || strings.HasPrefix(path, "/") {
			http.Error(w, "missing function name", http.StatusBadRequest)
			return
		}
		proxyLogs(logdaemonAddr, req.URL.RequestURI(), w, req)
	})

	mux.HandleFunc("/queues/", qm.StatusHandler)

	// POST /containers/{id}/metrics — 接收 agent 上报，转发给 scaler
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

		if scalerAddr != "" {
			go forwardMetrics(scalerAddr, path, body)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// --- Internal API (scaler → gateway) ---
	// Only intended for localhost access; no auth needed in this learning project.

	// GET /internal/stats/{funcName} — 返回 busy/idle 计数
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

	// GET /internal/containers/{funcName} — 返回该函数的 runtime 实例 ID 列表
	mux.HandleFunc("/internal/containers/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/containers/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.ContainerIDs(name))
	})

	// GET /internal/instances/{funcName} — 返回该函数实例与 node 映射
	mux.HandleFunc("/internal/instances/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/instances/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.Instances(name))
	})

	// GET /internal/queue/{funcName} — 返回 gateway queue 状态
	mux.HandleFunc("/internal/queue/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/queue/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(qm.Status(name))
	})

	// GET /internal/functions — 返回所有已注册函数名
	mux.HandleFunc("/internal/functions", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sched.FunctionNames())
	})

	// POST /internal/scale-up/{funcName} — 预热一个容器
	mux.HandleFunc("/internal/scale-up/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/scale-up/")
		sched.ColdStartOne(name)
		w.WriteHeader(http.StatusAccepted)
	})

	// POST /internal/scale-down/{funcName} — 移除最旧的 idle 容器
	mux.HandleFunc("/internal/scale-down/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/scale-down/")
		removed := sched.RemoveIdle(name)
		if removed {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	log.Println("gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

// proxyLogs forwards user-facing log reads to the internal logdaemon service.
func proxyLogs(logdaemonAddr, requestURI string, w http.ResponseWriter, req *http.Request) {
	url := "http://" + logdaemonAddr + requestURI
	logReq, err := http.NewRequestWithContext(req.Context(), http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, "failed to build log request", http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}
	if strings.HasSuffix(req.URL.Path, "/stream") {
		client = http.DefaultClient
	}

	resp, err := client.Do(logReq)
	if err != nil {
		log.Printf("[gateway] proxy logs: %v", err)
		http.Error(w, "logs unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		copyStreamingResponse(w, resp.Body)
		return
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[gateway] copy logs response: %v", err)
	}
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func copyStreamingResponse(w http.ResponseWriter, body io.Reader) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		if _, err := io.Copy(w, body); err != nil {
			log.Printf("[gateway] copy log stream: %v", err)
		}
		return
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				log.Printf("[gateway] write log stream: %v", err)
				return
			}
			flusher.Flush()
		}
		if readErr != nil {
			if readErr != io.EOF {
				log.Printf("[gateway] read log stream: %v", readErr)
			}
			return
		}
	}
}

// forwardMetrics sends the raw metrics body to the scaler service.
func forwardMetrics(scalerAddr, path string, body []byte) {
	url := "http://" + scalerAddr + "/metrics/" + strings.TrimSuffix(path, "/metrics")
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[gateway] forward metrics to scaler: %v", err)
		return
	}
	resp.Body.Close()
}
