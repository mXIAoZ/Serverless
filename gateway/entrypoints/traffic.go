package entrypoints

import (
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"serverless/gateway/queue"
)

func registerTrafficRoutes(mux *http.ServeMux, cfg Config, qm *queue.Manager) {
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
		proxyLogs(cfg.LogdaemonAddr, req.URL.RequestURI(), w, req)
	})

	mux.HandleFunc("/queues/", qm.StatusHandler)
}

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
