package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type invokeResult struct {
	statusCode int
	body       json.RawMessage
}

type invocation struct {
	id        string
	payload   json.RawMessage
	eventType string
	deadline  time.Time
	result    chan invokeResult
}

var (
	mu       sync.Mutex
	queue    []*invocation
	inflight sync.Map
	notify   = make(chan struct{}, 1)
)

func maxRequestBytes() int64 {
	return int64(envInt("RUNTIME_MAX_REQUEST_BYTES", 1<<20))
}

func maxQueueSize() int {
	max := envInt("RUNTIME_MAX_QUEUE", 128)
	if max <= 0 {
		return 128
	}
	return max
}

func clampTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 30 * time.Second
	}
	maxSeconds := envInt("MAX_FUNCTION_TIMEOUT_SECONDS", 300)
	if maxSeconds <= 0 {
		maxSeconds = 300
	}
	maxTimeout := time.Duration(maxSeconds) * time.Second
	if timeout > maxTimeout {
		return maxTimeout
	}
	return timeout
}

func envInt(key string, def int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return def
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/invoke", handleInvoke)
	mux.HandleFunc("/events", handleEvents)
	mux.HandleFunc("/runtime/invocation/next", handleNext)
	mux.HandleFunc("/runtime/invocation/", handleResponse)

	// 启动用户函数进程
	go startFunction()

	log.Println("runtime listening on :9000")
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Fatal(err)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func handleInvoke(w http.ResponseWriter, r *http.Request) {
	handleInvocation(w, r, "http")
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	handleInvocation(w, r, "mq")
}

func handleInvocation(w http.ResponseWriter, r *http.Request, eventType string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload json.RawMessage
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes())
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if eventType == "mq" {
			http.Error(w, "invalid event body", http.StatusBadRequest)
			return
		}
		payload = json.RawMessage(`{}`)
	}

	timeout := 30 * time.Second
	if t := r.Header.Get("X-Function-Timeout"); t != "" {
		if d, err := time.ParseDuration(t + "s"); err == nil {
			timeout = clampTimeout(d)
		}
	}

	inv := &invocation{
		id:        uuid.NewString(),
		payload:   payload,
		eventType: eventType,
		deadline:  time.Now().Add(timeout),
		result:    make(chan invokeResult, 1),
	}

	mu.Lock()
	if len(queue) >= maxQueueSize() {
		mu.Unlock()
		http.Error(w, "runtime queue full", http.StatusServiceUnavailable)
		return
	}
	inflight.Store(inv.id, inv.result)
	queue = append(queue, inv)
	mu.Unlock()

	select {
	case notify <- struct{}{}:
	default:
	}

	log.Printf("[%s] queued %s", eventType, inv.id)

	select {
	case res := <-inv.result:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"statusCode": res.statusCode,
			"body":       res.body,
		})
	case <-time.After(timeout):
		inflight.Delete(inv.id)
		removeQueuedInvocation(inv.id)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{"error": "function timeout"})
	}
}

func removeQueuedInvocation(id string) {
	mu.Lock()
	defer mu.Unlock()
	for i, inv := range queue {
		if inv.id == id {
			queue = append(queue[:i], queue[i+1:]...)
			return
		}
	}
}

func handleNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	for {
		mu.Lock()
		if len(queue) > 0 {
			inv := queue[0]
			queue = queue[1:]
			if time.Now().After(inv.deadline) {
				mu.Unlock()
				inflight.Delete(inv.id)
				continue
			}
			mu.Unlock()

			w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.id)
			w.Header().Set("Lambda-Runtime-Deadline-Ms",
				fmt.Sprintf("%d", inv.deadline.UnixMilli()))
			w.Header().Set("Lambda-Runtime-Event-Type", inv.eventType)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(inv.payload)
			log.Printf("[runtime-api] dispatched %s", inv.id)
			return
		}
		mu.Unlock()

		select {
		case <-notify:
		case <-time.After(60 * time.Second):
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func handleResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/runtime/invocation/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	requestID, action := parts[0], parts[1]
	if action != "response" && action != "error" {
		http.Error(w, "invalid response action", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes())
	body, err := io.ReadAll(r.Body)
	if err != nil || !json.Valid(body) {
		http.Error(w, "invalid response body", http.StatusBadRequest)
		return
	}

	statusCode := http.StatusOK
	if action == "error" {
		statusCode = http.StatusInternalServerError
	}

	val, ok := inflight.LoadAndDelete(requestID)
	if !ok {
		http.Error(w, "request id not found", http.StatusNotFound)
		return
	}

	val.(chan invokeResult) <- invokeResult{statusCode: statusCode, body: body}
	log.Printf("[runtime-api] %s for %s", action, requestID)
	w.WriteHeader(http.StatusAccepted)
}

// startFunction starts the selected language bootstrap and restarts it after exit.
func startFunction() {
	handler := os.Getenv("FUNCTION_HANDLER")
	runtime := os.Getenv("FUNCTION_RUNTIME")
	funcDir := os.Getenv("FUNCTION_DIR")

	if handler == "" {
		handler = "handler.handler"
	}
	if runtime == "" {
		runtime = "python3"
	}
	if funcDir == "" {
		funcDir = "/function"
	}

	for {
		bootstrapCmd, err := bootstrapCommand(runtime)
		if err != nil {
			log.Printf("[runtime] %v — retrying in 1s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("[runtime] starting function: %s %s (handler=%s, dir=%s)",
			runtime, strings.Join(bootstrapCmd, " "), handler, funcDir)

		cmd := exec.Command(bootstrapCmd[0], bootstrapCmd[1:]...)
		cmd.Dir = funcDir
		cmd.Env = append(os.Environ(),
			"RUNTIME_API=http://localhost:9000",
			"FUNCTION_HANDLER="+handler,
			"FUNCTION_DIR="+funcDir,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("[runtime] function process exited: %v — restarting in 1s", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func bootstrapCommand(runtime string) ([]string, error) {
	switch runtime {
	case "", "python3":
		return []string{"python3", "/runtime/bootstrap/python3_bootstrap.py"}, nil
	case "go":
		return []string{"/runtime/bootstrap/go-bootstrap"}, nil
	case "nodejs":
		return []string{"node", "/runtime/bootstrap/nodejs_bootstrap.js"}, nil
	case "java":
		return []string{"java", "-cp", "/runtime/bootstrap/java-bootstrap.jar", "JavaBootstrap"}, nil
	default:
		return nil, fmt.Errorf("unsupported runtime %q", runtime)
	}
}
