package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
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
	id      string
	payload json.RawMessage
	result  chan invokeResult
}

var (
	mu       sync.Mutex
	queue    []*invocation
	inflight sync.Map
	notify   = make(chan struct{}, 1)
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/invoke", handleInvoke)
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
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		payload = json.RawMessage(`{}`)
	}

	inv := &invocation{
		id:      uuid.NewString(),
		payload: payload,
		result:  make(chan invokeResult, 1),
	}
	inflight.Store(inv.id, inv.result)

	mu.Lock()
	queue = append(queue, inv)
	mu.Unlock()

	select {
	case notify <- struct{}{}:
	default:
	}

	log.Printf("[invoke] queued %s", inv.id)

	timeout := 30 * time.Second
	if t := r.Header.Get("X-Function-Timeout"); t != "" {
		if d, err := time.ParseDuration(t + "s"); err == nil {
			timeout = d
		}
	}

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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{"error": "function timeout"})
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
			mu.Unlock()

			w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.id)
			w.Header().Set("Lambda-Runtime-Deadline-Ms",
				fmt.Sprintf("%d", time.Now().Add(30*time.Second).UnixMilli()))
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
