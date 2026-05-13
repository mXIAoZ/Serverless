package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	inflight sync.Map // requestID → chan invokeResult
	notify   = make(chan struct{}, 1)
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/invoke", handleInvoke)
	mux.HandleFunc("/runtime/invocation/next", handleNext)
	mux.HandleFunc("/runtime/invocation/", handleResponse)

	log.Println("mock sandbox listening on :9000")
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Fatal(err)
	}
}

// handleInvoke 接收 gateway 请求，入队并等待 runtime 处理完成
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

	select {
	case res := <-inv.result:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"statusCode": res.statusCode,
			"body":       res.body,
		})
	case <-time.After(30 * time.Second):
		inflight.Delete(inv.id)
		http.Error(w, `{"error":"function timeout"}`, http.StatusGatewayTimeout)
	}
}

// handleNext Runtime API 长轮询，阻塞直到有任务
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

// handleResponse 接收 runtime 返回的结果
// POST /runtime/invocation/{id}/response
// POST /runtime/invocation/{id}/error
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

	var body json.RawMessage
	json.NewDecoder(r.Body).Decode(&body)

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
