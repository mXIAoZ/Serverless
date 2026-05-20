package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"serverless/gateway/scheduler"
)

type InvokeResponse struct {
	StatusCode int             `json:"statusCode"`
	Body       json.RawMessage `json:"body"`
}

type Router struct {
	sched *scheduler.Scheduler
}

func New(sched *scheduler.Scheduler) *Router {
	return &Router{sched: sched}
}

func (r *Router) Invoke(w http.ResponseWriter, req *http.Request, name string) {
	cfg, ok := r.sched.Get(name)
	if !ok {
		http.Error(w, fmt.Sprintf("function %q not found", name), http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// 获取容器（复用 idle 或冷启动）
	c, err := r.sched.Acquire(name)
	if err != nil {
		log.Printf("[router] acquire failed for %q: %v", name, err)
		http.Error(w, "failed to acquire function instance", http.StatusServiceUnavailable)
		return
	}
	defer r.sched.Release(c)

	timeout := time.Duration(cfg.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()

	instanceURL := fmt.Sprintf("http://%s/invoke", c.Addr())
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, instanceURL, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build function request", http.StatusInternalServerError)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Function-Name", name)

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("[router] function invoke error for %q: %v", name, err)
		http.Error(w, "function instance unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var result InvokeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		http.Error(w, "invalid function response", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.StatusCode)
	w.Write(result.Body)
}
