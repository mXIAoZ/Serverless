package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleEventsRejectsInvalidJSON(t *testing.T) {
	resetRuntimeState()
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader("not json"))
	res := httptest.NewRecorder()
	handleEvents(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", res.Code)
	}
}

func TestHandleInvocationRejectsOversizedHTTPBody(t *testing.T) {
	resetRuntimeState()
	t.Setenv("RUNTIME_MAX_REQUEST_BYTES", "4")
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"too":"large"}`))
	res := httptest.NewRecorder()
	handleInvoke(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", res.Code)
	}
}

func TestHandleEventsRejectsOversizedBody(t *testing.T) {
	resetRuntimeState()
	t.Setenv("RUNTIME_MAX_REQUEST_BYTES", "4")
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"too":"large"}`))
	res := httptest.NewRecorder()
	handleEvents(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", res.Code)
	}
}

func TestHandleInvocationRejectsFullQueue(t *testing.T) {
	resetRuntimeState()
	t.Setenv("RUNTIME_MAX_QUEUE", "1")
	queue = append(queue, &invocation{id: "queued"})
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"ok":true}`))
	res := httptest.NewRecorder()
	handleEvents(res, req)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.Code)
	}
}

func resetRuntimeState() {
	mu.Lock()
	queue = nil
	mu.Unlock()
	inflight.Range(func(key, _ any) bool {
		inflight.Delete(key)
		return true
	})
}

func TestHandleInvocationRegistersInflightBeforeDispatch(t *testing.T) {
	resetRuntimeState()
	defer resetRuntimeState()

	invokeDone := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"ok":true}`))
		res := httptest.NewRecorder()
		handleEvents(res, req)
		invokeDone <- res.Code
	}()

	var requestID string
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		req := httptest.NewRequest(http.MethodGet, "/runtime/invocation/next", nil)
		res := httptest.NewRecorder()
		handleNext(res, req)
		if res.Code == http.StatusOK {
			requestID = res.Header().Get("Lambda-Runtime-Aws-Request-Id")
			break
		}
	}
	if requestID == "" {
		t.Fatal("runtime invocation was not dispatched")
	}

	body, _ := json.Marshal(map[string]bool{"ok": true})
	res := httptest.NewRecorder()
	handleResponse(res, httptest.NewRequest(http.MethodPost, "/runtime/invocation/"+requestID+"/response", strings.NewReader(string(body))))
	if res.Code != http.StatusAccepted {
		t.Fatalf("response status = %d, want 202", res.Code)
	}

	select {
	case code := <-invokeDone:
		if code != http.StatusOK {
			t.Fatalf("invoke status = %d, want 200", code)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for invocation response")
	}
}

func TestHandleInvocationTimeoutRemovesQueuedRequest(t *testing.T) {
	resetRuntimeState()
	defer resetRuntimeState()

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"ok":true}`))
	req.Header.Set("X-Function-Timeout", "1")
	res := httptest.NewRecorder()
	handleEvents(res, req)
	if res.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", res.Code)
	}
	mu.Lock()
	queued := len(queue)
	mu.Unlock()
	if queued != 0 {
		t.Fatalf("queued = %d, want 0", queued)
	}
}
