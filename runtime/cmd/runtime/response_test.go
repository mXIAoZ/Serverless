package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleResponseRejectsInvalidJSON(t *testing.T) {
	id := "bad-json"
	result := make(chan invokeResult, 1)
	inflight.Store(id, result)
	defer inflight.Delete(id)

	req := httptest.NewRequest(http.MethodPost, "/runtime/invocation/"+id+"/response", strings.NewReader("not json"))
	w := httptest.NewRecorder()

	handleResponse(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	select {
	case <-result:
		t.Fatal("invalid JSON should not complete invocation")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestHandleResponseRejectsTrailingJSONData(t *testing.T) {
	id := "trailing-json"
	result := make(chan invokeResult, 1)
	inflight.Store(id, result)
	defer inflight.Delete(id)

	req := httptest.NewRequest(http.MethodPost, "/runtime/invocation/"+id+"/response", strings.NewReader(`{"ok":true} trailing`))
	w := httptest.NewRecorder()

	handleResponse(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	select {
	case <-result:
		t.Fatal("trailing JSON data should not complete invocation")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestHandleResponseAcceptsJSONError(t *testing.T) {
	id := "json-error"
	result := make(chan invokeResult, 1)
	inflight.Store(id, result)
	defer inflight.Delete(id)

	req := httptest.NewRequest(http.MethodPost, "/runtime/invocation/"+id+"/error", strings.NewReader(`{"errorType":"Error","errorMessage":"boom"}`))
	w := httptest.NewRecorder()

	handleResponse(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
	select {
	case got := <-result:
		if got.statusCode != http.StatusInternalServerError {
			t.Fatalf("statusCode = %d, want %d", got.statusCode, http.StatusInternalServerError)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for invocation result")
	}
}
