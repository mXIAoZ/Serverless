package entrypoints

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"serverless/gateway/queue"
	"serverless/gateway/scheduler"
)

func TestPublicMuxDoesNotExposeInternalRoutes(t *testing.T) {
	deps := testMuxDependencies()
	mux := NewPublicMux(Config{}, deps)

	req := httptest.NewRequest(http.MethodGet, "/internal/functions", nil)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.Code)
	}
}

func TestInternalMuxRequiresTokenWhenConfigured(t *testing.T) {
	deps := testMuxDependencies()
	mux := NewInternalMux(Config{InternalAPIToken: "secret"}, deps)

	req := httptest.NewRequest(http.MethodGet, "/internal/functions", nil)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/internal/functions", nil)
	req.Header.Set("Authorization", "Bearer secret")
	res = httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestPublicMuxDoesNotExposeMetricsIngestion(t *testing.T) {
	deps := testMuxDependencies()
	mux := NewPublicMux(Config{}, deps)

	req := httptest.NewRequest(http.MethodPost, "/containers/inst-1/metrics", strings.NewReader(`{}`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.Code)
	}
}

func TestInternalMuxProtectsMetricsIngestionWhenTokenConfigured(t *testing.T) {
	deps := testMuxDependencies()
	mux := NewInternalMux(Config{InternalAPIToken: "secret"}, deps)

	req := httptest.NewRequest(http.MethodPost, "/containers/inst-1/metrics", strings.NewReader(`{}`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/containers/inst-1/metrics", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer secret")
	res = httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d body=%s, want 204", res.Code, res.Body.String())
	}
}

func TestInternalMuxRejectsOversizedMetricsBody(t *testing.T) {
	deps := testMuxDependencies()
	mux := NewInternalMux(Config{}, deps)

	req := httptest.NewRequest(http.MethodPost, "/containers/inst-1/metrics", strings.NewReader(strings.Repeat("x", metricsMaxBodyBytes+1)))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", res.Code)
	}
}

func TestInternalMuxReturnsMQTriggers(t *testing.T) {
	deps := testMuxDependencies()
	if err := deps.Scheduler.Register(scheduler.FunctionConfig{Name: "orders", Triggers: []scheduler.TriggerConfig{{
		ID:       "orders-trigger",
		Type:     "mq",
		Enabled:  true,
		MQID:     "rabbitmq-main",
		Queue:    "orders",
		Prefetch: 2,
	}}}); err != nil {
		t.Fatal(err)
	}
	mux := NewInternalMux(Config{}, deps)

	req := httptest.NewRequest(http.MethodGet, "/internal/triggers", nil)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
	if body := res.Body.String(); !strings.Contains(body, "orders-trigger") || !strings.Contains(body, "orders") {
		t.Fatalf("unexpected body: %s", body)
	}
}

func testMuxDependencies() Dependencies {
	s := scheduler.NewForTesting(fakeLeaseBackend{})
	return Dependencies{Scheduler: s, Queue: queue.New(nil)}
}
