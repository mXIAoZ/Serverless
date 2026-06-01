package entrypoints

import (
	"net/http"

	"serverless/gateway/queue"
	"serverless/gateway/scheduler"
)

type Config struct {
	ScalerAddr       string
	LogdaemonAddr    string
	InternalAPIToken string
}

type Dependencies struct {
	Scheduler *scheduler.Scheduler
	Queue     *queue.Manager
}

func NewPublicMux(cfg Config, deps Dependencies) *http.ServeMux {
	mux := http.NewServeMux()
	registerFunctionRoutes(mux, deps.Scheduler)
	registerTrafficRoutes(mux, cfg, deps.Queue)
	return mux
}

func NewInternalMux(cfg Config, deps Dependencies) *http.ServeMux {
	mux := http.NewServeMux()
	registerMetricsRoutes(mux, cfg)
	registerLeaseRoutes(mux, deps.Scheduler)
	registerInternalAutoscalingRoutes(mux, deps.Scheduler, deps.Queue)
	registerColdStartRoutes(mux, deps.Scheduler)
	if cfg.InternalAPIToken == "" {
		return mux
	}
	return withInternalAuth(mux, cfg.InternalAPIToken)
}

func NewMux(cfg Config, deps Dependencies) *http.ServeMux {
	return NewPublicMux(cfg, deps)
}

func withInternalAuth(next http.Handler, token string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, req)
	}))
	return mux
}
