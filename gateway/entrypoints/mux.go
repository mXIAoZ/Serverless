package entrypoints

import (
	"net/http"

	"serverless/gateway/queue"
	"serverless/gateway/scheduler"
)

type Config struct {
	ScalerAddr    string
	LogdaemonAddr string
}

type Dependencies struct {
	Scheduler *scheduler.Scheduler
	Queue     *queue.Manager
}

func NewMux(cfg Config, deps Dependencies) *http.ServeMux {
	mux := http.NewServeMux()
	registerFunctionRoutes(mux, deps.Scheduler)
	registerTrafficRoutes(mux, cfg, deps.Queue)
	registerAutoscalingRoutes(mux, cfg, deps.Scheduler, deps.Queue)
	registerColdStartRoutes(mux, deps.Scheduler)
	return mux
}
