package entrypoints

import (
	"net/http"
	"strings"

	"serverless/gateway/scheduler"
)

func registerColdStartRoutes(mux *http.ServeMux, sched *scheduler.Scheduler) {
	mux.HandleFunc("/internal/scale-up/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/scale-up/")
		sched.ColdStartOne(name)
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("/internal/scale-down/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/internal/scale-down/")
		removed := sched.RemoveIdle(name)
		if removed {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})
}
