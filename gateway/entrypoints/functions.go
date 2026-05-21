package entrypoints

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"serverless/gateway/scheduler"
)

func registerFunctionRoutes(mux *http.ServeMux, sched *scheduler.Scheduler) {
	mux.HandleFunc("/functions/", func(w http.ResponseWriter, req *http.Request) {
		path := strings.TrimPrefix(req.URL.Path, "/functions/")
		if path == "" {
			http.Error(w, "missing function name", http.StatusBadRequest)
			return
		}

		if req.Method == http.MethodPut && strings.HasSuffix(path, "/code") {
			name := strings.TrimSuffix(path, "/code")
			data, err := io.ReadAll(io.LimitReader(req.Body, 50<<20))
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			if err := sched.UploadCode(name, data); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "uploaded", "name": name})
			return
		}

		name := path
		switch req.Method {
		case http.MethodPost:
			var cfg scheduler.FunctionConfig
			if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			cfg.Name = name
			if err := sched.Register(cfg); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "registered", "name": name})

		case http.MethodDelete:
			sched.Deregister(name)
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
