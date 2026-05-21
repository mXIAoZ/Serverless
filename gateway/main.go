package main

import (
	"log"
	"net/http"
	"os"

	"serverless/gateway/entrypoints"
	"serverless/gateway/queue"
	"serverless/gateway/router"
	"serverless/gateway/scheduler"
)

func main() {
	sched := scheduler.New()
	r := router.New(sched)
	qm := queue.New(r)

	logdaemonAddr := os.Getenv("LOGDAEMON_ADDR")
	if logdaemonAddr == "" {
		logdaemonAddr = "localhost:9200"
	}

	mux := entrypoints.NewMux(entrypoints.Config{
		ScalerAddr:    os.Getenv("SCALER_ADDR"),
		LogdaemonAddr: logdaemonAddr,
	}, entrypoints.Dependencies{
		Scheduler: sched,
		Queue:     qm,
	})

	log.Println("gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
