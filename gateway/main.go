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

	cfg := entrypoints.Config{
		ScalerAddr:       os.Getenv("SCALER_ADDR"),
		LogdaemonAddr:    logdaemonAddr,
		InternalAPIToken: os.Getenv("INTERNAL_API_TOKEN"),
	}
	deps := entrypoints.Dependencies{Scheduler: sched, Queue: qm}

	publicListen := os.Getenv("GATEWAY_PUBLIC_LISTEN")
	if publicListen == "" {
		publicListen = ":8080"
	}
	internalListen := os.Getenv("GATEWAY_INTERNAL_LISTEN")
	if internalListen == "" {
		internalListen = "127.0.0.1:8081"
	}

	internalMux := entrypoints.NewInternalMux(cfg, deps)
	go func() {
		log.Printf("gateway internal listening on %s", internalListen)
		if err := http.ListenAndServe(internalListen, internalMux); err != nil {
			log.Fatal(err)
		}
	}()

	publicMux := entrypoints.NewPublicMux(cfg, deps)
	log.Printf("gateway public listening on %s", publicListen)
	if err := http.ListenAndServe(publicListen, publicMux); err != nil {
		log.Fatal(err)
	}
}
