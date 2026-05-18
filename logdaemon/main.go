package main

import (
	"context"
	"log"
	"os"
)

const (
	ringSize   = 1000
	logDir     = "/tmp/faas-logs"
	dockerSock = "/var/run/docker.sock"
	listenAddr = ":9200"
	labelKey   = "faas.function"
)

func main() {
	d := newDaemon()
	ctx := context.Background()

	mode := os.Getenv("LOGDAEMON_MODE")
	if mode == "" {
		mode = defaultLogdaemonMode()
	}

	switch mode {
	case "docker":
		go d.collectExisting(ctx)
		go d.watchEvents(ctx)
	case "collector":
		go d.runCollector(ctx)
	case "proxy":
		d.proxy = newProxyClient()
	default:
		log.Fatalf("[daemon] unknown mode %q", mode)
	}

	d.serveHTTP(mode)
}

func defaultLogdaemonMode() string {
	switch os.Getenv("FAAS_BACKEND") {
	case "k8s", "kubernetes":
		return "proxy"
	default:
		return "docker"
	}
}
