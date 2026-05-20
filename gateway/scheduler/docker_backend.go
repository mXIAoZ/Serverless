package scheduler

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"
)

type dockerBackend struct {
	client *dockerHTTPClient
}

func newDockerBackend() RuntimeBackend {
	return &dockerBackend{client: newDockerHTTPClient()}
}

func (b *dockerBackend) Name() string { return "docker" }

func (b *dockerBackend) Start(ctx context.Context, cfg FunctionConfig) (*RuntimeInstance, error) {
	addr := fmt.Sprintf("localhost:%d", cfg.Port)

	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "host.docker.internal:8080"
	}

	name := fmt.Sprintf("faas-%s-%d", dnsLabel(cfg.Name), cfg.Port)
	body := map[string]any{
		"Image": cfg.Image,
		"Env": []string{
			"FUNCTION_HANDLER=" + cfg.Handler,
			"FUNCTION_RUNTIME=" + cfg.Runtime,
			"GATEWAY_ADDR=" + gatewayAddr,
		},
		"Labels": map[string]string{"faas.function": cfg.Name},
		"HostConfig": map[string]any{
			"AutoRemove": true,
			"Memory":     int64(cfg.Memory) * 1024 * 1024,
			"PortBindings": map[string][]map[string]string{
				"9001/tcp": {{"HostIp": "127.0.0.1", "HostPort": fmt.Sprintf("%d", cfg.Port)}},
			},
		},
		"ExposedPorts": map[string]any{"9001/tcp": map[string]any{}},
	}
	if cfg.CodeDir != "" {
		body["HostConfig"].(map[string]any)["Binds"] = []string{cfg.CodeDir + ":/function"}
	}

	var created struct {
		ID string `json:"Id"`
	}
	log.Printf("[scheduler] docker cold start %s on port %d", cfg.Name, cfg.Port)
	path := "/containers/create?" + url.Values{"name": {name}}.Encode()
	if err := b.client.do(ctx, httpMethodPost, path, body, &created); err != nil {
		return nil, fmt.Errorf("docker create: %w", err)
	}
	if err := b.client.do(ctx, httpMethodPost, "/containers/"+created.ID+"/start", nil, nil); err != nil {
		_ = b.Stop(context.Background(), created.ID)
		return nil, fmt.Errorf("docker start: %w", err)
	}

	id := created.ID
	if len(id) > 12 {
		id = id[:12]
	}
	if err := waitReady(ctx, addr, 10*time.Second); err != nil {
		b.Stop(context.Background(), created.ID)
		return nil, fmt.Errorf("container not ready: %w", err)
	}

	return &RuntimeInstance{ID: id, Addr: addr, FuncName: cfg.Name}, nil
}

func (b *dockerBackend) Stop(ctx context.Context, id string) error {
	if err := b.client.do(ctx, httpMethodPost, "/containers/"+id+"/stop", nil, nil); err != nil {
		log.Printf("[scheduler] stop container %s: %v", id, err)
		return err
	}
	return nil
}

const httpMethodPost = "POST"
