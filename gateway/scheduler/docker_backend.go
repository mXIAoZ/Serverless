package scheduler

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

type dockerBackend struct{}

func newDockerBackend() RuntimeBackend {
	return &dockerBackend{}
}

func (b *dockerBackend) Name() string { return "docker" }

func (b *dockerBackend) Start(ctx context.Context, cfg FunctionConfig) (*RuntimeInstance, error) {
	addr := fmt.Sprintf("localhost:%d", cfg.Port)

	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "host.docker.internal:8080"
	}

	args := []string{
		"run", "-d", "--rm",
		"-p", fmt.Sprintf("%d:9001", cfg.Port),
		"-m", fmt.Sprintf("%dm", cfg.Memory),
		"-e", "FUNCTION_HANDLER=" + cfg.Handler,
		"-e", "GATEWAY_ADDR=" + gatewayAddr,
		"--label", "faas.function=" + cfg.Name,
		"--name", fmt.Sprintf("faas-%s-%d", cfg.Name, cfg.Port),
	}

	if cfg.CodeDir != "" {
		args = append(args, "-v", cfg.CodeDir+":/function")
	}

	args = append(args, cfg.Image)

	log.Printf("[scheduler] docker cold start %s on port %d", cfg.Name, cfg.Port)
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}

	id := strings.TrimSpace(string(out))[:12]
	if err := waitReady(ctx, addr, 10*time.Second); err != nil {
		b.Stop(context.Background(), id)
		return nil, fmt.Errorf("container not ready: %w", err)
	}

	return &RuntimeInstance{ID: id, Addr: addr, FuncName: cfg.Name}, nil
}

func (b *dockerBackend) Stop(ctx context.Context, id string) error {
	if err := exec.CommandContext(ctx, "docker", "stop", id).Run(); err != nil {
		log.Printf("[scheduler] stop container %s: %v", id, err)
		return err
	}
	return nil
}
