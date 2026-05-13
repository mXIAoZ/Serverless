package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

type k8sBackend struct {
	namespace string
	mu        sync.Mutex
	forwards  map[string]*exec.Cmd
}

func newK8sBackend() RuntimeBackend {
	ns := os.Getenv("K8S_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	return &k8sBackend{namespace: ns, forwards: make(map[string]*exec.Cmd)}
}

func (b *k8sBackend) Name() string { return "k8s" }

func (b *k8sBackend) Start(ctx context.Context, cfg FunctionConfig) (*RuntimeInstance, error) {
	podName := fmt.Sprintf("faas-%s-%d", dnsLabel(cfg.Name), cfg.Port)
	addr := fmt.Sprintf("localhost:%d", cfg.Port)

	if cfg.CodeDir != "" {
		if err := syncCodeToMinikube(ctx, cfg.CodeDir); err != nil {
			return nil, err
		}
	}

	manifest := b.podManifest(podName, cfg)
	apply := exec.CommandContext(ctx, "kubectl", "apply", "-n", b.namespace, "-f", "-")
	apply.Stdin = strings.NewReader(manifest)
	if out, err := apply.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("kubectl apply: %w\n%s", err, out)
	}

	wait := exec.CommandContext(ctx, "kubectl", "wait", "-n", b.namespace, "--for=condition=Ready", "pod/"+podName, "--timeout=30s")
	if out, err := wait.CombinedOutput(); err != nil {
		b.Stop(context.Background(), podName)
		return nil, fmt.Errorf("kubectl wait: %w\n%s", err, out)
	}

	pf := exec.CommandContext(context.Background(), "kubectl", "port-forward", "-n", b.namespace, "pod/"+podName, fmt.Sprintf("%d:9001", cfg.Port))
	var pfOut bytes.Buffer
	pf.Stdout = &pfOut
	pf.Stderr = &pfOut
	if err := pf.Start(); err != nil {
		b.Stop(context.Background(), podName)
		return nil, fmt.Errorf("kubectl port-forward: %w", err)
	}

	b.mu.Lock()
	b.forwards[podName] = pf
	b.mu.Unlock()

	log.Printf("[scheduler] k8s cold start %s pod=%s port=%d", cfg.Name, podName, cfg.Port)
	if err := waitReady(ctx, addr, 15*time.Second); err != nil {
		b.Stop(context.Background(), podName)
		return nil, fmt.Errorf("pod not ready through port-forward: %w\n%s", err, pfOut.String())
	}

	return &RuntimeInstance{ID: podName, Addr: addr, FuncName: cfg.Name}, nil
}

func (b *k8sBackend) Stop(ctx context.Context, id string) error {
	b.mu.Lock()
	pf := b.forwards[id]
	delete(b.forwards, id)
	b.mu.Unlock()

	if pf != nil && pf.Process != nil {
		_ = pf.Process.Kill()
		_, _ = pf.Process.Wait()
	}

	cmd := exec.CommandContext(ctx, "kubectl", "delete", "pod", id, "-n", b.namespace, "--ignore-not-found=true")
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("[scheduler] delete pod %s: %v\n%s", id, err, out)
		return err
	}
	return nil
}

func (b *k8sBackend) podManifest(podName string, cfg FunctionConfig) string {
	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "host.minikube.internal:8080"
	}

	mount := ""
	volume := ""
	if cfg.CodeDir != "" {
		mount = `
        volumeMounts:
        - name: function-code
          mountPath: /function`
		volume = fmt.Sprintf(`
      volumes:
      - name: function-code
        hostPath:
          path: %s
          type: Directory`, cfg.CodeDir)
	}

	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    faas.managed-by: local-faas
    faas.function: %s
    faas.instance: %s
spec:
  restartPolicy: Never
  containers:
  - name: runtime
    image: %s
    imagePullPolicy: IfNotPresent
    ports:
    - containerPort: 9001
    env:
    - name: FUNCTION_HANDLER
      value: %q
    - name: GATEWAY_ADDR
      value: %q
    - name: CONTAINER_ID
      value: %q
    resources:
      limits:
        memory: %dMi%s%s
`, podName, cfg.Name, podName, cfg.Image, cfg.Handler, gatewayAddr, podName, cfg.Memory, mount, volume)
}

func syncCodeToMinikube(ctx context.Context, codeDir string) error {
	mkdir := exec.CommandContext(ctx, "minikube", "ssh", "--", "sudo mkdir -p "+shellQuote(codeDir))
	if out, err := mkdir.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube mkdir: %w\n%s", err, out)
	}

	cmd := exec.CommandContext(ctx, "minikube", "cp", codeDir, codeDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube cp: %w\n%s", err, out)
	}
	return nil
}

var dnsLabelRE = regexp.MustCompile(`[^a-z0-9-]+`)

func dnsLabel(s string) string {
	s = strings.ToLower(s)
	s = dnsLabelRE.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		return "fn"
	}
	if len(s) > 40 {
		s = s[:40]
		s = strings.Trim(s, "-")
	}
	return s
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
