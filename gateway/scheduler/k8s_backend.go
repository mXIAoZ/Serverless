package scheduler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

type k8sBackend struct {
	namespace  string
	client     kubernetes.Interface
	restConfig *rest.Config
	mu         sync.Mutex
	forwards   map[string]*podPortForward
}

type podPortForward struct {
	stopCh chan struct{}
	doneCh chan struct{}
	out    *strings.Builder
}

func newK8sBackend() RuntimeBackend {
	ns := os.Getenv("K8S_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	client, restConfig, err := newK8sClient()
	if err != nil {
		log.Fatalf("[scheduler] k8s client: %v", err)
	}
	return &k8sBackend{namespace: ns, client: client, restConfig: restConfig, forwards: make(map[string]*podPortForward)}
}

func newK8sClient() (kubernetes.Interface, *rest.Config, error) {
	cfg, err := newK8sRestConfig()
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	return client, cfg, nil
}

func newK8sRestConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		if home, err := os.UserHomeDir(); err == nil {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
	}
	if kubeconfig != "" {
		if cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig); err == nil {
			return cfg, nil
		}
	}
	return rest.InClusterConfig()
}

func (b *k8sBackend) Name() string { return "k8s" }

func (b *k8sBackend) Start(ctx context.Context, cfg FunctionConfig) (*RuntimeInstance, error) {
	podName := fmt.Sprintf("faas-%s-%d", dnsLabel(cfg.Name), cfg.Port)
	addr := fmt.Sprintf("localhost:%d", cfg.Port)

	if cfg.CodeDir != "" && cfg.CodeKey == "" {
		if err := syncCodeToMinikube(ctx, cfg.CodeDir); err != nil {
			return nil, err
		}
	}

	pod := b.podSpec(podName, cfg)
	if _, err := b.client.CoreV1().Pods(b.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create pod %s: %w", podName, err)
	}

	nodeName, err := b.waitPodReady(ctx, podName, 30*time.Second)
	if err != nil {
		b.Stop(context.Background(), podName)
		return nil, err
	}

	pf, err := b.startPortForward(podName, cfg.Port)
	if err != nil {
		b.Stop(context.Background(), podName)
		return nil, err
	}

	b.mu.Lock()
	b.forwards[podName] = pf
	b.mu.Unlock()

	log.Printf("[scheduler] k8s cold start %s pod=%s port=%d", cfg.Name, podName, cfg.Port)
	if err := waitReady(ctx, addr, 15*time.Second); err != nil {
		b.Stop(context.Background(), podName)
		return nil, fmt.Errorf("pod not ready through port-forward: %w\n%s", err, pf.out.String())
	}

	return &RuntimeInstance{ID: podName, Addr: addr, FuncName: cfg.Name, NodeName: nodeName}, nil
}

func (b *k8sBackend) Stop(ctx context.Context, id string) error {
	b.mu.Lock()
	pf := b.forwards[id]
	delete(b.forwards, id)
	b.mu.Unlock()

	if pf != nil {
		close(pf.stopCh)
		<-pf.doneCh
	}

	if err := b.deletePod(ctx, id); err != nil {
		log.Printf("[scheduler] delete pod %s: %v", id, err)
		return err
	}
	return nil
}

func (b *k8sBackend) deletePod(ctx context.Context, podName string) error {
	err := b.client.CoreV1().Pods(b.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (b *k8sBackend) startPortForward(podName string, port int) (*podPortForward, error) {
	req := b.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(b.namespace).
		Name(podName).
		SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(b.restConfig)
	if err != nil {
		return nil, err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())

	readyCh := make(chan struct{})
	pf := &podPortForward{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		out:    &strings.Builder{},
	}
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf("%d:9001", port)}, pf.stopCh, readyCh, pf.out, pf.out)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(pf.doneCh)
		if err := forwarder.ForwardPorts(); err != nil {
			_, _ = fmt.Fprintf(pf.out, "port-forward: %v", err)
		}
	}()

	select {
	case <-readyCh:
		return pf, nil
	case <-pf.doneCh:
		return nil, fmt.Errorf("port-forward exited before ready: %s", pf.out.String())
	case <-time.After(5 * time.Second):
		close(pf.stopCh)
		<-pf.doneCh
		return nil, fmt.Errorf("timeout starting port-forward: %s", pf.out.String())
	}
}

func (b *k8sBackend) waitPodReady(ctx context.Context, podName string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		pod, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("get pod %s: %w", podName, err)
		}
		if pod != nil {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					if pod.Spec.NodeName == "" {
						return "", fmt.Errorf("pod %s has no assigned node", podName)
					}
					return pod.Spec.NodeName, nil
				}
			}
			if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				return "", fmt.Errorf("pod %s finished before becoming ready: %s", podName, pod.Status.Phase)
			}
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timeout waiting for pod %s ready", podName)
		case <-ticker.C:
		}
	}
}

func (b *k8sBackend) podSpec(podName string, cfg FunctionConfig) *corev1.Pod {
	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "host.minikube.internal:8080"
	}
	gatewayInternalAddr := os.Getenv("GATEWAY_INTERNAL_ADDR")
	if gatewayInternalAddr == "" {
		gatewayInternalAddr = "host.minikube.internal:8081"
	}

	container := corev1.Container{
		Name:            "runtime",
		Image:           cfg.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{{
			ContainerPort: 9001,
		}},
		Env: []corev1.EnvVar{
			{Name: "FUNCTION_HANDLER", Value: cfg.Handler},
			{Name: "FUNCTION_RUNTIME", Value: cfg.Runtime},
			{Name: "GATEWAY_ADDR", Value: gatewayAddr},
			{Name: "GATEWAY_INTERNAL_ADDR", Value: gatewayInternalAddr},
			{Name: "INTERNAL_API_TOKEN", Value: os.Getenv("INTERNAL_API_TOKEN")},
			{Name: "CONTAINER_ID", Value: podName},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", cfg.Memory)),
			},
		},
	}

	var volumes []corev1.Volume
	var initContainers []corev1.Container
	if cfg.CodeKey != "" {
		container.VolumeMounts = []corev1.VolumeMount{{
			Name:      "function-code",
			MountPath: "/function",
		}}
		initContainers = []corev1.Container{b.codeInitContainer(cfg)}
		volumes = []corev1.Volume{{
			Name: "function-code",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}}
	} else if cfg.CodeDir != "" {
		hostPathType := corev1.HostPathDirectory
		container.VolumeMounts = []corev1.VolumeMount{{
			Name:      "function-code",
			MountPath: "/function",
		}}
		volumes = []corev1.Volume{{
			Name: "function-code",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: cfg.CodeDir,
					Type: &hostPathType,
				},
			},
		}}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			Labels: map[string]string{
				"faas.managed-by": "local-faas",
				"faas.function":   cfg.Name,
				"faas.instance":   podName,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: initContainers,
			Containers:     []corev1.Container{container},
			Volumes:        volumes,
		},
	}
}

func (b *k8sBackend) codeInitContainer(cfg FunctionConfig) corev1.Container {
	return corev1.Container{
		Name:            "code-loader",
		Image:           cfg.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"python3", "-c", fmt.Sprintf(`
import os
import urllib.request
import zipfile

root = '/function'
zip_path = '/tmp/function.zip'
urllib.request.urlretrieve(%q, zip_path)

root_real = os.path.realpath(root)
with zipfile.ZipFile(zip_path) as archive:
    for info in archive.infolist():
        target = os.path.realpath(os.path.join(root, info.filename))
        if target != root_real and not target.startswith(root_real + os.sep):
            raise Exception('zip entry escapes function directory: ' + info.filename)
        if info.is_dir():
            os.makedirs(target, exist_ok=True)
            continue
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with archive.open(info) as src, open(target, 'wb') as dst:
            dst.write(src.read())
        os.chmod(target, info.external_attr >> 16 or 0o644)
`, cfg.CodeURL)},
		VolumeMounts: []corev1.VolumeMount{{
			Name:      "function-code",
			MountPath: "/function",
		}},
	}
}

func syncCodeToMinikube(ctx context.Context, codeDir string) error {
	targetDir := filepath.Clean(codeDir)
	parentDir := filepath.Dir(targetDir)
	baseName := filepath.Base(targetDir)
	tarPath := filepath.Join(os.TempDir(), baseName+".tgz")

	tarCmd := exec.CommandContext(ctx, "tar", "-C", parentDir, "-czf", tarPath, baseName)
	if out, err := tarCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("tar function code: %w\n%s", err, out)
	}
	defer os.Remove(tarPath)

	mkdir := exec.CommandContext(ctx, "minikube", "ssh", "--", "sudo mkdir -p "+shellQuote(parentDir))
	if out, err := mkdir.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube mkdir: %w\n%s", err, out)
	}

	targetTar := filepath.Join(parentDir, baseName+".tgz")
	cp := exec.CommandContext(ctx, "minikube", "cp", tarPath, targetTar)
	if out, err := cp.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube cp: %w\n%s", err, out)
	}

	untarCmd := fmt.Sprintf("sudo rm -rf %s && sudo tar -C %s -xzf %s", shellQuote(targetDir), shellQuote(parentDir), shellQuote(targetTar))
	untar := exec.CommandContext(ctx, "minikube", "ssh", "--", untarCmd)
	if out, err := untar.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube untar: %w\n%s", err, out)
	}

	cleanup := exec.CommandContext(ctx, "minikube", "ssh", "--", "sudo rm -f "+shellQuote(targetTar))
	if out, err := cleanup.CombinedOutput(); err != nil {
		return fmt.Errorf("minikube cleanup: %w\n%s", err, out)
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
