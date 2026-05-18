package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

// FunctionConfig 函数注册信息
type FunctionConfig struct {
	Name    string `json:"name"`
	Image   string `json:"image"`
	Timeout int    `json:"timeout"`
	Memory  int    `json:"memory"`
	Handler string `json:"handler"` // e.g. "handler.handler"
	CodeDir string `json:"-"`       // 解压后的代码目录，由 UploadCode 设置
	Port    int    `json:"-"`
}

// containerState 容器状态
type containerState int

const (
	stateIdle     containerState = iota // 空闲，可复用
	stateBusy                           // 正在处理请求
	stateStarting                       // 冷启动中
)

// container 代表一个沙箱容器实例
type container struct {
	id       string
	addr     string // host:port，指向容器内 /invoke
	state    containerState
	lastUsed time.Time
	funcName string
	nodeName string
}

// Scheduler 管理函数注册表和容器池
type Scheduler struct {
	mu        sync.RWMutex
	functions map[string]FunctionConfig
	pool      map[string][]*container // funcName → 容器列表
	nextPort  int
	backend   RuntimeBackend
}

func New() *Scheduler {
	backend := newRuntimeBackend()
	s := &Scheduler{
		functions: make(map[string]FunctionConfig),
		pool:      make(map[string][]*container),
		nextPort:  9100,
		backend:   backend,
	}
	log.Printf("[scheduler] backend=%s", backend.Name())
	go s.reaper()
	return s
}

func newRuntimeBackend() RuntimeBackend {
	switch os.Getenv("FAAS_BACKEND") {
	case "k8s", "kubernetes":
		return newK8sBackend()
	default:
		return newDockerBackend()
	}
}

func (s *Scheduler) Register(cfg FunctionConfig) error {
	if cfg.Image == "" {
		cfg.Image = "faas-runtime:latest"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30
	}
	if cfg.Memory == 0 {
		cfg.Memory = 128
	}
	if cfg.Handler == "" {
		cfg.Handler = "handler.handler"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.functions[cfg.Name]; exists {
		return errors.New("function already registered")
	}
	s.functions[cfg.Name] = cfg
	return nil
}

func (s *Scheduler) Deregister(name string) {
	s.mu.Lock()
	delete(s.functions, name)
	containers := s.pool[name]
	delete(s.pool, name)
	s.mu.Unlock()

	for _, c := range containers {
		go s.stop(c.id)
	}
}

// UploadCode 将 zip 内容解压到 /tmp/faas/{name}/，并更新函数配置
func (s *Scheduler) UploadCode(name string, zipData []byte) error {
	s.mu.Lock()
	cfg, ok := s.functions[name]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("function %q not registered", name)
	}

	dir := fmt.Sprintf("/tmp/faas/%s", name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	zipPath := dir + "/code.zip"
	if err := os.WriteFile(zipPath, zipData, 0644); err != nil {
		return fmt.Errorf("write zip: %w", err)
	}
	if out, err := exec.Command("unzip", "-o", zipPath, "-d", dir).CombinedOutput(); err != nil {
		return fmt.Errorf("unzip: %w\n%s", err, out)
	}
	os.Remove(zipPath)

	s.mu.Lock()
	cfg.CodeDir = dir
	s.functions[name] = cfg
	containers := s.pool[name]
	s.pool[name] = nil
	s.mu.Unlock()

	for _, c := range containers {
		go s.stop(c.id)
	}

	log.Printf("[scheduler] code uploaded for %s → %s", name, dir)
	return nil
}

func (s *Scheduler) Get(name string) (FunctionConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.functions[name]
	return cfg, ok
}

// Addr 返回容器的 host:port 地址
func (c *container) Addr() string { return c.addr }

// Acquire 返回一个可用容器（复用 idle 或冷启动新容器）
func (s *Scheduler) Acquire(name string) (*container, error) {
	s.mu.Lock()
	cfg, ok := s.functions[name]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("function %q not registered", name)
	}

	for _, c := range s.pool[name] {
		if c.state == stateIdle {
			c.state = stateBusy
			c.lastUsed = time.Now()
			s.mu.Unlock()
			return c, nil
		}
	}

	port := s.nextPort
	s.nextPort++
	s.mu.Unlock()

	c, err := s.start(cfg, port, stateBusy)
	if err != nil {
		return nil, fmt.Errorf("cold start failed: %w", err)
	}

	s.mu.Lock()
	s.pool[name] = append(s.pool[name], c)
	s.mu.Unlock()

	return c, nil
}

// Release 将容器标记为 idle，供下次复用
func (s *Scheduler) Release(c *container) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.state = stateIdle
	c.lastUsed = time.Now()
}

func (s *Scheduler) start(cfg FunctionConfig, port int, state containerState) (*container, error) {
	cfg.Port = port
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	inst, err := s.backend.Start(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &container{
		id:       inst.ID,
		addr:     inst.Addr,
		state:    state,
		lastUsed: time.Now(),
		funcName: inst.FuncName,
		nodeName: inst.NodeName,
	}, nil
}

type InstanceInfo struct {
	ID       string `json:"id"`
	FuncName string `json:"func_name"`
	NodeName string `json:"node_name"`
	State    string `json:"state"`
}

func (s *Scheduler) stateName(state containerState) string {
	switch state {
	case stateIdle:
		return "idle"
	case stateBusy:
		return "busy"
	case stateStarting:
		return "starting"
	default:
		return "unknown"
	}
}

// waitReady 轮询 /health 直到实例就绪
func waitReady(ctx context.Context, addr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout waiting for runtime")
		default:
		}
		out, err := exec.CommandContext(ctx, "curl", "-sf", fmt.Sprintf("http://%s/health", addr)).Output()
		if err == nil && string(out) == "ok" {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// reaper 定期回收长时间 idle 的容器（超过 5 分钟）
func (s *Scheduler) reaper() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		var stops []string
		for name, containers := range s.pool {
			var alive []*container
			for _, c := range containers {
				if c.state == stateIdle && time.Since(c.lastUsed) > 5*time.Minute {
					log.Printf("[scheduler] reaping idle container %s (%s)", c.id, name)
					stops = append(stops, c.id)
				} else {
					alive = append(alive, c)
				}
			}
			s.pool[name] = alive
		}
		s.mu.Unlock()

		for _, id := range stops {
			go s.stop(id)
		}
	}
}

func (s *Scheduler) stop(id string) {
	s.backend.Stop(context.Background(), id)
}

// FunctionNames returns all registered function names.
func (s *Scheduler) FunctionNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.functions))
	for name := range s.functions {
		names = append(names, name)
	}
	return names
}

// ContainerIDs returns all runtime instance IDs for a function.
func (s *Scheduler) ContainerIDs(funcName string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.pool[funcName]))
	for _, c := range s.pool[funcName] {
		ids = append(ids, c.id)
	}
	return ids
}

// Instances returns runtime instance metadata for a function.
func (s *Scheduler) Instances(funcName string) []InstanceInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	instances := make([]InstanceInfo, 0, len(s.pool[funcName]))
	for _, c := range s.pool[funcName] {
		instances = append(instances, InstanceInfo{
			ID:       c.id,
			FuncName: c.funcName,
			NodeName: c.nodeName,
			State:    s.stateName(c.state),
		})
	}
	return instances
}

// Stats returns the number of busy and idle containers for a function.
// Containers in stateStarting are excluded from both counts.
func (s *Scheduler) Stats(funcName string) (busy, idle int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.pool[funcName] {
		switch c.state {
		case stateBusy:
			busy++
		case stateIdle:
			idle++
		}
	}
	return
}

// ColdStartOne pre-warms one container for funcName in the background.
// The container is added to the pool as idle once it is ready.
func (s *Scheduler) ColdStartOne(funcName string) {
	s.mu.Lock()
	cfg, ok := s.functions[funcName]
	if !ok {
		s.mu.Unlock()
		return
	}
	port := s.nextPort
	s.nextPort++
	s.mu.Unlock()

	go func() {
		c, err := s.start(cfg, port, stateIdle)
		if err != nil {
			log.Printf("[scheduler] ColdStartOne failed for %s: %v", funcName, err)
			return
		}
		s.mu.Lock()
		s.pool[funcName] = append(s.pool[funcName], c)
		s.mu.Unlock()
		log.Printf("[scheduler] pre-warmed instance %s for %s", c.id, funcName)
	}()
}

// RemoveIdle removes the oldest idle container for funcName if it has been
// idle for more than 2 minutes. Returns false if no such container exists.
func (s *Scheduler) RemoveIdle(funcName string) bool {
	s.mu.Lock()

	containers := s.pool[funcName]
	oldest := -1
	for i, c := range containers {
		if c.state != stateIdle {
			continue
		}
		if oldest == -1 || c.lastUsed.Before(containers[oldest].lastUsed) {
			oldest = i
		}
	}
	if oldest == -1 || time.Since(containers[oldest].lastUsed) < 2*time.Minute {
		s.mu.Unlock()
		return false
	}

	c := containers[oldest]
	s.pool[funcName] = append(containers[:oldest], containers[oldest+1:]...)
	s.mu.Unlock()

	go s.stop(c.id)
	log.Printf("[scheduler] scale-down removed idle container %s for %s", c.id, funcName)
	return true
}
