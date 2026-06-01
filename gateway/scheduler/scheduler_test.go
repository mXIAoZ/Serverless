package scheduler

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRegisterDefaultsRuntime(t *testing.T) {
	s := testScheduler()
	if err := s.Register(FunctionConfig{Name: "default-runtime"}); err != nil {
		t.Fatal(err)
	}
	cfg, ok := s.Get("default-runtime")
	if !ok {
		t.Fatal("function missing after register")
	}
	if cfg.Runtime != "python3" {
		t.Fatalf("Runtime = %q, want python3", cfg.Runtime)
	}
}

func TestRegisterAcceptsSupportedRuntimes(t *testing.T) {
	for _, runtime := range []string{"python3", "go", "nodejs", "java"} {
		t.Run(runtime, func(t *testing.T) {
			s := testScheduler()
			if err := s.Register(FunctionConfig{Name: "fn-" + runtime, Runtime: runtime}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRegisterRejectsUnsupportedRuntime(t *testing.T) {
	s := testScheduler()
	if err := s.Register(FunctionConfig{Name: "bad-runtime", Runtime: "ruby"}); err == nil {
		t.Fatal("expected unsupported runtime to fail")
	}
}

func TestRegisterAcceptsMQTrigger(t *testing.T) {
	s := testScheduler()
	err := s.Register(FunctionConfig{Name: "mq-fn", Triggers: []TriggerConfig{{
		ID:      "orders-trigger",
		Type:    "mq",
		Enabled: true,
		MQID:    "rabbitmq-main",
		Queue:   "orders",
	}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegisterRejectsInvalidMQTrigger(t *testing.T) {
	s := testScheduler()
	err := s.Register(FunctionConfig{Name: "bad-trigger", Triggers: []TriggerConfig{{
		ID:      "orders-trigger",
		Type:    "mq",
		Enabled: true,
		Queue:   "orders",
	}}})
	if err == nil {
		t.Fatal("expected missing mq_id to fail")
	}
}

func TestMQTriggersIncludesFunctionAndTuningFields(t *testing.T) {
	s := testScheduler()
	if err := s.Register(FunctionConfig{Name: "mq-fn", Triggers: []TriggerConfig{{
		ID:             "orders-trigger",
		Type:           "mq",
		Enabled:        true,
		MQID:           "rabbitmq-main",
		Queue:          "orders",
		MaxConcurrency: 3,
		Prefetch:       2,
		MaxAttempts:    4,
		RetryBackoffMS: 250,
		DLQ:            "orders.dlq",
	}}}); err != nil {
		t.Fatal(err)
	}
	triggers := s.MQTriggers()
	if len(triggers) != 1 {
		t.Fatalf("triggers = %d, want 1", len(triggers))
	}
	trigger := triggers[0]
	if trigger.Function != "mq-fn" || trigger.Prefetch != 2 || trigger.RetryBackoffMS != 250 || trigger.DLQ != "orders.dlq" {
		t.Fatalf("unexpected trigger: %+v", trigger)
	}
}

func TestAcquireInstanceLeaseReleasesContainer(t *testing.T) {
	s := testScheduler()
	s.functions["lease-fn"] = FunctionConfig{Name: "lease-fn", Timeout: 7, Triggers: []TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}
	c := &container{id: "inst-1", addr: "127.0.0.1:9001", state: stateIdle, funcName: "lease-fn"}
	s.pool["lease-fn"] = []*container{c}

	lease, err := s.AcquireInstance(AcquireInstanceRequest{
		Function:  "lease-fn",
		Source:    "mq",
		MQID:      "rabbitmq-main",
		TriggerID: "orders-trigger",
		MessageID: "msg-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if lease.LeaseID == "" || lease.InstanceID != "inst-1" || lease.TimeoutSeconds != 7 {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if c.state != stateBusy {
		t.Fatalf("container state = %v, want busy", c.state)
	}
	if err := s.ReleaseInstance(lease.LeaseID, "success"); err != nil {
		t.Fatal(err)
	}
	if c.state != stateIdle {
		t.Fatalf("container state = %v, want idle", c.state)
	}
	if err := s.ReleaseInstance(lease.LeaseID, "success"); err == nil {
		t.Fatal("expected second release to fail")
	}
}

func TestAcquireInstanceRejectsWrongTrigger(t *testing.T) {
	s := testScheduler()
	s.functions["lease-fn"] = FunctionConfig{Name: "lease-fn", Triggers: []TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}
	err := s.ReleaseInstance("missing", "success")
	if err == nil {
		t.Fatal("expected missing lease to fail")
	}
	_, err = s.AcquireInstance(AcquireInstanceRequest{Function: "lease-fn", Source: "mq", MQID: "wrong", TriggerID: "orders-trigger"})
	if !errors.Is(err, ErrInvalidTrigger) {
		t.Fatalf("err = %v, want ErrInvalidTrigger", err)
	}
	_, err = s.AcquireInstance(AcquireInstanceRequest{Function: "lease-fn", MQID: "rabbitmq-main", TriggerID: "orders-trigger"})
	if !errors.Is(err, ErrInvalidTrigger) {
		t.Fatalf("err = %v, want ErrInvalidTrigger", err)
	}
}

func TestReapExpiredLeasesRemovesContainer(t *testing.T) {
	backend := &recordingBackend{}
	s := NewForTesting(backend)
	s.functions["lease-fn"] = FunctionConfig{Name: "lease-fn", Timeout: 1, Triggers: []TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}
	s.AddIdleTestInstance("lease-fn", "inst-1", "127.0.0.1:9001")
	lease, err := s.AcquireInstance(AcquireInstanceRequest{Function: "lease-fn", Source: "mq", MQID: "rabbitmq-main", TriggerID: "orders-trigger", TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.reapExpiredLeases(time.Now().Add(10 * time.Second))
	if ids := s.ContainerIDs("lease-fn"); len(ids) != 0 {
		t.Fatalf("container ids = %v, want empty", ids)
	}
	backend.waitStopped(t, "inst-1")
	if err := s.ReleaseInstance(lease.LeaseID, "success"); err == nil {
		t.Fatal("expected expired lease to be gone")
	}
}

func TestAcquireInstanceClampsTimeout(t *testing.T) {
	s := testScheduler()
	s.functions["lease-fn"] = FunctionConfig{Name: "lease-fn", Timeout: 7, Triggers: []TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}
	s.pool["lease-fn"] = []*container{{id: "inst-1", addr: "127.0.0.1:9001", state: stateIdle, funcName: "lease-fn"}}

	lease, err := s.AcquireInstance(AcquireInstanceRequest{Function: "lease-fn", Source: "mq", MQID: "rabbitmq-main", TriggerID: "orders-trigger", TimeoutSeconds: 999})
	if err != nil {
		t.Fatal(err)
	}
	if lease.TimeoutSeconds != 7 {
		t.Fatalf("TimeoutSeconds = %d, want 7", lease.TimeoutSeconds)
	}
}

func TestReleaseInstanceRejectsInvalidStatus(t *testing.T) {
	s := testScheduler()
	if err := s.ReleaseInstance("lease", "bad"); !errors.Is(err, ErrInvalidLeaseStatus) {
		t.Fatalf("err = %v, want ErrInvalidLeaseStatus", err)
	}
}

func TestReleaseInstanceTimeoutRemovesContainer(t *testing.T) {
	backend := &recordingBackend{}
	s := NewForTesting(backend)
	s.functions["lease-fn"] = FunctionConfig{Name: "lease-fn", Timeout: 7, Triggers: []TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}
	s.AddIdleTestInstance("lease-fn", "inst-1", "127.0.0.1:9001")

	lease, err := s.AcquireInstance(AcquireInstanceRequest{Function: "lease-fn", Source: "mq", MQID: "rabbitmq-main", TriggerID: "orders-trigger"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseInstance(lease.LeaseID, LeaseStatusTimeout); err != nil {
		t.Fatal(err)
	}
	if ids := s.ContainerIDs("lease-fn"); len(ids) != 0 {
		t.Fatalf("container ids = %v, want empty", ids)
	}
	backend.waitStopped(t, "inst-1")
}

func TestExtractZipRejectsEscapedPath(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("../escape.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := extractZip(buf.Bytes(), dir); err == nil {
		t.Fatal("expected escaped zip path to fail")
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "escape.txt")); !os.IsNotExist(err) {
		t.Fatalf("escaped file exists or stat failed unexpectedly: %v", err)
	}
}

func TestExtractZipPreservesExecutableMode(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	h, err := zip.FileInfoHeader(fakeFileInfo{name: "bootstrap", mode: 0755})
	if err != nil {
		t.Fatal(err)
	}
	w, err := zw.CreateHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("#!/bin/sh\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := extractZip(buf.Bytes(), dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "bootstrap"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0755 {
		t.Fatalf("mode = %v, want 0755", got)
	}
}

func TestUploadCodeReplacesOldFiles(t *testing.T) {
	name := "replace-test"
	s := &Scheduler{
		functions: map[string]FunctionConfig{name: {Name: name}},
		pool:      make(map[string][]*container),
		store:     newMemoryFunctionStore(),
	}
	dir := filepath.Join("/tmp/faas", name)
	defer os.RemoveAll(dir)

	if err := s.UploadCode(name, zipWithFiles(t, map[string]string{"old.txt": "old"})); err != nil {
		t.Fatal(err)
	}
	if err := s.UploadCode(name, zipWithFiles(t, map[string]string{"new.txt": "new"})); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("old file still exists or stat failed unexpectedly: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.txt")); err != nil {
		t.Fatalf("new file missing: %v", err)
	}
}

func TestUploadCodeStoresCodeURL(t *testing.T) {
	name := "url-test"
	s := &Scheduler{
		functions: map[string]FunctionConfig{name: {Name: name}},
		pool:      make(map[string][]*container),
		store:     newMemoryFunctionStore(),
		codeStore: fakeCodeStore{obj: CodeObject{Bucket: "bucket", Key: "key.zip", URL: "http://example.invalid/key.zip"}},
	}
	defer os.RemoveAll(filepath.Join("/tmp/faas", name))

	if err := s.UploadCode(name, zipWithFiles(t, map[string]string{"handler.py": "def handler(event, ctx): return {}"})); err != nil {
		t.Fatal(err)
	}
	cfg, ok := s.Get(name)
	if !ok {
		t.Fatal("function missing after upload")
	}
	if cfg.CodeKey != "key.zip" {
		t.Fatalf("CodeKey = %q, want key.zip", cfg.CodeKey)
	}
	if cfg.CodeURL != "http://example.invalid/key.zip" {
		t.Fatalf("CodeURL = %q, want presigned URL", cfg.CodeURL)
	}
}

func TestValidFunctionNameRejectsUnsafePaths(t *testing.T) {
	for _, name := range []string{"../escape", "with/slash", "with space", "UPPER"} {
		if isValidFunctionName(name) {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
	if !isValidFunctionName("hello-go") {
		t.Fatal("expected hello-go to be valid")
	}
}

func zipWithFiles(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testScheduler() *Scheduler {
	return &Scheduler{
		functions: make(map[string]FunctionConfig),
		pool:      make(map[string][]*container),
		store:     newMemoryFunctionStore(),
	}
}

type fakeCodeStore struct {
	obj CodeObject
}

func (s fakeCodeStore) SaveCode(context.Context, string, []byte) (CodeObject, error) {
	return s.obj, nil
}

type fakeFileInfo struct {
	name string
	mode os.FileMode
}

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return false }
func (f fakeFileInfo) Sys() any           { return nil }

type recordingBackend struct {
	once    sync.Once
	stopped chan string
}

func (b *recordingBackend) Name() string { return "recording" }

func (b *recordingBackend) Start(context.Context, FunctionConfig) (*RuntimeInstance, error) {
	return nil, errors.New("unexpected start")
}

func (b *recordingBackend) stopChan() chan string {
	b.once.Do(func() {
		b.stopped = make(chan string, 1)
	})
	return b.stopped
}

func (b *recordingBackend) Stop(_ context.Context, id string) error {
	b.stopChan() <- id
	return nil
}

func (b *recordingBackend) waitStopped(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-b.stopChan():
		if got != want {
			t.Fatalf("stopped = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("backend did not stop %q", want)
	}
}
