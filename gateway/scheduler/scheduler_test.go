package scheduler

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
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
