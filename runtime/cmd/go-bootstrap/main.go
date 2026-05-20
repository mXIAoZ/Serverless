package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

type invocationContext struct {
	awsRequestID string
	deadlineMS   string
}

type invocationError struct {
	ErrorType    string `json:"errorType"`
	ErrorMessage string `json:"errorMessage"`
}

func main() {
	runtimeAPI := envOr("RUNTIME_API", "http://localhost:9000")
	functionDir := envOr("FUNCTION_DIR", "/function")
	bootstrapPath := filepath.Join(functionDir, "bootstrap")

	if _, err := os.Stat(bootstrapPath); err != nil {
		log.Fatalf("[go-bootstrap] missing function binary %s: %v", bootstrapPath, err)
	}

	log.Printf("[go-bootstrap] using function binary %s", bootstrapPath)
	for {
		payload, ctx, err := nextInvocation(runtimeAPI)
		if err != nil {
			log.Printf("[go-bootstrap] next error: %v", err)
			continue
		}

		response, execErr := runFunctionBinary(bootstrapPath, payload, ctx)
		if execErr != nil {
			body, _ := json.Marshal(invocationError{
				ErrorType:    "FunctionError",
				ErrorMessage: execErr.Error(),
			})
			if postInvocation(runtimeAPI, ctx.awsRequestID, "error", body) != nil {
				log.Printf("[go-bootstrap] report error failed for %s", ctx.awsRequestID)
			}
			continue
		}

		if postInvocation(runtimeAPI, ctx.awsRequestID, "response", response) != nil {
			log.Printf("[go-bootstrap] report response failed for %s", ctx.awsRequestID)
		}
	}
}

func nextInvocation(runtimeAPI string) ([]byte, invocationContext, error) {
	resp, err := http.Get(runtimeAPI + "/runtime/invocation/next")
	if err != nil {
		return nil, invocationContext{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, invocationContext{}, fmt.Errorf("no invocation")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, invocationContext{}, fmt.Errorf("unexpected status %s: %s", resp.Status, bytes.TrimSpace(body))
	}

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, invocationContext{}, err
	}
	ctx := invocationContext{
		awsRequestID: resp.Header.Get("Lambda-Runtime-Aws-Request-Id"),
		deadlineMS:   resp.Header.Get("Lambda-Runtime-Deadline-Ms"),
	}
	if ctx.awsRequestID == "" {
		return nil, invocationContext{}, fmt.Errorf("missing request id")
	}
	return payload, ctx, nil
}

func runFunctionBinary(binaryPath string, payload []byte, inv invocationContext) ([]byte, error) {
	if inv.deadlineMS == "" {
		return nil, fmt.Errorf("missing deadline")
	}
	deadline, err := strconv.ParseInt(inv.deadlineMS, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid deadline %q: %w", inv.deadlineMS, err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.UnixMilli(deadline))
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = &bytes.Buffer{}
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"AWS_REQUEST_ID="+inv.awsRequestID,
		"DEADLINE_MS="+inv.deadlineMS,
	)

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("function timed out")
		}
		return nil, err
	}

	out := cmd.Stdout.(*bytes.Buffer).Bytes()
	if !json.Valid(out) {
		return nil, fmt.Errorf("function output is not valid JSON")
	}
	return out, nil
}

func postInvocation(runtimeAPI, requestID, action string, body []byte) error {
	url := fmt.Sprintf("%s/runtime/invocation/%s/%s", runtimeAPI, requestID, action)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("post %s: %s %s", action, resp.Status, bytes.TrimSpace(payload))
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
