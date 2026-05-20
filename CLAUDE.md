# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

- Format Go code: `gofmt -w <files>`
- Run all Go tests: `go test ./...`
- Run one package's tests: `go test ./gateway/scheduler`
- Run one test: `go test ./gateway/scheduler -run TestUploadCodeStoresCodeURL`
- Check shell script syntax: `bash -n start.sh stop.sh test.sh`
- Build main binaries manually: `go build -o bin/gateway ./gateway/ && go build -o bin/scalersvc ./scalersvc/ && go build -o bin/logdaemon ./logdaemon/`
- Build runtime Linux binaries used by the image: `GOOS=linux GOARCH=arm64 go build -o bin/runtime-server-linux ./runtime/cmd/runtime/ && GOOS=linux GOARCH=arm64 go build -o bin/runtime-agent-linux ./runtime/cmd/agent/ && GOOS=linux GOARCH=arm64 go build -o bin/go-bootstrap-linux ./runtime/cmd/go-bootstrap/`
- Build runtime image: `docker build -t faas-runtime:latest .`
- Start full Docker-backed system: `./start.sh`
- Run end-to-end smoke/integration test: `./test.sh`
- Stop local system: `./stop.sh`
- Start Kubernetes/minikube backend: `PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./start.sh`
- Test Kubernetes/minikube backend: `PATH="$PATH:$HOME/.local/bin" MAX_REPLICAS=10 FAAS_BACKEND=k8s ./test.sh`
- Stop Kubernetes/minikube backend: `PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./stop.sh`
- Run with Mongo persistence: `MONGO_URI=mongodb://localhost:27017 MONGO_DB=faas ./start.sh`

## Architecture Overview

This is a local FaaS/serverless learning platform. The main datapath is: client -> gateway queue -> router -> scheduler -> Docker container or Kubernetes Pod -> runtime-agent -> runtime-server -> language bootstrap -> user handler.

- `gateway/` is the external API and control-plane entrypoint on `:8080`. `gateway/main.go` wires function management routes, invocation routes, metrics forwarding, scaler internal APIs, and logdaemon internal APIs.
- `gateway/queue/` implements per-function admission control and backpressure before invocation reaches the router. Its env knobs are `MAX_INFLIGHT_PER_FUNCTION`, `MAX_QUEUE_PER_FUNCTION`, and `QUEUE_TIMEOUT_MS`.
- `gateway/router/` owns one synchronous invoke: validate the function, acquire an instance from scheduler, POST to the instance agent `/invoke`, return the function response, and release the instance.
- `gateway/scheduler/` owns function metadata, uploaded code, instance pools, cold starts, idle reaping, and backend abstraction. `FunctionConfig` includes runtime settings plus `CodeDir`, `CodeKey`, and `CodeURL` for local/MinIO code delivery.
- `gateway/scheduler/docker_backend.go` starts function containers through the Docker Engine HTTP API over `/var/run/docker.sock`, not by shelling out to `docker` from the service process.
- `gateway/scheduler/k8s_backend.go` uses `client-go` for Pod lifecycle and SPDY port-forward. Kubernetes code delivery prefers MinIO object storage plus an initContainer that downloads a presigned `CodeURL` into an `emptyDir`; the older minikube hostPath sync path remains as fallback when there is `CodeDir` but no `CodeKey`.
- `gateway/scheduler/code_store.go` stores uploaded zip bytes in MinIO when `MINIO_ENDPOINT` is set. `MINIO_PUBLIC_READ=false` is the default path; code loading uses presigned URLs, and Mongo persistence stores both `CodeKey` and `CodeURL`.
- `runtime/` is what runs inside each function container. `runtime/entrypoint.sh` starts `runtime-server` in the background and `runtime-agent` in the foreground. The agent listens on `:9001`; the runtime-server listens on `:9000` and implements the Lambda-style Runtime API.
- `runtime/bootstrap/python3_bootstrap.py` loads Python handlers from `/function`. `runtime/cmd/go-bootstrap/` executes a user-provided `/function/bootstrap` binary for Go functions and requires JSON stdout.
- `scalersvc/` listens on `:9300`, receives agent metrics via gateway, periodically queries gateway internal APIs for functions/stats/queues/containers, and calls gateway scale-up/scale-down internal endpoints. It can persist latest metrics, status, and decisions to Mongo when `MONGO_URI` is set.
- `logdaemon/` listens on `:9200`. Docker mode follows Docker events/logs for containers labeled `faas.function`. Kubernetes uses a host proxy plus a DaemonSet collector; the collector uses in-cluster Kubernetes APIs, and the proxy uses gateway instance metadata to route log requests by node.
- `start.sh`, `stop.sh`, and `test.sh` are local development/validation entrypoints. They intentionally clean local processes, Docker containers, Kubernetes Pods, MinIO port-forward state, and temp manifests; do not treat them as production-safe scripts.

## Important Runtime Details

- Function names are constrained to DNS-label style via `isValidFunctionName`; unsafe names should be rejected at registration rather than sanitized silently.
- Uploaded zip extraction in Go must preserve executable bits for Go `bootstrap` binaries and must keep zip-slip protection intact.
- Kubernetes initContainer extraction also checks real paths to avoid zip-slip and restores file mode from zip metadata.
- `UploadCode()` replaces `/tmp/faas/<name>` through a temp directory and `.old` backup, then stops old instances so the next call uses new code.
- Docker backend bind-mounts local `CodeDir` at `/function`; Kubernetes MinIO path mounts an `emptyDir` at `/function` and fills it in the initContainer.
- `runtime/cmd/runtime/main.go` selects bootstrap by `FUNCTION_RUNTIME`: Python uses `/runtime/bootstrap/python3_bootstrap.py`, Go uses `/runtime/bootstrap/go-bootstrap`.
- `runtime-agent` reports metrics every 10s to `http://{GATEWAY_ADDR}/containers/{containerID}/metrics`; gateway forwards to `SCALER_ADDR` when configured.
- Mongo is optional. Without `MONGO_URI`, gateway and scaler use in-memory stores. With Mongo, function metadata and scaler state survive process restart, but running containers/Pods are still runtime state managed separately.

## Verification Notes

- For normal code changes, run `gofmt` on touched Go files and `go test ./...`.
- For script changes, run `bash -n start.sh stop.sh test.sh`.
- For scheduler/runtime/backend changes, prefer at least `./start.sh && ./test.sh && ./stop.sh` on Docker backend.
- For Kubernetes backend changes, use `PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s MAX_REPLICAS=10 ./test.sh` after starting with the same backend.
- `test.sh` is an integration smoke script with some timing-sensitive autoscaling checks; unit tests are the faster signal for small logic changes.

## Documentation Caveat

Some README sections still describe older implementation details such as service-process use of `docker run`, `kubectl apply`, `kubectl exec`, or in-memory-only metadata. Prefer current source files over README text when they disagree, especially in `gateway/scheduler/*`, `logdaemon/shared.go`, and `gateway/scheduler/mongo_store.go`.
