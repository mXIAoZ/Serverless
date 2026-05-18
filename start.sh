#!/usr/bin/env bash
# start.sh — 一键启动整个 FaaS 系统
set -e

cd "$(dirname "$0")"

BACKEND="${FAAS_BACKEND:-docker}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"

start_k8s_log_collector() {
  echo "==> deploying k8s log collector..."
  mkdir -p /tmp/faas-bin
  cp ./bin/logdaemon-linux /tmp/faas-bin/logdaemon
  chmod +x /tmp/faas-bin/logdaemon
  minikube ssh -- "sudo mkdir -p /tmp/faas-bin"
  minikube cp /tmp/faas-bin/logdaemon /tmp/faas-bin/logdaemon >/dev/null
  python3 - "$K8S_NAMESPACE" <<'PY' > /tmp/faas-logdaemon.yaml
import pathlib, sys
namespace = sys.argv[1]
template = pathlib.Path("k8s-logdaemon.yaml").read_text()
print(template.replace("__K8S_NAMESPACE__", namespace), end="")
PY
  kubectl apply -f /tmp/faas-logdaemon.yaml >/dev/null
  kubectl rollout status daemonset/faas-log-collector -n "$K8S_NAMESPACE" --timeout=60s >/dev/null
}

# ── 1. 构建所有组件 ──────────────────────────────────────────────
echo "==> building binaries..."
go build -o bin/gateway       ./gateway/
go build -o bin/scalersvc     ./scalersvc/
go build -o bin/logdaemon     ./logdaemon/
GOOS=linux GOARCH=arm64 go build -o bin/logdaemon-linux ./logdaemon/

echo "==> cross-compiling runtime for Linux..."
GOOS=linux GOARCH=arm64 go build -o bin/runtime-server-linux ./runtime/cmd/runtime/
GOOS=linux GOARCH=arm64 go build -o bin/runtime-agent-linux  ./runtime/cmd/agent/

echo "==> building Docker image..."
docker build -t faas-runtime:latest . --quiet

if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  echo "==> loading image into minikube..."
  minikube status >/dev/null
  minikube image load faas-runtime:latest
fi

# ── 2. 清理残留进程和实例 ────────────────────────────────────────
echo "==> cleaning up..."
lsof -ti :8080 | xargs kill -9 2>/dev/null || true
lsof -ti :9200 | xargs kill -9 2>/dev/null || true
lsof -ti :9300 | xargs kill -9 2>/dev/null || true
if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  kubectl delete pod -n "$K8S_NAMESPACE" -l faas.managed-by=local-faas --ignore-not-found=true >/dev/null
  kubectl delete -f /tmp/faas-logdaemon.yaml --ignore-not-found=true >/dev/null 2>&1 || true
  pkill -f "kubectl port-forward.*faas-" 2>/dev/null || true
else
  docker rm -f $(docker ps -aq --filter "label=faas.function") 2>/dev/null || true
fi
sleep 0.5

# ── 3. 启动服务 ──────────────────────────────────────────────────
if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  echo "==> starting gateway (:8080, backend=$BACKEND)..."
  FAAS_BACKEND="$BACKEND" \
  K8S_NAMESPACE="$K8S_NAMESPACE" \
  GATEWAY_ADDR="${GATEWAY_ADDR:-host.minikube.internal:8080}" \
  SCALER_ADDR="localhost:9300" \
  PATH="$PATH:$HOME/.local/bin" \
    ./bin/gateway &
else
  echo "==> starting gateway (:8080, backend=$BACKEND)..."
  FAAS_BACKEND="$BACKEND" \
  GATEWAY_ADDR="${GATEWAY_ADDR:-host.docker.internal:8080}" \
  SCALER_ADDR="localhost:9300" \
    ./bin/gateway &
fi
echo $! > /tmp/faas-gateway.pid

echo "==> starting scalersvc (:9300)..."
GATEWAY_INTERNAL_ADDR="localhost:8080" \
  ./bin/scalersvc &
echo $! > /tmp/faas-scalersvc.pid

if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  start_k8s_log_collector
  echo "==> starting logdaemon proxy (:9200)..."
  LOGDAEMON_MODE="proxy" \
  K8S_NAMESPACE="$K8S_NAMESPACE" \
  GATEWAY_INTERNAL_ADDR="localhost:8080" \
  PATH="$PATH:$HOME/.local/bin" \
    ./bin/logdaemon &
else
  echo "==> starting logdaemon (:9200)..."
  LOGDAEMON_MODE="docker" ./bin/logdaemon &
fi
echo $! > /tmp/faas-logdaemon.pid

# ── 4. 等待就绪 ──────────────────────────────────────────────────
echo -n "==> waiting for services..."
until curl -sf http://localhost:8080/health >/dev/null 2>&1; do echo -n "."; sleep 0.3; done
until curl -sf http://localhost:9300/health >/dev/null 2>&1; do echo -n "."; sleep 0.3; done
until curl -sf http://localhost:9200/health >/dev/null 2>&1; do echo -n "."; sleep 0.3; done
echo " ready"

echo ""
echo "Services running:"
echo "  gateway    http://localhost:8080"
echo "  scalersvc  http://localhost:9300"
echo "  logdaemon  http://localhost:9200"
echo "  backend    $BACKEND"
echo ""
echo "Run ./test.sh to deploy and test a function."
echo "Run ./stop.sh to shut everything down."
