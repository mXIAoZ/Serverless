#!/usr/bin/env bash
# stop.sh — 停止所有 FaaS 服务和运行实例
cd "$(dirname "$0")"

BACKEND="${FAAS_BACKEND:-docker}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"

echo "==> stopping services..."
for pid_file in /tmp/faas-gateway.pid /tmp/faas-scalersvc.pid /tmp/faas-logdaemon.pid; do
  if [ -f "$pid_file" ]; then
    kill "$(cat $pid_file)" 2>/dev/null || true
    rm "$pid_file"
  fi
done

echo "==> stopping runtime instances..."
if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  kubectl delete pod -n "$K8S_NAMESPACE" -l faas.managed-by=local-faas --ignore-not-found=true 2>/dev/null || true
  pkill -f "kubectl port-forward.*faas-" 2>/dev/null || true
else
  docker rm -f $(docker ps -aq --filter "label=faas.function") 2>/dev/null || true
fi

echo "done."
