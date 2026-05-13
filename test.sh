#!/usr/bin/env bash
# test.sh — 部署示例函数并运行完整测试
set -e
cd "$(dirname "$0")"

GATEWAY="http://localhost:8080"
SCALER="http://localhost:9300"
LOGS="http://localhost:9200"
FUNC="hello"
BACKEND="${FAAS_BACKEND:-docker}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

ok()  { echo "  [ok] $*"; }
fail(){ echo "  [FAIL] $*"; exit 1; }
sep() { echo ""; echo "── $* ──────────────────────────────────────"; }

# ── 1. 健康检查 ──────────────────────────────────────────────────
sep "health checks"
curl -sf $GATEWAY/health >/dev/null && ok "gateway"
curl -sf $SCALER/health  >/dev/null && ok "scalersvc"
curl -sf $LOGS/health    >/dev/null && ok "logdaemon"

# ── 2. 注册函数 ──────────────────────────────────────────────────
sep "register function"
RESP=$(curl -s -X POST $GATEWAY/functions/$FUNC \
  -H "Content-Type: application/json" \
  -d '{"handler":"handler.handler"}')
echo "  $RESP"

# ── 3. 上传代码 ──────────────────────────────────────────────────
sep "upload code"
zip -j /tmp/$FUNC.zip runtime/examples/python/handler.py -q
RESP=$(curl -sf -X PUT $GATEWAY/functions/$FUNC/code \
  --data-binary @/tmp/$FUNC.zip)
echo "  $RESP"

# ── 4. 首次调用（冷启动）────────────────────────────────────────
sep "first invoke (cold start)"
START=$(now_ms)
RESP=$(curl -sf -X POST $GATEWAY/invoke/$FUNC \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice"}')
END=$(now_ms)
echo "  response: $RESP"
echo "  latency:  $((END - START))ms"
echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['statusCode']==200" \
  && ok "status 200"

# ── 5. 热调用（复用容器）────────────────────────────────────────
sep "warm invokes"
for name in Bob Carol Dave; do
  START=$(now_ms)
  RESP=$(curl -sf -X POST $GATEWAY/invoke/$FUNC \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$name\"}")
  END=$(now_ms)
  MSG=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['message'])")
  echo "  $MSG  ($((END - START))ms)"
done

# ── 6. 查看队列状态 ────────────────────────────────────────────────
sep "queue status"
curl -sf $GATEWAY/queues/$FUNC | python3 -m json.tool

# ── 7. 查看扩缩容状态 ────────────────────────────────────────────
sep "scale status"
curl -sf $SCALER/scale/$FUNC | python3 -m json.tool

# ── 8. 等待 agent 上报指标 ───────────────────────────────────────
sep "waiting 12s for agent metrics..."
sleep 12
echo "  scale status after metrics:"
curl -sf $SCALER/scale/$FUNC | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  replicas: total={d[\"total\"]} busy={d[\"busy\"]} idle={d[\"idle\"]}')
dec = d.get('last_decision') or {}
print(f'  decision: {dec.get(\"action\",\"n/a\")} — {dec.get(\"reason\",\"\")}')
m = d.get('metrics', {})
for cid, v in m.items():
    print(f'  container {cid}: inv={v[\"invocation_count\"]} p99={v[\"p99_latency_ms\"]:.1f}ms mem={v[\"memory_bytes\"]>>20}MB')
"

# ── 9. 注入高 p99 触发 scale-up ──────────────────────────────────
sep "inject high-p99 metrics → trigger scale-up"
CID=$(curl -sf $SCALER/scale/$FUNC | python3 -c "
import sys, json
d = json.load(sys.stdin)
metrics = d.get('metrics') or {}
print(next(iter(metrics), ''))
")
if [ -z "$CID" ]; then
  if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
    CID=$(kubectl get pod -n "$K8S_NAMESPACE" -l faas.function=$FUNC -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  else
    CID=$(docker ps -q --filter "label=faas.function" | head -1)
  fi
fi
[ -n "$CID" ] || fail "no runtime instance found for metrics injection"
curl -sf -X POST $SCALER/metrics/$CID \
  -H "Content-Type: application/json" \
  -d "{\"container_id\":\"$CID\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"invocation_count\":200,\"success_count\":180,\"error_count\":20,\"p99_latency_ms\":800,\"memory_bytes\":26214400}" \
  && echo "  metrics injected"

sleep 6
echo "  scale status after injection:"
curl -sf $SCALER/scale/$FUNC | python3 -c "
import sys, json
d = json.load(sys.stdin)
dec = d.get('last_decision') or {}
print(f'  action={dec.get(\"action\")} reason={dec.get(\"reason\")}')
print(f'  total replicas: {d[\"total\"]}')
"

# ── 10. 查看日志 ──────────────────────────────────────────────────
sep "function logs (last 10)"
RESP=$(curl -s "$LOGS/logs/$FUNC?tail=10" || true)
if [ -z "$RESP" ]; then
  echo '  (no logs yet — logdaemon may have started after containers)'
else
  echo "$RESP" | python3 -c "
import sys, json
try:
    entries = json.load(sys.stdin)
except Exception:
    print('  (no logs yet — logdaemon may have no buffer for this function)')
    sys.exit(0)
if not entries:
    print('  (no logs yet — logdaemon may have started after containers)')
for e in entries:
    ts = e['time'][:19].replace('T',' ')
    print(f'  [{ts}] [{e[\"stream\"]}] {e[\"line\"]}')
"
fi

sep "all tests passed"
