#!/usr/bin/env bash
# test.sh — 部署示例函数并运行完整测试
set -e
cd "$(dirname "$0")"

GATEWAY="http://localhost:8080"
SCALER="http://localhost:9300"
LOGDAEMON_INTERNAL="http://localhost:9200"
LOGS="$GATEWAY"
FUNC="hello"
QUEUE_FUNC="hello-queue"
GO_FUNC="hello-go"
NODE_FUNC="hello-node"
JAVA_FUNC="hello-java"
BACKEND="${FAAS_BACKEND:-docker}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

ok()  { echo "  [ok] $*"; }
fail(){ echo "  [FAIL] $*"; exit 1; }
sep() { echo ""; echo "── $* ──────────────────────────────────────"; }
json_field() { python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(sys.argv[1], ""))' "$1"; }

# ── 1. 健康检查 ──────────────────────────────────────────────────
sep "health checks"
curl -sf $GATEWAY/health >/dev/null && ok "gateway"
curl -sf $SCALER/health  >/dev/null && ok "scalersvc"
curl -sf $LOGDAEMON_INTERNAL/health >/dev/null && ok "internal logdaemon"

# ── 2. 注册函数 ──────────────────────────────────────────────────
sep "register function"
RESP=$(curl -s -X POST $GATEWAY/functions/$FUNC \
  -H "Content-Type: application/json" \
  -d '{"handler":"handler.handler"}')
echo "  $RESP"
case "$RESP" in
  *'already registered'*) echo "  (reusing existing function)" ;;
esac

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
if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  CID=$(curl -sf $GATEWAY/internal/containers/$FUNC | python3 -c "
import sys, json
containers = json.load(sys.stdin)
print(containers[0] if containers else '')
")
else
  CID=$(docker ps -q --filter "label=faas.function=$FUNC" | head -1)
fi
if [ -z "$CID" ]; then
  CID=$(curl -sf $SCALER/scale/$FUNC | python3 -c "
import sys, json
d = json.load(sys.stdin)
metrics = d.get('metrics') or {}
for cid in metrics:
    if cid.startswith('faas-' + '$FUNC' + '-'):
        print(cid)
        break
")
fi
[ -n "$CID" ] || fail "no runtime instance found for metrics injection"
curl -sf -X POST $SCALER/metrics/$CID \
  -H "Content-Type: application/json" \
  -d "{\"container_id\":\"$CID\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"invocation_count\":200,\"success_count\":180,\"error_count\":20,\"p99_latency_ms\":800,\"memory_bytes\":26214400}" \
  && echo "  metrics injected"

sleep 6
STATUS=$(curl -sf $SCALER/scale/$FUNC)
echo "  scale status after injection:"
echo "$STATUS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
dec = d.get('last_decision') or {}
print(f'  action={dec.get(\"action\")} reason={dec.get(\"reason\")}')
print(f'  total replicas: {d[\"total\"]}')
"
echo "$STATUS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
dec = d.get('last_decision') or {}
assert dec.get('action') == 'scale-up', dec
assert 'p99=' in (dec.get('reason') or ''), dec
" && ok "p99 metrics trigger scale-up"

# ── 10. 并发压测队列触发扩容 ───────────────────────────────────────
sep "queue backlog triggers scale-up"
RESP=$(curl -s -X POST $GATEWAY/functions/$QUEUE_FUNC \
  -H "Content-Type: application/json" \
  -d '{"handler":"handler.handler"}')
echo "  $RESP"
case "$RESP" in
  *'already registered'*) echo "  (reusing existing function)" ;;
esac
zip -j /tmp/$QUEUE_FUNC.zip runtime/examples/python/handler.py -q
curl -sf -X PUT $GATEWAY/functions/$QUEUE_FUNC/code \
  --data-binary @/tmp/$QUEUE_FUNC.zip >/dev/null
Q_STATUS=$(curl -sf $GATEWAY/queues/$QUEUE_FUNC)
MAX_INFLIGHT=$(echo "$Q_STATUS" | json_field max_inflight)
QUEUE_TARGET=$((MAX_INFLIGHT + 4))
python3 - "$GATEWAY" "$QUEUE_FUNC" "$QUEUE_TARGET" <<'PY' &
import concurrent.futures, json, sys, urllib.request

gateway, func_name, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
payload = json.dumps({"name": "Queue", "sleep_ms": 5000}).encode()
url = f"{gateway}/invoke/{func_name}"

def invoke(_):
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()

with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
    futures = [pool.submit(invoke, i) for i in range(count)]
    for fut in futures:
        fut.result()
PY
LOAD_PID=$!
sleep 1
QUEUE_JSON=$(curl -sf $GATEWAY/queues/$QUEUE_FUNC)
echo "$QUEUE_JSON" | python3 -m json.tool
QUEUED=$(echo "$QUEUE_JSON" | json_field queued)
[ "$QUEUED" -ge 1 ] || fail "expected queued requests during burst"

QUEUE_SCALE_OK=0
for _ in 1 2 3 4 5 6 7 8; do
  QUEUE_SCALE=$(curl -sf $SCALER/scale/$QUEUE_FUNC)
  echo "$QUEUE_SCALE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
dec = d.get('last_decision') or {}
print(f'  queued={d.get(\"queued\")} action={dec.get(\"action\")} reason={dec.get(\"reason\")}')
print(f'  total replicas: {d[\"total\"]}')
"
  if echo "$QUEUE_SCALE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
dec = d.get('last_decision') or {}
assert dec.get('action') == 'scale-up', dec
reason = dec.get('reason') or ''
assert 'queue=' in reason or 'queued=' in reason or 'desired=' in reason, dec
" >/dev/null 2>&1; then
    QUEUE_SCALE_OK=1
    break
  fi
  sleep 1
done
[ "$QUEUE_SCALE_OK" -eq 1 ] || fail "expected scaler queue-based scale-up decision"
ok "queue backlog triggers scale-up"
wait $LOAD_PID


# ── 11. Bootstrap unit tests ─────────────────────────────────────────
sep "bootstrap unit tests"
if command -v node >/dev/null 2>&1; then
  node runtime/bootstrap/nodejs_bootstrap_test.js && ok "nodejs bootstrap tests"
else
  fail "node is required to run runtime/bootstrap/nodejs_bootstrap_test.js"
fi
if ! command -v javac >/dev/null 2>&1; then
  fail "javac is required to compile runtime/tests/java/JavaBootstrapJsonTest.java"
fi
JAVA_BOOTSTRAP_TEST_DIR="/tmp/faas-java-bootstrap-test"
rm -rf "$JAVA_BOOTSTRAP_TEST_DIR"
mkdir -p "$JAVA_BOOTSTRAP_TEST_DIR"
javac --release 21 -d "$JAVA_BOOTSTRAP_TEST_DIR" \
  runtime/bootstrap/java/JavaBootstrap.java \
  runtime/tests/java/JavaBootstrapJsonTest.java
java -cp "$JAVA_BOOTSTRAP_TEST_DIR" JavaBootstrapJsonTest && ok "java bootstrap JSON tests"

# ── 12. Go runtime smoke test ───────────────────────────────────────
sep "go runtime invoke"
RESP=$(curl -s -X POST $GATEWAY/functions/$GO_FUNC \
  -H "Content-Type: application/json" \
  -d '{"runtime":"go","handler":"bootstrap"}')
echo "  $RESP"
case "$RESP" in
  *'already registered'*) echo "  (reusing existing function)" ;;
esac
GO_BUILD_DIR="/tmp/faas-go-example"
rm -rf "$GO_BUILD_DIR"
mkdir -p "$GO_BUILD_DIR"
GOOS=linux GOARCH=arm64 go build -o "$GO_BUILD_DIR/bootstrap" ./runtime/examples/go
zip -j /tmp/$GO_FUNC.zip "$GO_BUILD_DIR/bootstrap" -q
curl -sf -X PUT $GATEWAY/functions/$GO_FUNC/code \
  --data-binary @/tmp/$GO_FUNC.zip >/dev/null
RESP=$(curl -sf -X POST $GATEWAY/invoke/$GO_FUNC \
  -H "Content-Type: application/json" \
  -d '{"name":"Gopher"}')
echo "  response: $RESP"
echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['statusCode']==200 and d['message']=='Hello, Gopher!' and d.get('requestId')" \
  && ok "go runtime response"

# ── 13. Node.js runtime smoke test ───────────────────────────────────
sep "nodejs runtime invoke"
RESP=$(curl -s -X POST $GATEWAY/functions/$NODE_FUNC \
  -H "Content-Type: application/json" \
  -d '{"runtime":"nodejs","handler":"handler.handler"}')
echo "  $RESP"
case "$RESP" in
  *'already registered'*) echo "  (reusing existing function)" ;;
esac
zip -j /tmp/$NODE_FUNC.zip runtime/examples/nodejs/handler.js -q
curl -sf -X PUT $GATEWAY/functions/$NODE_FUNC/code \
  --data-binary @/tmp/$NODE_FUNC.zip >/dev/null
RESP=$(curl -sf -X POST $GATEWAY/invoke/$NODE_FUNC \
  -H "Content-Type: application/json" \
  -d '{"name":"Node"}')
echo "  response: $RESP"
echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['statusCode']==200 and d['message']=='Hello, Node!' and d.get('requestId')" \
  && ok "nodejs runtime response"

# ── 14. Java runtime smoke test ──────────────────────────────────────
sep "java runtime invoke"
RESP=$(curl -s -X POST $GATEWAY/functions/$JAVA_FUNC \
  -H "Content-Type: application/json" \
  -d '{"runtime":"java","handler":"Hello::handleRequest"}')
echo "  $RESP"
case "$RESP" in
  *'already registered'*) echo "  (reusing existing function)" ;;
esac
if ! command -v javac >/dev/null 2>&1; then
  fail "javac is required to compile runtime/examples/java/Hello.java for the smoke test"
fi
JAVA_BUILD_DIR="/tmp/faas-java-example"
rm -rf "$JAVA_BUILD_DIR"
mkdir -p "$JAVA_BUILD_DIR"
javac --release 21 -d "$JAVA_BUILD_DIR" runtime/examples/java/Hello.java
zip -j /tmp/$JAVA_FUNC.zip "$JAVA_BUILD_DIR"/*.class -q
curl -sf -X PUT $GATEWAY/functions/$JAVA_FUNC/code \
  --data-binary @/tmp/$JAVA_FUNC.zip >/dev/null
RESP=$(curl -sf -X POST $GATEWAY/invoke/$JAVA_FUNC \
  -H "Content-Type: application/json" \
  -d '{"name":"Java"}')
echo "  response: $RESP"
echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['statusCode']==200 and d['message']=='Hello, Java!'" \
  && ok "java runtime response"

# ── 15. 查看日志 ──────────────────────────────────────────────────
sep "function logs (last 10)"
if [ "$BACKEND" = "k8s" ] || [ "$BACKEND" = "kubernetes" ]; then
  LOG_OK=0
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    RESP=$(curl -s "$LOGS/logs/$FUNC?tail=10" || true)
    if [ -n "$RESP" ] && echo "$RESP" | python3 -c "
import sys, json
entries = json.load(sys.stdin)
assert entries, entries
assert any(e.get('stream') == 'stdout' for e in entries), entries
assert any('[runtime-api]' in e.get('line', '') or '[bootstrap]' in e.get('line', '') or 'Hello, Alice!' in e.get('line', '') for e in entries), entries
" >/dev/null 2>&1; then
      LOG_OK=1
      break
    fi
    sleep 1
  done
  [ "$LOG_OK" -eq 1 ] || fail "expected gateway log API through collector/proxy for $FUNC"
  echo "$RESP" | python3 -c "
import sys, json
for e in json.load(sys.stdin):
    ts = e['time'][:19].replace('T',' ')
    print(f'  [{ts}] [{e[\"stream\"]}] {e[\"line\"]}')
"
  ok "gateway log API through collector/proxy visible"
else
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
fi

sep "all tests passed"
