# Runtime Component

`runtime/` 是函数容器内部真正执行用户代码的部分。它把“平台控制逻辑”和“用户 handler 逻辑”拆成两个进程：

- `runtime-server`：实现最小版 Lambda Runtime API。
- `runtime-agent`：作为容器入口，代理调用、暴露健康检查与指标。

此外，这个目录还包含 Python bootstrap 和示例 handler。

## 目录结构

```text
runtime/
├── cmd/
│   ├── runtime/
│   │   └── main.go
│   └── agent/
│       └── main.go
├── bootstrap/
│   └── python3_bootstrap.py
├── examples/
│   └── python/
│       └── handler.py
└── entrypoint.sh
```

## 组件职责

`runtime/` 解决的是“容器内如何把一次平台请求交给用户代码执行”的问题。

### `runtime-server`

职责：

- 保存待执行调用队列。
- 让语言 bootstrap 通过 Runtime API 拉取事件。
- 接收 bootstrap 上报的成功或失败结果。
- 管理用户函数进程，崩溃后自动重启。

### `runtime-agent`

职责：

- 作为容器内对外入口，监听 `:9001`。
- 把 `/invoke` 转发给本机 `runtime-server`。
- 代理 `/health`。
- 统计调用数、成功数、错误数、延迟分位数。
- 周期性把指标上报给 gateway。

### `bootstrap/python3_bootstrap.py`

职责：

- 动态加载用户 `handler`。
- 轮询 Runtime API 获取事件。
- 调用 handler。
- 把结果或异常回传给 runtime-server。

## 进程关系

容器启动命令由 `runtime/entrypoint.sh` 控制：

```sh
/usr/local/bin/runtime-server &
exec /usr/local/bin/runtime-agent
```

也就是说：

1. `runtime-server` 先在后台启动。
2. `runtime-agent` 作为前台主进程启动。
3. `runtime-server` 再自己拉起 Python bootstrap 子进程。

最终在一个函数容器内，至少有三层角色：

- `runtime-agent`
- `runtime-server`
- `python3_bootstrap.py`
- 用户 `handler.py`

## runtime-server 详细说明

实现位于 `runtime/cmd/runtime/main.go`。

### 监听接口

- `GET /health`
- `POST /invoke`
- `GET /runtime/invocation/next`
- `POST /runtime/invocation/{requestId}/response`
- `POST /runtime/invocation/{requestId}/error`

### 关键结构

#### `invocation`

表示一次待执行调用：

- `id`
- `payload`
- `result chan invokeResult`

#### `invokeResult`

bootstrap 回传结果后，最终会通过它回到最初的 `/invoke` 请求：

- `statusCode`
- `body`

### 内部状态

- `queue []*invocation`
  - 等待 bootstrap 拉取的调用。
- `inflight sync.Map`
  - `requestID -> resultChan`。
- `notify chan struct{}`
  - 新调用到来时唤醒 `/runtime/invocation/next` 的阻塞轮询。

### 调用流程

#### `handleInvoke()`

1. 读取请求 body。
2. 生成一个 `requestID`。
3. 把调用塞入内存 `queue`。
4. 把 `requestID -> resultChan` 存入 `inflight`。
5. 等待 bootstrap 通过 `/response` 或 `/error` 回来。
6. 超时则返回 `504 function timeout`。

#### `handleNext()`

这是 bootstrap 的长轮询接口：

1. 如果 `queue` 有待执行调用，立即弹出。
2. 把 payload 返回给 bootstrap。
3. 通过 header 返回：
   - `Lambda-Runtime-Aws-Request-Id`
   - `Lambda-Runtime-Deadline-Ms`
4. 如果当前没有任务，就阻塞等待 `notify` 或超时。

#### `handleResponse()`

bootstrap 调用 `/runtime/invocation/{id}/response` 或 `/error` 时：

1. 从 URL 提取 `requestID` 和动作。
2. 从 `inflight` 中找到对应 `resultChan`。
3. 把结果写回 channel。
4. 最初卡在 `/invoke` 的 goroutine 拿到结果并响应客户端。

### `startFunction()`

这是 runtime-server 最关键的一段：

- 从环境变量拿到：
  - `FUNCTION_HANDLER`
  - `FUNCTION_RUNTIME`
  - `FUNCTION_DIR`
- 推导 bootstrap 路径，如 `/runtime/bootstrap/python3_bootstrap.py`。
- 启动 bootstrap 子进程。
- 子进程退出后等待 1 秒并自动重启。

这段代码体现了“平台进程”和“用户语言进程”分离的思想。

## runtime-agent 详细说明

实现位于 `runtime/cmd/agent/main.go`。

### 对外接口

- `POST /invoke`
- `GET /health`
- `GET /metrics`

### 为什么需要 agent

理论上 gateway 也可以直接访问 runtime-server，但这里单独放一个 agent，有几个好处：

- 可以把 Runtime API 与平台边界分开。
- 可以在 agent 层做 metrics、健康代理、未来的鉴权或 tracing。
- 更接近真实 FaaS 平台里“sidecar / shim / runtime adapter”的形态。

### `handleInvoke()`

1. 接收 gateway 送来的请求。
2. 记录开始时间。
3. 转发到 `http://localhost:9000/invoke`。
4. 计算延迟。
5. 调用 `record()` 更新指标窗口。
6. 把 runtime-server 返回的响应原样转发回 gateway。

### `handleHealth()`

- 实际上是在代理 `runtime-server` 的 `/health`。
- 带 3 次重试，每次间隔 200ms。
- 这样 scheduler 只要探测 agent，就能间接验证容器内部是否完整就绪。

### `handleMetrics()`

返回当前 metrics 快照，对调试很有用。

### 指标模型

核心结构是 `ContainerMetrics`：

- `InvocationCount`
- `SuccessCount`
- `ErrorCount`
- `P50LatencyMs`
- `P95LatencyMs`
- `P99LatencyMs`
- `MemoryBytes`
- `CPUUsageUs`

### 延迟统计

`agent.record()` 会把每次调用延迟放进 `latencies`，并保留最近 `1000` 次。

`snapshot()` 里会：

1. 复制延迟数组。
2. 排序。
3. 用 `percentile()` 取 p50 / p95 / p99。

这不是严格的 streaming quantile 算法，但足够适合这个学习项目。

### cgroup 指标读取

- `readMemoryBytes()` 读取 `/sys/fs/cgroup/memory.current`
- `readCPUUsageUs()` 读取 `/sys/fs/cgroup/cpu.stat`

这意味着：

- 代码更偏向 Linux / cgroup v2。
- 在 macOS 宿主机直接运行时可能拿不到完整值，但在容器里是合理的。

### 周期上报

`startReporter()` 每 10 秒做一次：

- 构造 `ContainerMetrics`
- POST 到 `http://{GATEWAY_ADDR}/containers/{containerID}/metrics`

这里的 `containerID`：

- 优先读 `CONTAINER_ID`。
- 否则尝试从 `/proc/self/cgroup` 猜。
- 再不行就退化成 hostname。

## Python bootstrap 详细说明

实现位于 `runtime/bootstrap/python3_bootstrap.py`。

### 启动阶段

1. 读环境变量：
   - `RUNTIME_API`
   - `FUNCTION_HANDLER`
   - `FUNCTION_DIR`
2. 把 `/function` 加入 `sys.path`。
3. 通过 `importlib.import_module()` 动态导入 handler。

### 主循环

bootstrap 的 `main()` 是个无限循环：

1. 请求 `GET /runtime/invocation/next`
2. 拿到 `request_id`、`deadline_ms`、事件 payload
3. 构造 `Context`
4. 调用用户 `handler(event, context)`
5. 成功时 POST `/response`
6. 失败时 POST `/error`

### `Context`

目前只给用户 handler 提供两个字段：

- `aws_request_id`
- `deadline_ms`

这个接口故意保持很小，方便后面继续扩展。

## 示例函数

`runtime/examples/python/handler.py` 是默认测试用函数。

它主要说明两件事：

- handler 需要是 `handler(event, context)` 形式。
- 用户代码可以从 event 里读业务参数，也能利用 `sleep_ms` 模拟慢调用，帮助测试 queue 与 autoscaling。

## 关键环境变量

### runtime-server

- `FUNCTION_HANDLER`
- `FUNCTION_RUNTIME`
- `FUNCTION_DIR`

### runtime-agent

- `CONTAINER_ID`
- `GATEWAY_ADDR`

### bootstrap

- `RUNTIME_API`
- `FUNCTION_HANDLER`
- `FUNCTION_DIR`

## 源码阅读建议

建议按顺序阅读：

1. `runtime/entrypoint.sh`
   - 先建立容器内进程模型。
2. `runtime/cmd/agent/main.go`
   - 看平台入口、指标采集、上报。
3. `runtime/cmd/runtime/main.go`
   - 看 Runtime API 和调用队列。
4. `runtime/bootstrap/python3_bootstrap.py`
   - 看语言层如何接入 Runtime API。
5. `runtime/examples/python/handler.py`
   - 看用户代码最小形态。

## 当前限制

- `runtime-server` 的调用队列只在内存里。
- timeout 逻辑仍比较粗，只是 gateway -> agent -> runtime 这一段的整体超时。
- bootstrap 只支持 Python，且只有同步 handler。
- 没有 dependency install、layer、init hook、streaming response 等能力。
- `startFunction()` 崩溃自动重启是无上限重试，适合学习，不适合生产直接照搬。
