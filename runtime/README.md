# Runtime Component

`runtime/` 是函数容器内部真正执行用户代码的部分。它把“平台控制逻辑”和“用户 handler 逻辑”拆成两个进程：

- `runtime-server`：实现最小版 Lambda Runtime API。
- `runtime-agent`：作为容器入口，代理调用、暴露健康检查与指标。

此外，这个目录还包含 Python、Go、Node.js、Java bootstrap，以及对应示例 handler。

## 目录结构

```text
runtime/
├── cmd/
│   ├── runtime/
│   │   └── main.go
│   ├── agent/
│   │   └── main.go
│   └── go-bootstrap/
│       └── main.go
├── bootstrap/
│   ├── python3_bootstrap.py
│   ├── nodejs_bootstrap.js
│   └── java/
│       └── JavaBootstrap.java
├── examples/
│   ├── python/
│   │   └── handler.py
│   ├── go/
│   │   └── main.go
│   ├── nodejs/
│   │   └── handler.js
│   └── java/
│       └── Hello.java
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

### 语言 bootstrap

职责：

- Python runtime 使用 `bootstrap/python3_bootstrap.py` 动态加载用户 `handler`。
- Go runtime 使用 `cmd/go-bootstrap` 执行用户提供的 `/function/bootstrap` 二进制。
- Node.js runtime 使用 `bootstrap/nodejs_bootstrap.js` 动态加载 CommonJS export。
- Java runtime 使用 `java-bootstrap.jar` 反射调用 `ClassName::methodName`。
- 轮询 Runtime API 获取事件。
- 调用用户 handler 或 bootstrap 进程。
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
3. `runtime-server` 根据 `FUNCTION_RUNTIME` 拉起对应语言 bootstrap 子进程。

最终在一个函数容器内，至少有三层角色：

- `runtime-agent`
- `runtime-server`
- 语言 bootstrap（Python、Go、Node.js 或 Java）
- 用户代码（Python handler、Go `/function/bootstrap`、Node.js handler 或 Java class）

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
- 根据 `FUNCTION_RUNTIME` 选择 bootstrap：当前支持 `python3 -> python3 /runtime/bootstrap/python3_bootstrap.py`，`go -> /runtime/bootstrap/go-bootstrap`，`nodejs -> node /runtime/bootstrap/nodejs_bootstrap.js`，`java -> java -cp /runtime/bootstrap/java-bootstrap.jar JavaBootstrap`。
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
- POST 到 `http://{GATEWAY_INTERNAL_ADDR}/containers/{containerID}/metrics`，未配置 `GATEWAY_INTERNAL_ADDR` 时回退到 `GATEWAY_ADDR`。
- 配置 `INTERNAL_API_TOKEN` 时携带 `Authorization: Bearer <token>`。

这里的 `containerID`：

- 优先读 `CONTAINER_ID`。
- 否则尝试从 `/proc/self/cgroup` 猜。
- 再不行就退化成 hostname。

## 语言 bootstrap 详细说明

Python 实现位于 `runtime/bootstrap/python3_bootstrap.py`，Go adapter 实现位于 `runtime/cmd/go-bootstrap/main.go`，Node.js 实现位于 `runtime/bootstrap/nodejs_bootstrap.js`，Java 源码位于 `runtime/bootstrap/java/JavaBootstrap.java` 并在镜像构建时打包为 `java-bootstrap.jar`。

### Python 启动阶段

1. 读环境变量：
   - `RUNTIME_API`
   - `FUNCTION_HANDLER`
   - `FUNCTION_DIR`
2. 把 `/function` 加入 `sys.path`。
3. 通过 `importlib.import_module()` 动态导入 handler。

### Go 启动阶段

1. 读 `FUNCTION_DIR`，默认 `/function`。
2. 执行用户上传包里的 `/function/bootstrap`。
3. 把 Runtime API 事件 JSON 写入子进程 stdin，并要求 stdout 返回 JSON。

### Node.js 启动阶段

1. 读环境变量：
   - `RUNTIME_API`
   - `FUNCTION_HANDLER`
   - `FUNCTION_DIR`
2. `FUNCTION_HANDLER=module.exportName` 会映射到 `/function/{module}.js` 的 CommonJS export。
3. 调用 `handler(event, context)`，同步返回和 Promise 都支持。
4. 如果 handler 返回非对象 JSON 值，会包装成 `{ "result": value }`。

### Java 启动阶段

1. 读环境变量：
   - `RUNTIME_API`
   - `FUNCTION_HANDLER`
   - `FUNCTION_DIR`
2. `FUNCTION_HANDLER=ClassName::methodName` 会从 `/function` classpath 加载类。
3. 反射调用 `public static String methodName(String eventJson)`。
4. 入参是原始事件 JSON 字符串，返回值必须是 JSON 字符串。

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

`runtime/examples/python/handler.py` 是默认 Python 测试函数，`runtime/examples/go/main.go` 展示 Go 函数如何编译成 `/function/bootstrap`，`runtime/examples/nodejs/handler.js` 展示 Node.js `handler.handler`，`runtime/examples/java/Hello.java` 展示 Java `Hello::handleRequest`。

示例主要说明四件事：

- Python handler 需要是 `handler(event, context)` 形式。
- Go 函数需要提供可执行 `bootstrap`，从 stdin 读取 JSON 并向 stdout 输出 JSON。
- Node.js handler 需要导出 `exports.handler = async function(event, context) { ... }` 这类函数。
- Java handler 需要提供 `public static String handleRequest(String eventJson)`，并返回 JSON 字符串；用较新 JDK 编译给当前镜像运行时使用时，应指定 `--release 21`；项目的 `test.sh` 会运行 `runtime/tests/java/JavaBootstrapJsonTest.java` 覆盖 Java bootstrap JSON 校验。
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
   - 看 Python 语言层如何接入 Runtime API。
5. `runtime/cmd/go-bootstrap/main.go`
   - 看 Go adapter 如何执行用户 bootstrap。
6. `runtime/bootstrap/nodejs_bootstrap.js`
   - 看 Node.js 如何动态加载 CommonJS handler。
7. `runtime/bootstrap/java/JavaBootstrap.java`
   - 看 Java 如何通过反射调用 handler。
8. `runtime/examples/*`
   - 看各语言用户代码最小形态。

## 当前限制

- `runtime-server` 的调用队列只在内存里。
- timeout 逻辑仍比较粗，只是 gateway -> agent -> runtime 这一段的整体超时。
- Python bootstrap 支持同步 handler；Node.js bootstrap 支持同步和 async handler；Go runtime 依赖用户提供可执行 `bootstrap` 并遵守 stdin/stdout JSON 协议；Java runtime 目前只支持 `public static String method(String eventJson)` 反射契约。
- 没有 dependency install、layer、init hook、streaming response 等能力。
- `startFunction()` 崩溃自动重启是无上限重试，适合学习，不适合生产直接照搬。
