# Gateway Component

`gateway/` 是整个系统的唯一用户入口，负责把“函数管理请求、函数调用请求、日志查询、指标转发、内部控制接口”组织成一个统一的 HTTP 进程。

它不是单纯的 API 路由器，而是把 HTTP 入口层和三个核心子模块串起来：

- `main.go`：进程启动与依赖装配。
- `entrypoints/`：HTTP 边界层，按入口类型拆分路由。
- `router/`：同步调用路径。
- `queue/`：每个函数独立的排队与背压。
- `scheduler/`：函数注册表、实例池、冷启动与回收。

## 组件职责

Gateway 主要负责五类事情：

1. 对外提供函数管理 API：注册、删除、上传代码、调用函数。
2. 对外提供日志查询 API，并把请求代理到内部 logdaemon。
3. 在调用入口做 per-function 限流和排队，避免无限冷启动。
4. 通过 scheduler 复用已有实例，或在没有空闲实例时触发冷启动。
5. 暴露给 `scalersvc` 与 `logdaemon proxy` 使用的内部 API。

## 目录结构

```text
gateway/
├── main.go
├── entrypoints/
│   ├── mux.go
│   ├── functions.go
│   ├── traffic.go
│   ├── autoscaling.go
│   └── coldstart.go
├── queue/
│   └── queue.go
├── router/
│   └── router.go
└── scheduler/
    ├── backend.go
    ├── scheduler.go
    ├── docker_backend.go
    └── k8s_backend.go
```

## 进程入口

入口在 `gateway/main.go`。

启动时会完成四件事：

1. 创建 `scheduler.New()`。
2. 创建 `router.New(sched)`。
3. 创建 `queue.New(r)`。
4. 调用 `entrypoints.NewMux(...)` 注册 HTTP 入口，再监听 `:8080`。

主监听地址固定是 `:8080`。

## HTTP 入口拆分

`entrypoints/` 只负责 HTTP 边界，不拥有调度、排队或运行时调用逻辑：

- `mux.go`：集中装配 `http.ServeMux`，让所有 route group 一眼可见。
- `functions.go`：函数注册、注销和代码上传入口。
- `traffic.go`：用户流量入口，包括 `/invoke/*`、`/queues/*`、`/logs/*` 和 `/health`。
- `autoscaling.go`：metrics ingress 和只读内部控制面查询。
- `coldstart.go`：会改变实例池状态的冷启动/缩容入口。

把 `coldstart.go` 从 `autoscaling.go` 拆出来，是因为 `/internal/scale-up/*` 和 `/internal/scale-down/*` 会触发实例创建或停止，而 stats / containers / queue 只是内部观测接口。

## 外部接口

### 函数管理

- `POST /functions/{name}`
  - 注册函数。
  - body 对应 `scheduler.FunctionConfig`。
- `DELETE /functions/{name}`
  - 注销函数，并异步停止这个函数已有实例。
- `PUT /functions/{name}/code`
  - 上传 zip 代码包，最大 50MB。
  - 代码会被解压到 `/tmp/faas/{name}`；配置 MinIO 时还会保存 zip 对象供 Kubernetes Pod 拉取。

### 函数调用

- `POST /invoke/{name}`
  - 先进入 `queue.Manager.Invoke()`。
  - 被允许执行后，再进入 `router.Router.Invoke()`。

### 队列与健康检查

- `GET /queues/{name}`
  - 返回某个函数当前的 `in_flight`、`queued`、阈值信息。
- `GET /health`
  - 仅返回 `ok`。

### 日志查询

- `GET /logs/{name}`
  - 用户查询函数日志的入口。
  - gateway 代理到内部 logdaemon，并保留 `tail`、`stream` 等查询参数。
- `GET /logs/{name}/stream`
  - 用户查询 SSE 日志流的入口。
  - gateway 代理到内部 logdaemon，并把 event stream 持续 flush 给客户端。

### 指标入口

- `POST /containers/{id}/metrics`
  - 仅注册在 internal listener 上，接收 runtime-agent 上报的容器指标。
  - 如果配置了 `SCALER_ADDR`，会异步转发到 `scalersvc`。

## 内部接口

这些接口给本机的 `scalersvc`、`logdaemon`、`mqsvc` 和 runtime-agent 使用；配置 `INTERNAL_API_TOKEN` 后需要 `Authorization: Bearer <token>`。

### 给 scalersvc 用

- `GET /internal/functions`
  - 返回全部已注册函数名。
- `GET /internal/stats/{funcName}`
  - 返回 busy / idle 数量。
- `GET /internal/containers/{funcName}`
  - 返回该函数当前实例 ID 列表。
- `GET /internal/queue/{funcName}`
  - 返回该函数队列状态。
- `POST /internal/scale-up/{funcName}`
  - 触发预热一个实例。
- `POST /internal/scale-down/{funcName}`
  - 尝试移除一个最旧 idle 实例。

### 给 logdaemon proxy 用

- `GET /internal/instances/{funcName}`
  - 返回函数实例与 `nodeName` 映射。
  - 这是 k8s 日志 proxy 做“按函数找到对应 collector”路由的关键接口。

## 核心调用链路

### 1. 注册函数

- `functions.go` 解析 `POST /functions/{name}`。
- JSON body 解码为 `scheduler.FunctionConfig`。
- 调用 `sched.Register(cfg)` 写入注册表；配置 Mongo 时会同步持久化函数元数据。

### 2. 上传代码

- `functions.go` 读取 zip body。
- 调用 `sched.UploadCode(name, data)`。
- scheduler 会：
  - 解压到 `/tmp/faas/{name}`，作为 Docker/local 路径和 Kubernetes fallback。
  - 配置 MinIO 时保存 zip 对象，并更新 `FunctionConfig.CodeKey` / `CodeURL`。
  - 更新 `FunctionConfig.CodeDir`。
  - 持久化元数据后停掉旧实例，确保下次调用使用新代码。

### 3. 调用函数

- `/invoke/{name}` 进入 `queue.Manager.Invoke()`。
- 如果有空闲 slot，直接调用 router。
- 如果 slot 用满但队列未满，进入等待。
- router 从 scheduler 获取实例。
- scheduler 优先复用 idle；没有 idle 时走后端冷启动。
- router 将请求转发到实例内 `runtime-agent` 的 `/invoke`。
- 调用完成后 `defer sched.Release(c)`，实例重新变成 idle。

## queue 子模块

实现位于 `gateway/queue/queue.go`。

### 设计目的

不是全局队列，而是“按函数名隔离”的轻量限流器。这样单个热点函数不会把其他函数全部拖死。

### 关键结构

- `Manager`
  - 保存路由器、默认阈值、以及每个函数对应的 `functionQueue`。
- `functionQueue`
  - `sem chan struct{}`：并发执行槽位。
  - `queued int`：当前等待中的请求数。

### 关键逻辑

`Manager.Invoke()` 分三段：

1. 尝试直接占用 `sem`。
2. 如果失败，再检查 `queued >= maxQueue`。
3. 未超限则进入等待，并在：
   - 等到 slot 时继续调用。
   - 超时后返回 `503 queue timeout`。
   - 客户端取消时直接退出。

### 关键配置

- `MAX_INFLIGHT_PER_FUNCTION`
- `MAX_QUEUE_PER_FUNCTION`
- `QUEUE_TIMEOUT_MS`

## router 子模块

实现位于 `gateway/router/router.go`。

### 职责

router 不做函数生命周期管理，只做“一次同步请求”的完整转发。

### `Router.Invoke()` 流程

1. `sched.Get(name)` 查函数配置。
2. 读取用户请求 body。
3. `sched.Acquire(name)` 取实例。
4. 以函数 timeout 为上限创建 context。
5. POST 到 `http://{containerAddr}/invoke`。
6. 把 agent 返回的 `InvokeResponse` 解包后写回客户端。

### 关键点

- router 依赖 scheduler 提供已就绪的地址，不关心后端是 Docker 还是 Kubernetes。
- 实例释放使用 `defer`，保证正常和异常路径都能回收 busy 状态。

## scheduler 子模块

实现位于 `gateway/scheduler/`。

### 核心职责

1. 存函数注册表。
2. 维护每个函数的实例池。
3. 复用 idle 实例。
4. 在需要时触发冷启动。
5. 定期回收长时间未使用的实例。
6. 给 scaler / log proxy 提供实例元数据。

### 关键结构体

#### `FunctionConfig`

定义在 `gateway/scheduler/scheduler.go`。

重要字段：

- `Name`
- `Image`
- `Timeout`
- `Runtime`
- `Memory`
- `Handler`
- `CodeDir`
- `Port`

#### `container`

内部实例对象，不直接暴露。

重要字段：

- `id`
- `addr`
- `state`
- `lastUsed`
- `funcName`
- `nodeName`

#### `RuntimeInstance`

定义在 `gateway/scheduler/backend.go`。

它是后端返回给 scheduler 的统一实例描述：

- `ID`
- `Addr`
- `FuncName`
- `NodeName`

### 关键方法

#### `Register()`

- 补默认值。
- 写入 `functions` map。
- 不立即启动实例。

#### `UploadCode()`

- 把 zip 写到临时目录。
- 调 `unzip -o` 解压。
- 更新 `CodeDir`。
- 清空该函数实例池并异步 stop 旧实例。

#### `Acquire()`

- 先在池里找 `stateIdle`。
- 找不到就分配新端口并 `start()`。
- 冷启动成功后放入池中。

#### `Release()`

- 仅改状态为 idle，并更新 `lastUsed`。

#### `ColdStartOne()`

- 给 scaler 的预热接口使用。
- 后台冷启动一个实例，加入池时状态直接是 idle。

#### `RemoveIdle()`

- 找最旧的 idle 实例。
- 只有空闲超过 2 分钟才会真正缩掉。

#### `reaper()`

- 每 30 秒扫描一次。
- 超过 5 分钟未使用的 idle 实例会被后台清理。

### 运行后端抽象

通过 `RuntimeBackend` 隔离 Docker 与 Kubernetes 差异：

- `docker_backend.go`
- `k8s_backend.go`

#### Docker backend

`docker_backend.go` 会：

- 通过 Docker Engine HTTP API 创建实例。
- 把宿主机端口映射到容器 `9001`。
- 注入 `FUNCTION_HANDLER`、`GATEWAY_ADDR` 等环境变量。
- 用 `waitReady()` 轮询 agent `/health`。

#### Kubernetes backend

`k8s_backend.go` 会：

- 优先用 MinIO presigned `CodeURL` 让 initContainer 下载并解压代码到 `/function`。
- 没有 `CodeKey` 时回退到 minikube hostPath 代码同步。
- 动态生成 Pod manifest。
- 通过 client-go 创建 Pod 并等待 Ready。
- 通过 SPDY port-forward 暴露 agent 到宿主机端口。
- 再读取 `spec.nodeName`，写回 `RuntimeInstance.NodeName`。

这个 `nodeName` 是日志系统的重要桥梁，因为 `logdaemon proxy` 要靠它把函数实例映射到对应 node 上的 collector。

## 关键环境变量

### gateway 进程

- `SCALER_ADDR`
  - metrics 转发目标，默认空。
- `LOGDAEMON_ADDR`
  - 日志代理上游地址，默认 `localhost:9200`。

### scheduler / backend

- `FAAS_BACKEND`
  - `docker` 或 `k8s`。
- `K8S_NAMESPACE`
- `GATEWAY_ADDR`
  - 容器内 agent 上报 metrics 时访问宿主机 gateway 的地址。

### queue

- `MAX_INFLIGHT_PER_FUNCTION`
- `MAX_QUEUE_PER_FUNCTION`
- `QUEUE_TIMEOUT_MS`

## 源码阅读建议

建议按下面顺序看：

1. `gateway/main.go`
   - 看进程启动、依赖装配和 `entrypoints.NewMux(...)`。
2. `gateway/entrypoints/mux.go`
   - 看 HTTP route group 如何按入口类型装配。
3. `gateway/entrypoints/functions.go`
   - 看函数注册、注销和代码上传入口。
4. `gateway/entrypoints/traffic.go`
   - 看用户流量、队列状态、健康检查和日志代理入口。
5. `gateway/entrypoints/autoscaling.go`
   - 看 metrics ingress 和只读内部控制面查询。
6. `gateway/entrypoints/coldstart.go`
   - 看触发冷启动和缩容的内部 mutation 入口。
7. `gateway/queue/queue.go`
   - 看入口背压。
8. `gateway/router/router.go`
   - 看单次调用路径。
9. `gateway/scheduler/scheduler.go`
   - 看函数注册表、实例池、冷启动与回收。
10. `gateway/scheduler/docker_backend.go`
    - 看 Docker 冷启动。
11. `gateway/scheduler/k8s_backend.go`
    - 看 Kubernetes 冷启动、initContainer 代码分发与 SPDY port-forward。

## 当前限制

- 未配置 `MONGO_URI` 时函数元数据仍只在内存里；Mongo 持久化不代表运行中的容器/Pod 会自动恢复。
- queue 只做简单 per-function 阈值，没有优先级和公平调度。
- scheduler 仍面向本地学习环境，Docker/Kubernetes 后端缺少生产级隔离、配额和鉴权。
- k8s backend 当前依赖 Pod port-forward 连接宿主机 gateway，还不是生产形态的数据面。
