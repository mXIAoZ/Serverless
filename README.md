# Local Serverless / FaaS Learning Project

这是一个用于学习 AWS Lambda 架构思想的本地 FaaS 系统。项目从最小 gateway 开始，逐步加入 Docker/Kubernetes 运行后端、Runtime API、函数代码上传、runtime-agent、日志 daemon、独立 autoscaler，以及 gateway 入口层的请求队列和背压。

当前实现目标不是生产可用，而是把一个 serverless 平台的关键控制面和数据面拆开，方便理解每个组件为什么存在、它们之间如何协作，以及后续要补哪些能力才能接近主流开源 FaaS 平台。

## 这是什么

这个项目适合用来理解一条完整的本地 FaaS 调用链路：

- gateway 如何接收函数管理与调用请求
- scheduler 如何复用实例、触发冷启动、管理双后端
- runtime-agent 和 runtime-server 如何把平台请求交给用户 handler
- scalersvc 如何根据 queue / p99 / error rate 做扩缩容判断
- gateway 如何统一承载函数管理、调用和日志查询入口

如果你把它当成“一个可读、可跑、可逐步扩展的 serverless 平台骨架”会比较合适。

## 快速开始

### Docker 后端

```bash
cd /Users/z/serverless
./start.sh
./test.sh
./stop.sh
```

### Kubernetes / minikube 后端

```bash
minikube start
cd /Users/z/serverless
PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./start.sh
PATH="$PATH:$HOME/.local/bin" MAX_REPLICAS=10 FAAS_BACKEND=k8s ./test.sh
PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./stop.sh
```

如果你只想先读懂项目，建议先看：

- [`LEARNING_PATH.md`](LEARNING_PATH.md)
- [`gateway/README.md`](gateway/README.md)
- [`runtime/README.md`](runtime/README.md)

## 当前能力

- 函数注册、注销、代码 zip 上传。
- Docker 或 Kubernetes Pod 作为函数运行沙箱。
- 容器内实现 Lambda 风格 Runtime API。
- Python、Go、Node.js、Java 四种 runtime：Python/Node.js 动态加载 handler，Go 执行 `/function/bootstrap`，Java 通过 `ClassName::methodName` 反射调用。
- runtime-agent 负责请求转发、健康检查、指标采集和周期上报。
- gateway 提供统一日志查询入口，logdaemon 在内部负责 Docker 日志采集，Kubernetes 下采用 node-local collector + host proxy 查询链路。
- scalersvc 独立运行，收集 agent 指标并按默认策略扩缩容。
- gateway 内置 per-function 请求队列和背压。
- MQ 触发器支持 RabbitMQ，通过独立 `mqsvc`、gateway 实例租约 API 和 runtime `/events` 入口完成 ack/retry/DLQ 链路。
- 本地一键启动、停止和端到端测试脚本。

## 先看哪里

### 想先运行起来

- [`SCRIPTS.md`](SCRIPTS.md)
- `./start.sh`
- `./test.sh`
- `./stop.sh`

### 想先理解整体架构

- 当前架构图
- 请求调用链路
- 指标与扩缩容链路
- 日志采集链路

### 想直接读源码主线

- [`gateway/main.go`](gateway/main.go)
- [`gateway/scheduler/scheduler.go`](gateway/scheduler/scheduler.go)
- [`runtime/cmd/agent/main.go`](runtime/cmd/agent/main.go)
- [`runtime/cmd/runtime/main.go`](runtime/cmd/runtime/main.go)
- [`scalersvc/main.go`](scalersvc/main.go)
- [`logdaemon/shared.go`](logdaemon/shared.go)

## 文档索引

### 组件文档

- [`gateway/README.md`](gateway/README.md)：gateway 入口、queue、router、scheduler、双后端实现。
- [`runtime/README.md`](runtime/README.md)：runtime-server、runtime-agent、多语言 bootstrap、容器内调用链路。
- [`scalersvc/README.md`](scalersvc/README.md)：指标接收、扩缩容策略、决策循环、内部接口协作。
- [`logdaemon/README.md`](logdaemon/README.md)：docker / collector / proxy 三种模式与日志查询链路。

### 学习与运维文档

- [`LEARNING_PATH.md`](LEARNING_PATH.md)：按总览 → gateway → runtime → scaler → logdaemon → scripts 的顺序阅读整个项目。
- [`SCRIPTS.md`](SCRIPTS.md)：`start.sh`、`stop.sh`、`test.sh` 的职责、执行流程、环境变量与排障说明。

## 目录结构

```text
serverless/
├── gateway/
│   ├── main.go                 # HTTP API 入口，连接 router、scheduler、queue、scalersvc
│   ├── queue/queue.go          # per-function 请求队列与背压
│   ├── scheduler/scheduler.go  # 函数注册表、容器池、冷启动、释放、回收
│   └── scheduler/*_backend.go  # Docker / Kubernetes 双后端
├── runtime/
│   ├── cmd/runtime/main.go     # 容器内 Runtime API server
│   ├── cmd/agent/main.go       # 容器内 runtime-agent
│   ├── bootstrap/python3_bootstrap.py
│   ├── bootstrap/nodejs_bootstrap.js
│   ├── bootstrap/java/JavaBootstrap.java
│   ├── examples/python/handler.py
│   ├── examples/go/main.go
│   ├── examples/nodejs/handler.js
│   ├── examples/java/Hello.java
│   └── entrypoint.sh
├── mqsvc/                      # 独立 MQ 触发服务，当前支持 RabbitMQ
├── scalersvc/main.go           # 独立自动扩缩容服务
├── logdaemon/main.go           # 独立日志采集 daemon
├── Dockerfile                  # runtime 镜像
├── start.sh                    # 构建并启动完整系统
├── test.sh                     # 端到端测试
└── stop.sh                     # 停止服务和函数容器
```

## 当前架构图

![当前架构图](docs/diagrams/system-architecture.svg)

## 请求调用链路

![请求调用链路](docs/diagrams/invocation-sequence.svg)

## 指标与扩缩容链路

![指标与扩缩容链路](docs/diagrams/metrics-scaling.svg)

## 日志采集链路

![日志采集链路](docs/diagrams/logging-path.svg)

## MQ 触发器设计

当前项目支持 RabbitMQ MQ 触发器。MQ 不嵌入 gateway，而是由独立的 `mqsvc` 运行：`mqsvc` 消费 RabbitMQ 消息后，通过 gateway internal lease API 获取函数实例和 runtime-agent 地址，再直接把 MQ 事件转发给函数实例。NATS/Kafka 仍是未来扩展方向。

目标调用链：

```text
broker message -> mqsvc -> gateway acquire instance lease -> runtime-agent /events -> runtime-server -> language bootstrap -> handler -> gateway release lease
```

### 设计目标

- `mqsvc` 是独立服务，第一版已接入 RabbitMQ；NATS/Kafka 后续可通过 broker interface 扩展。
- gateway 不消费 MQ，只作为控制面，负责函数校验、实例分配和租约释放。
- runtime 原生支持 MQ 事件，避免把 MQ 消息伪装成普通 HTTP invoke。
- MQ 调用和 HTTP 调用共享 scheduler 的实例池、busy/idle 状态和冷启动逻辑。
- 第一版采用 at-least-once 语义，handler 需要基于消息 ID 做幂等。

### ID 模型

MQ 触发链路需要明确区分几个 ID：

- `mq_id`：MQ 实例 ID，例如 `rabbitmq-main`、`rabbitmq-payment`，用于区分多个 broker 实例。
- `trigger_id`：触发器 ID，例如 `orders-trigger`，用于区分同一个 MQ 实例上的多条触发规则。
- `message_id`：broker 消息 ID，用于幂等、日志、重试和 DLQ 追踪。
- `lease_id`：gateway/scheduler 返回的函数实例租约 ID，用于 release，防止错释放或重复释放。
- `instance_id`：具体函数实例 ID，例如容器 ID 或 Pod ID。

`mq_id` 是必须字段，不应只用 `broker: rabbitmq` 表示 MQ 来源。后续如果同时接入多个 RabbitMQ、NATS 或 Kafka 集群，`mq_id` 才能稳定标识具体实例。

### 独立 mqsvc

新增独立 binary `mqsvc`，建议目录结构：

```text
mqsvc/
├── main.go
├── config.go
├── gateway_client.go
├── worker.go
├── broker.go
└── rabbitmq.go
```

`mqsvc` 负责：

- 加载 MQ 实例配置，例如 `mq_id -> broker type/url/credentials`。
- 加载 trigger 配置，例如 `trigger_id -> mq_id/queue/function`。
- 连接 broker 并订阅 queue/topic。
- 控制每个 trigger 的最大并发和 broker prefetch。
- 消费消息后先向 gateway acquire 函数实例 lease。
- 直接调用 runtime-agent 的 `/events`。
- 调用结束后释放 lease。
- 根据执行结果 ack、nack、retry 或写入 DLQ。
- 暴露 `/health`、`/metrics`、`/triggers` 等状态接口。

当前实现支持 RabbitMQ：手动 ack、prefetch、按 trigger 并发控制、retry/backoff、publisher confirm 和 DLQ。

### Gateway 内部租约 API

MQ 服务不应该只向 gateway “拿 IP”。如果只返回实例地址，scheduler 无法可靠维护 busy/idle 状态，也无法避免同一个实例被多个调用同时复用。

gateway 同时监听 public 和 internal mux：public 默认 `:8080`，internal 在源码中默认 `127.0.0.1:8081`；`start.sh` 为了让本地容器访问内部接口，会默认设置 `GATEWAY_INTERNAL_LISTEN=:8081`。internal API 可通过 `INTERNAL_API_TOKEN` 开启 `Authorization: Bearer <token>` 校验；本地脚本默认使用 `local-internal-token`。MQ 使用基于 lease 的 internal API。

Acquire：

```text
POST /internal/functions/{name}/instances/acquire
```

请求示例：

```json
{
  "source": "mq",
  "mq_id": "rabbitmq-main",
  "trigger_id": "orders-trigger",
  "message_id": "msg-123",
  "timeout_seconds": 30
}
```

响应示例：

```json
{
  "lease_id": "lease-abc",
  "function": "order-handler",
  "instance_id": "container-or-pod-id",
  "address": "127.0.0.1:9101",
  "timeout_seconds": 30
}
```

Release：

```text
POST /internal/leases/{lease_id}/release
```

请求示例：

```json
{
  "status": "success"
}
```

`status` 可选值：

- `success`
- `error`
- `timeout`
- `abandoned`

Release 使用 `lease_id`，而不是再次传 function 和 instance path 参数，避免 mqsvc 传入的信息和 gateway 记录的租约不一致。

### 为什么必须显式 release

现有 HTTP 调用链里，gateway 自己负责转发 runtime 请求，所以 gateway 可以在请求结束后调用 scheduler 释放实例。MQ 架构里，runtime 请求由 `mqsvc` 直接发给 runtime-agent，gateway 不知道调用什么时候结束。

如果没有显式 release，会出现这些问题：

- scheduler 可能一直认为实例 busy，导致实例无法复用。
- scheduler 也可能不知道实例 busy，导致多个调用错误复用同一个实例。
- HTTP 调用和 MQ 调用无法公平共享实例池。
- autoscaler 会拿到错误的 busy/idle 状态。
- timeout/error 后无法判断实例应该归还、标记异常还是回收。
- idle reaper 无法准确判断实例是否空闲。

因此 MQ 调用必须遵循：

```text
acquire lease -> use runtime-agent address -> release lease
```

### Scheduler 变更

`gateway/scheduler/scheduler.go` 需要暴露安全的实例租约类型和方法，避免把内部 container 指针直接暴露给 gateway handler。

示例类型：

```go
type InstanceLease struct {
    LeaseID        string
    Function       string
    InstanceID     string
    Address        string
    TimeoutSeconds int
    Source         string
    MQID           string
    TriggerID      string
    MessageID      string
}
```

示例方法：

```go
AcquireInstance(req AcquireInstanceRequest) (InstanceLease, error)
ReleaseInstance(leaseID string, status string) error
```

第一版可以把 lease 保存在 gateway/scheduler 内存中。gateway 重启导致运行中 lease 丢失时，可先依赖实例健康检查或 idle reaper 恢复，后续再考虑持久化 lease。

### MQ 实例配置

MQ 实例配置和 trigger 配置分开。MQ 实例描述 broker 连接，trigger 描述哪个队列触发哪个函数。

MQ 实例示例：

```json
{
  "mq_id": "rabbitmq-main",
  "broker": "rabbitmq",
  "url": "amqp://guest:guest@localhost:5672/"
}
```

注意：

- `mq_id` 是稳定 ID，不等于 broker 类型。
- broker 凭证不应暴露给普通函数配置响应。
- 本地第一版可以从环境变量或 `MQ_CONFIG_PATH` 指向的 mqsvc 配置文件读取。
- 后续如果需要 UI/API 管理，再把 MQ instance registry 放到 gateway 或 mqsvc 的持久化存储里。

### Trigger 配置

第一版可以先把 trigger 配置放进函数配置，每个 trigger 必须引用 `mq_id`。

示例：

```json
{
  "runtime": "python3",
  "handler": "handler.handler",
  "triggers": [
    {
      "id": "orders-trigger",
      "type": "mq",
      "enabled": true,
      "mq_id": "rabbitmq-main",
      "queue": "orders",
      "max_concurrency": 10,
      "prefetch": 10,
      "max_attempts": 3,
      "retry_backoff_ms": 1000,
      "dlq": "orders.dlq"
    }
  ]
}
```

后续如果 trigger 生命周期要和函数生命周期分离，可以再新增独立 API：

- `GET /triggers`
- `POST /triggers`
- `PUT /triggers/{id}`
- `DELETE /triggers/{id}`

### Runtime MQ 事件入口

runtime-agent 新增 typed event endpoint：

```text
POST /events
```

`/invoke` 保持现有 HTTP 调用兼容，`/events` 用于 MQ、未来 cron、对象存储事件等 typed event。

MQ event envelope 示例：

```json
{
  "version": "1.0",
  "type": "mq",
  "source": "rabbitmq",
  "mq_id": "rabbitmq-main",
  "trigger_id": "orders-trigger",
  "id": "msg-123",
  "time": "2026-05-28T00:00:00Z",
  "subject": "orders",
  "headers": {
    "content-type": "application/json"
  },
  "body": {
    "orderId": "o-123"
  }
}
```

如果原始消息不是 JSON：

```json
{
  "version": "1.0",
  "type": "mq",
  "source": "rabbitmq",
  "mq_id": "rabbitmq-main",
  "trigger_id": "orders-trigger",
  "id": "msg-123",
  "subject": "orders",
  "bodyBase64": "...",
  "isBase64Encoded": true
}
```

runtime-server 的 invocation 结构需要增加 event type、headers 和 deadline。Python、Node.js、Java、Go bootstrap 第一版都可以直接把 envelope 作为 handler event 输入。

### 交付语义

第一版明确采用 at-least-once：

- runtime 成功：ack。
- gateway acquire 失败：不 ack，延迟重试或 nack requeue。
- runtime 连接失败：retry。
- runtime timeout：retry。
- runtime 5xx：retry。
- 超过 `max_attempts`：发 DLQ。
- 函数不存在或 trigger 配置非法：视为不可重试错误，停用 trigger 或进入 DLQ。
- 只要 acquire 成功，无论 runtime 成功还是失败，`mqsvc` 都必须尽力 release lease。

不保证 exactly-once。函数 handler 必须基于 `message_id` 做幂等，MQ 消息也可能被重复投递。

### 扩缩容和背压

因为 MQ 是独立服务，不能让它无限消费：

- `mqsvc` 每个 trigger 有 `max_concurrency`。
- broker prefetch 不应超过并发上限太多。
- acquire gateway 实例失败时，`mqsvc` 应该 backoff，不能热循环打 gateway。
- scheduler acquire 后实例会被标记 busy，因此 MQ 和 HTTP 会共享实例池容量。

后续可把 MQ backlog 接入 scaler：

- `mqsvc` 暴露每个 `mq_id` / `trigger_id` 的 broker backlog、in-flight、retry、DLQ 指标。
- scalersvc 查询 mqsvc metrics。
- 扩容时考虑 HTTP queue backlog + MQ backlog。

第一版可以先靠 `mqsvc` 并发限制和 scheduler 冷启动跑通，第二阶段再做 autoscaler 深度集成。

### 实施顺序

1. Gateway 已增加实例 lease API：`POST /internal/functions/{name}/instances/acquire` 和 `POST /internal/leases/{lease_id}/release`。
2. Runtime 已增加 MQ event 入口：runtime-agent 和 runtime-server 均支持 `/events`，各语言 bootstrap 接收统一 JSON envelope。
3. `mqsvc` 已实现 MQ instance config、gateway trigger sync、worker loop、metrics、broker interface。
4. RabbitMQ adapter 已实现手动 ack、prefetch、max attempts、DLQ、retry backoff 和 publisher confirms。
5. Trigger 配置已进入 `FunctionConfig` 并随 Mongo function metadata 持久化；`mqsvc` 周期拉取 `GET /internal/triggers`。
6. `start.sh` 支持 `MQ_ENABLED=1` 启动 RabbitMQ/mqsvc，`test.sh` 支持 RabbitMQ ack/retry/DLQ smoke。

### 验证计划

- `go test ./gateway/scheduler ./runtime/cmd/agent ./runtime/cmd/runtime ./mqsvc`
- `go test ./...`
- `bash -n start.sh stop.sh test.sh`
- Docker 本地 smoke：`./start.sh && ./test.sh && ./stop.sh`
- MQ smoke：`MQ_ENABLED=1 ./start.sh && MQ_ENABLED=1 ./test.sh && MQ_ENABLED=1 ./stop.sh`；脚本会启动 gateway、runtime image、mqsvc、RabbitMQ，注册 `mq_id=rabbitmq-main` 的 RabbitMQ trigger，publish 成功/失败消息，并验证 ack、release、retry 和 DLQ。

## 核心组件说明

### Gateway

Gateway 是系统唯一用户入口，监听 `:8080`，负责对外暴露函数管理、调用和日志查询接口：

- `POST /functions/{name}`：注册函数。
- `DELETE /functions/{name}`：注销函数并停止相关容器。
- `PUT /functions/{name}/code`：上传 zip 代码包。
- `POST /invoke/{name}`：调用函数。
- `GET /queues/{name}`：查看函数请求队列状态。
- `GET /logs/{funcName}`：用户查询函数日志，gateway 同步代理到内部 logdaemon。
- runtime-agent 指标入口 `POST /containers/{id}/metrics` 仅注册在 internal listener 上，接收指标并转发给 scalersvc。
- public listener 不暴露 `/internal/*` 或 metrics ingest；内部控制接口在 `GATEWAY_INTERNAL_LISTEN`，源码默认 `127.0.0.1:8081`，`start.sh` 本地默认 `:8081`。

### Queue / Backpressure

`gateway/queue` 在 gateway 入口处按函数名做隔离：

- `MAX_INFLIGHT_PER_FUNCTION`：单函数最大并发执行数，默认 `5`。
- `MAX_QUEUE_PER_FUNCTION`：单函数最大排队数，默认 `100`。
- `QUEUE_TIMEOUT_MS`：请求排队最长等待时间，默认 `30000`。

行为：

- 有空闲 in-flight slot：立即进入 router。
- in-flight 满但队列未满：进入等待队列。
- 队列满：返回 `429 queue full`。
- 等待超时：返回 `503 queue timeout`。

这个组件对应 Knative queue-proxy / API gateway backpressure 的简化版，目的是防止 gateway 无限堆积请求或无限触发容器冷启动。

### Router

Router 是同步调用路径：

1. 检查函数是否注册。
2. 读取请求 body。
3. 调用 scheduler 获取容器。
4. 将请求转发到容器内 runtime-agent 的 `/invoke`。
5. 将 handler 结果返回给客户端。
6. 调用 scheduler 释放容器。

### Scheduler

Scheduler 管理函数注册表和容器池：

- 默认 runtime 镜像：`faas-runtime:latest`。
- 默认 handler：`handler.handler`。
- 默认 timeout：`30s`。
- 默认 memory：`128MB`。
- idle 容器优先复用。
- 没有 idle 容器时通过当前 RuntimeBackend 冷启动 Docker 容器或 Kubernetes Pod。
- 上传新代码后停止旧容器，下次调用使用新代码。
- 定期回收长时间 idle 容器。

### RuntimeBackend

Scheduler 通过 `RuntimeBackend` 抽象创建和停止函数实例，目前支持两种后端：

- `FAAS_BACKEND=docker`：默认后端，通过 Docker Engine HTTP API 创建本地容器，并映射到宿主机端口。
- `FAAS_BACKEND=k8s`：Kubernetes 后端，面向 minikube，通过 client-go 创建 Pod，并用 SPDY port-forward 让 gateway 访问 Pod 内 runtime-agent。

上传代码会先解压到本地 `/tmp/faas/{name}`。当配置 MinIO 时，scheduler 还会保存 zip 对象并记录 `CodeKey`/`CodeURL`；Kubernetes 后端优先用 initContainer 从 presigned URL 下载代码并解压到 `/function`，只有没有对象存储信息时才回退到旧的 minikube hostPath 同步路径。Scheduler 还会记录实例所在 `nodeName`，供 logdaemon proxy 按函数实例路由到对应 node collector。

### Runtime API Server

每个函数容器内运行 `runtime-server`，监听 `:9000`，实现 Lambda 风格 Runtime API：

- `POST /invoke`：接收 agent 转发的 HTTP 调用请求。
- `POST /events`：接收 agent 转发的 MQ typed event。
- `GET /runtime/invocation/next`：语言 bootstrap 长轮询获取下一次调用。
- `POST /runtime/invocation/{requestId}/response`：bootstrap 上报成功结果。
- `POST /runtime/invocation/{requestId}/error`：bootstrap 上报错误结果。

Runtime server 会启动用户函数 bootstrap 进程，并在进程退出后自动重启。

### Python Bootstrap

Python bootstrap 负责：

1. 读取 `FUNCTION_HANDLER`，如 `handler.handler`。
2. 将 `/function` 加入 `sys.path`。
3. 动态 import 用户 handler。
4. 长轮询 Runtime API 获取事件。
5. 执行 `handler(event, context)`。
6. 将响应或错误发回 Runtime API。

### Runtime Bootstraps

runtime-server 会根据 `FUNCTION_RUNTIME` 选择语言 bootstrap：

- `python3`：运行 `python3 /runtime/bootstrap/python3_bootstrap.py`，加载 `handler.handler` 这类 Python 函数。
- `go`：运行 `/runtime/bootstrap/go-bootstrap`，再执行用户上传的 `/function/bootstrap` 二进制。
- `nodejs`：运行 `node /runtime/bootstrap/nodejs_bootstrap.js`，加载 `module.exportName` 这类 CommonJS handler。
- `java`：运行 `java -cp /runtime/bootstrap/java-bootstrap.jar JavaBootstrap`，通过 `ClassName::methodName` 反射调用 `public static String method(String eventJson)`。

### Runtime Agent

runtime-agent 运行在每个函数容器内，监听 `:9001`，是 gateway 访问容器的入口：

- 代理 `POST /invoke` 到 runtime server。
- 代理 `GET /health` 到 runtime server。
- 暴露 `GET /metrics`。
- 记录 invocation count、success、error、p50/p95/p99 latency。
- 读取 cgroup v2 memory 和 cpu 使用量。
- 每 10 秒向 gateway 上报指标。

### ScalerSvc

scalersvc 独立运行，监听 `:9300`，通过 HTTP REST 与 gateway 通信：

- `POST /metrics/{containerID}`：接收 gateway 转发的 agent 指标。
- `GET /scale/{funcName}`：查看扩缩容状态。
- `GET /health`：健康检查。

默认策略：

- scale-up：任一条件满足即可扩容。
  - utilization >= `80%`
  - p99 latency > `500ms`
  - error rate > `10%`
- scale-down：条件同时满足才缩容。
  - utilization < `20%`
  - p99 latency < `100ms` 或没有指标
  - idle replicas 大于 min replicas

### LogDaemon

logdaemon 是内部日志采集服务，监听 `:9200` 供 gateway 调用；用户通过 gateway 的 `GET /logs/{funcName}` 查询日志。它按后端分两种实现：

- `FAAS_BACKEND=docker`：单进程 docker mode，通过 Docker socket 收集日志。
- `FAAS_BACKEND=k8s`：宿主机运行 proxy mode，Kubernetes 内每个 node 跑一个 collector mode DaemonSet。

Docker 模式下：

- 监听带 `faas.function` label 的 Docker container events。
- 启动时也会 attach 已存在的 FaaS 容器。
- 读取 stdout/stderr 日志流。
- 维护 per-function ring buffer。
- 写入 `/tmp/faas-logs/{funcName}.log`。
- gateway 代理 `GET /logs/{funcName}?tail=10` 到内部 logdaemon。

Kubernetes 模式下：

- collector 通过 ServiceAccount 直连 in-cluster Kubernetes API，不依赖 collector 容器内安装 `kubectl`。
- collector 只跟随本 node 上、带 `faas.managed-by=local-faas` 的函数 Pod 日志。
- proxy 通过 gateway `/internal/instances/{funcName}` 获取函数实例和 `nodeName`。
- gateway 把 `/logs/{funcName}` 查询交给内部 proxy，proxy 再定位对应 collector。
- collector 同样维护 node-local ring buffer，并暴露 `/local/logs/{funcName}` 供 proxy 查询。

## 本地运行

### 依赖

- Go
- Docker Desktop
- minikube（仅 `FAAS_BACKEND=k8s` 需要）
- kubectl（仅 `FAAS_BACKEND=k8s` 需要）
- curl
- zip / unzip
- python3
- JDK / javac（仅 `./test.sh` 编译 Java 示例函数时需要；脚本使用 `javac --release 21` 兼容 runtime 镜像内固定安装的 Java 21 JRE）

### 启动完整系统（Docker 默认后端）

```bash
cd /Users/z/serverless
./start.sh
```

### 启动完整系统（Kubernetes / minikube 后端）

```bash
minikube start
cd /Users/z/serverless
FAAS_BACKEND=k8s ./start.sh
```

Kubernetes 后端会执行 `minikube image load faas-runtime:latest`。函数 Pod 由 client-go 创建，gateway 通过 SPDY port-forward 连接 Pod 内 runtime-agent，因此 gateway 仍可在宿主机上运行。

在 `FAAS_BACKEND=k8s` 下，`start.sh` 还会：

- 交叉编译 Linux 版 `logdaemon` 到 `bin/logdaemon-linux`。
- 把 collector 二进制同步到 minikube node 的 `/tmp/faas-bin`。
- 应用 `k8s-logdaemon.yaml`，部署 `faas-log-collector` DaemonSet、ServiceAccount 和 RBAC。
- 在宿主机启动 `LOGDAEMON_MODE=proxy` 的内部 logdaemon，gateway 通过它查询 collector 日志。

`start.sh` 会执行：

1. 构建 gateway、scalersvc、logdaemon。
2. 交叉编译 Linux arm64 runtime-server、runtime-agent 和 collector 用的 `logdaemon`。
3. 构建 runtime Docker 镜像。
4. 清理旧服务和旧函数实例。
5. k8s 模式下部署 log collector DaemonSet。
6. 启动 gateway、scalersvc、logdaemon。
7. 等待三个服务健康检查通过。

### 运行端到端测试

```bash
cd /Users/z/serverless
./test.sh
```

测试覆盖：

- gateway / scalersvc / logdaemon 健康检查。
- 注册 `hello` 函数。
- 上传 Python handler zip。
- 冷启动调用。
- 热调用复用容器。
- 查询 queue 状态。
- 查询 scale 状态。
- 等待 runtime-agent 指标上报。
- 注入高 p99 指标触发 scale-up。
- 验证 queue backlog 触发扩容。
- Node.js bootstrap 单测、Java bootstrap JSON 单测，以及 Python handler、Go bootstrap、Node.js handler、Java reflection handler 的 smoke test。

### 停止系统

```bash
cd /Users/z/serverless
./stop.sh

# Kubernetes 后端
FAAS_BACKEND=k8s ./stop.sh
```

## 重要接口

### Gateway

```bash
curl -X POST http://localhost:8080/functions/hello \
  -H 'Content-Type: application/json' \
  -d '{"handler":"handler.handler"}'

zip -j /tmp/hello.zip runtime/examples/python/handler.py
curl -X PUT http://localhost:8080/functions/hello/code \
  --data-binary @/tmp/hello.zip

curl -X POST http://localhost:8080/invoke/hello \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice"}'

curl http://localhost:8080/queues/hello
```

### Internal scaler debug

scalersvc 监听 `:9300`，但它是内部控制面服务。普通用户操作都应通过 gateway；直接访问 scaler 只适合本地调试自动扩缩容：

```bash
curl http://localhost:9300/scale/hello
```

### Logs

```bash
curl 'http://localhost:8080/logs/hello?tail=10'
```

## 与主流 FaaS 架构的关系

这个项目与 AWS Lambda、OpenWhisk、Knative、OpenFaaS 等系统有相似的核心概念，但实现被压缩到本地学习场景：

| 能力 | 当前项目 | 主流 FaaS |
|------|----------|-----------|
| API Gateway | Go gateway | API Gateway / Activator / Controller |
| 函数运行环境 | Docker 容器 / Kubernetes Pod | Firecracker / containerd / Kubernetes Pod |
| Runtime API | 自研简化版 | Lambda Runtime API / OpenWhisk runtime contract |
| 函数代码管理 | zip 上传到本地目录 | 对象存储、镜像仓库、版本发布系统 |
| 调度 | 单机 scheduler | 多节点 scheduler / Kubernetes scheduler |
| 扩缩容 | 独立 scalersvc | KEDA / Knative autoscaler / OpenWhisk controller |
| 日志 | 自研 logdaemon | Fluent Bit / CloudWatch / Loki / Elasticsearch |
| 背压 | gateway queue | queue-proxy / activator / broker |
| 隔离 | Docker container | microVM、sandboxed container、gVisor、Kata |

## 当前已知限制

- Kubernetes 后端当前面向 minikube，本地 gateway 通过 Pod port-forward 访问 runtime-agent，不是生产 datapath。
- 未设置 `MONGO_URI` 时函数元数据只在内存中；设置 Mongo 后函数配置可持久化，但运行中的容器/Pod 仍是运行时状态。
- 上传代码会落到本地 `/tmp/faas/{name}`；配置 MinIO 后会同时保存 zip 对象并用 `CodeKey`/`CodeURL` 支持 Kubernetes 代码分发，但还没有函数版本、alias 和 rollback。
- scheduler 冷启动没有全局并发保护，主要靠 gateway queue 减少入口压力。
- scalersvc 当前指标聚合仍偏简化，容器指标与函数映射还可以更严格。
- 没有鉴权、租户隔离、配额和审计。
- Kubernetes 日志链路使用 DaemonSet collector 和 host proxy；proxy 依赖 gateway 实例元数据按 node 路由，仍是学习环境形态而非生产级日志数据面。
- Runtime API 已支持 Python、Go、Node.js、Java 四种最小语言接入，但仍缺少依赖安装、layer、init hook、streaming response 等能力。

## 后续改进方向

### 1. 更完整的请求队列和背压

当前 queue 在 gateway 内实现 per-function 限流，后续可以继续增强：

- 支持按函数配置不同的 max in-flight、max queue、queue timeout。
- 增加队列等待时长、拒绝数、超时数等指标。
- 将 queue 状态提供给 scalersvc，让扩容不仅看容器指标，也看排队压力。
- 支持优先级队列或公平调度，避免单个函数占满 gateway。

### 2. 更准确的 autoscaling

后续 scalersvc 可以从简单阈值策略升级为更接近生产的策略：

- 指标按 function/container 精确归属。
- 引入 queue length、queue wait p95、cold start rate。
- 支持 cooldown window，避免频繁扩缩容抖动。
- 支持 target concurrency 或 target utilization。
- scale-to-zero 和 min replicas 更细化。
- 支持预热池和预测式扩容。

### 3. 持久化控制面

当前无 `MONGO_URI` 时仍以内存状态运行；配置 Mongo 后，函数元数据和 scaler 最新状态可以持久化，MinIO 可保存上传代码包。后续可以继续加入：

- SQLite / Postgres 作为轻量控制面存储选项。
- 函数版本、alias、rollback。
- gateway 重启后更完整地协调已存在的运行中容器/Pod。

### 4. 多节点调度

从单机 Docker 进化到多节点 FaaS，需要拆出更清晰的控制面：

- worker agent 管理每台机器上的容器。
- scheduler 根据节点资源选择部署位置。
- gateway 通过服务发现调用对应 worker。
- 节点心跳、故障转移和容器迁移。
- 跨节点日志、指标和 tracing 聚合。

### 5. 更强隔离和安全

学习阶段使用普通 Docker 容器即可，后续可以研究：

- gVisor / Kata Containers / Firecracker。
- seccomp、AppArmor、只读 rootfs。
- 网络隔离和 egress policy。
- 函数运行身份和最小权限。
- 上传代码扫描和依赖安全检查。

### 6. Runtime 和语言生态

当前 runtime 已覆盖 Python、Go、Node.js、Java 的最小调用闭环，后续可以继续增强：

- Runtime Interface Client 抽象。
- handler 初始化阶段和 invoke 阶段分离。
- init duration、invoke duration 分开上报。
- 函数依赖安装和 layer 机制。
- streaming response 和异步 invocation。

### 7. 可观测性

后续可以把日志、指标、trace 做成完整闭环：

- Prometheus metrics endpoint。
- OpenTelemetry trace id 穿透 gateway、agent、runtime。
- structured logs。
- 函数级 dashboard。
- cold start、queue wait、runtime duration、handler error 的统一视图。

### 8. 开发者体验

后续可以补齐用户侧工作流：

- CLI：`faas deploy`、`faas invoke`、`faas logs`。
- 本地模板：Python / Node / Go。
- 配置文件：`faas.yaml`。
- 热更新代码。
- 本地调试模式。

## 推荐演进路线

建议按下面顺序继续做：

1. 稳定 queue 与 p99 驱动的 autoscaling 时序，补掉当前测试里的竞态。
2. 把 Kubernetes log proxy 升级为真正的 collector 直连或 Service/EndpointSlice 路由。
3. 加函数代码版本、alias 和 rollback。
4. 增加 Prometheus metrics，统一暴露 gateway、agent、scaler 指标。
5. 完善 Java / Node.js dependency packaging，让用户函数包可以携带或安装第三方依赖。
6. 用 Service/EndpointSlice 替代本地 Pod port-forward，形成更接近生产的数据面。
