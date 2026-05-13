# Local Serverless / FaaS Learning Project

这是一个用于学习 AWS Lambda 架构思想的本地 FaaS 系统。项目从最小 gateway 开始，逐步加入 Docker/Kubernetes 运行后端、Runtime API、函数代码上传、runtime-agent、日志 daemon、独立 autoscaler，以及 gateway 入口层的请求队列和背压。

当前实现目标不是生产可用，而是把一个 serverless 平台的关键控制面和数据面拆开，方便理解每个组件为什么存在、它们之间如何协作，以及后续要补哪些能力才能接近主流开源 FaaS 平台。

## 当前能力

- 函数注册、注销、代码 zip 上传。
- Docker 或 Kubernetes Pod 作为函数运行沙箱。
- 容器内实现 Lambda 风格 Runtime API。
- Python handler 动态加载和执行。
- runtime-agent 负责请求转发、健康检查、指标采集和周期上报。
- logdaemon 独立监听 Docker 容器日志并提供 HTTP 查询接口。
- scalersvc 独立运行，收集 agent 指标并按默认策略扩缩容。
- gateway 内置 per-function 请求队列和背压。
- 本地一键启动、停止和端到端测试脚本。

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
│   ├── examples/python/handler.py
│   └── entrypoint.sh
├── scalersvc/main.go           # 独立自动扩缩容服务
├── logdaemon/main.go           # 独立日志采集 daemon
├── Dockerfile                  # runtime 镜像
├── start.sh                    # 构建并启动完整系统
├── test.sh                     # 端到端测试
└── stop.sh                     # 停止服务和函数容器
```

## 当前架构图

```mermaid
flowchart LR
    Client[Client / curl] --> Gateway[Gateway :8080]

    subgraph GatewayProcess[Gateway Process]
        Gateway --> Queue[Per-function Queue / Backpressure]
        Queue --> Router[Router]
        Router --> Scheduler[Scheduler / Container Pool]
        Gateway --> InternalAPI[Internal APIs for Scaler]
    end

    Scheduler --> Backend[RuntimeBackend]
    Backend --> Docker[Docker Engine]
    Backend --> K8s[Kubernetes / minikube]
    Docker --> Container[Function Container]
    K8s --> Container

    subgraph Container[Function Container]
        Agent[Runtime Agent :9001]
        Runtime[Runtime API Server :9000]
        Bootstrap[Python Bootstrap]
        Handler[User Handler]
        Agent --> Runtime
        Runtime --> Bootstrap
        Bootstrap --> Handler
    end

    Router --> Agent
    Agent -- metrics every 10s --> Gateway
    Gateway -- forward metrics --> ScalerSvc[ScalerSvc :9300]
    ScalerSvc --> InternalAPI
    InternalAPI --> Scheduler

    Docker -- events/logs --> LogDaemon[LogDaemon :9200]
    LogDaemon --> LogAPI[Log Query API]
```

## 请求调用链路

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant Q as Queue
    participant R as Router
    participant S as Scheduler
    participant A as Runtime Agent
    participant RT as Runtime API
    participant B as Python Bootstrap
    participant H as Handler

    C->>G: POST /invoke/{name}
    G->>Q: acquire per-function slot
    alt queue full
        Q-->>C: 429 queue full
    else queue timeout
        Q-->>C: 503 queue timeout
    else slot acquired
        Q->>R: Invoke
        R->>S: Acquire container
        alt no idle instance
            S->>S: backend cold start Docker container or Kubernetes Pod
        end
        S-->>R: runtime instance addr
        R->>A: POST /invoke
        A->>RT: proxy /invoke
        RT->>B: dispatch via /runtime/invocation/next
        B->>H: call handler(event, context)
        H-->>B: result
        B->>RT: POST /runtime/invocation/{id}/response
        RT-->>A: invoke result
        A-->>R: response + metrics recorded
        R->>S: Release container
        R-->>Q: done
        Q-->>C: handler response
    end
```

## 指标与扩缩容链路

```mermaid
sequenceDiagram
    participant A as Runtime Agent
    participant G as Gateway
    participant SC as ScalerSvc
    participant GI as Gateway Internal API
    participant S as Scheduler

    A->>G: POST /containers/{id}/metrics
    G->>SC: POST /metrics/{id}
    loop every 5s
        SC->>GI: GET /internal/functions
        SC->>GI: GET /internal/stats/{funcName}
        SC->>SC: evaluate util / p99 / error rate
        alt scale up
            SC->>GI: POST /internal/scale-up/{funcName}
            GI->>S: ColdStartOne(funcName)
        else scale down
            SC->>GI: POST /internal/scale-down/{funcName}
            GI->>S: RemoveIdle(funcName)
        else none
            SC->>SC: record decision
        end
    end
```

## 日志采集链路

```mermaid
flowchart LR
    Docker[Docker Engine] --> Events[Docker Events]
    Docker --> Logs[Container stdout/stderr]
    Events --> LogDaemon[logdaemon]
    Logs --> LogDaemon
    LogDaemon --> Buffer[Per-function Ring Buffer]
    LogDaemon --> Files[/tmp/faas-logs/*.log]
    Client[Client] --> LogAPI[GET /logs/{funcName}]
    LogAPI --> Buffer
```

## 核心组件说明

### Gateway

Gateway 是系统入口，监听 `:8080`，负责对外暴露函数管理和调用接口：

- `POST /functions/{name}`：注册函数。
- `DELETE /functions/{name}`：注销函数并停止相关容器。
- `PUT /functions/{name}/code`：上传 zip 代码包。
- `POST /invoke/{name}`：调用函数。
- `GET /queues/{name}`：查看函数请求队列状态。
- `POST /containers/{id}/metrics`：接收 runtime-agent 指标并转发给 scalersvc。
- `/internal/*`：给 scalersvc 调用的内部控制接口。

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

- `FAAS_BACKEND=docker`：默认后端，使用 `docker run` 和端口映射启动本地容器。
- `FAAS_BACKEND=k8s`：Kubernetes 后端，面向 minikube，用 `kubectl` 创建 Pod，并用本地 `kubectl port-forward` 让 gateway 访问 Pod 内 runtime-agent。

Kubernetes 后端会把上传到 `/tmp/faas/{name}` 的函数代码同步到 minikube VM，再用 `hostPath` 挂载到 Pod 的 `/function`。

### Runtime API Server

每个函数容器内运行 `runtime-server`，监听 `:9000`，实现 Lambda 风格 Runtime API：

- `POST /invoke`：接收 agent 转发的调用请求。
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

logdaemon 独立运行，监听 `:9200`，通过 Docker socket 收集日志：

- 监听带 `faas.function` label 的 Docker container events。
- 启动时也会 attach 已存在的 FaaS 容器。
- 读取 stdout/stderr 日志流。
- 维护 per-function ring buffer。
- 写入 `/tmp/faas-logs/{funcName}.log`。
- 提供 `GET /logs/{funcName}?tail=10` 查询接口。

## 本地运行

### 依赖

- Go
- Docker Desktop
- minikube（仅 `FAAS_BACKEND=k8s` 需要）
- kubectl（仅 `FAAS_BACKEND=k8s` 需要）
- curl
- zip / unzip
- python3

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

Kubernetes 后端会执行 `minikube image load faas-runtime:latest`，函数 Pod 通过 `kubectl port-forward` 暴露到本地 `9100+` 端口，因此 gateway 仍可在宿主机上运行。

`start.sh` 会执行：

1. 构建 gateway、scalersvc、logdaemon。
2. 交叉编译 Linux arm64 runtime-server 和 runtime-agent。
3. 构建 runtime Docker 镜像。
4. 清理旧服务和旧函数实例。
5. 启动 gateway、scalersvc、logdaemon。
6. 等待三个服务健康检查通过。

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
- 查询函数日志。

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

### ScalerSvc

```bash
curl http://localhost:9300/scale/hello
```

### LogDaemon

```bash
curl 'http://localhost:9200/logs/hello?tail=10'
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

- Kubernetes 后端当前面向 minikube，本地 gateway 通过 `kubectl port-forward` 访问 Pod，不是生产 datapath。
- 函数元数据只在内存中，gateway 重启后丢失。
- 上传的代码放在 `/tmp/faas/{name}`，没有版本管理和制品存储。
- scheduler 冷启动没有全局并发保护，主要靠 gateway queue 减少入口压力。
- scalersvc 当前指标聚合仍偏简化，容器指标与函数映射还可以更严格。
- 没有鉴权、租户隔离、配额和审计。
- 日志 daemon 依赖 Docker socket；Kubernetes 后端仍需补 Kubernetes logs/watch 版本。
- Runtime API 只实现了最小调用闭环。
- Python bootstrap 只支持同步 handler，没有依赖安装和多语言 runtime 管理。

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

当前函数注册表、容器池和扩缩容状态主要在内存里。后续可以加入：

- SQLite / Postgres 存函数元数据。
- 本地对象存储或 MinIO 存函数代码包。
- 函数版本、alias、rollback。
- gateway 重启后恢复函数列表和运行状态。

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

当前只实现 Python bootstrap。后续可以扩展：

- Node.js、Go custom runtime。
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

1. 把 queue 指标接入 scalersvc，让排队长度能触发扩容。
2. 修正 scalersvc 的指标归属，做到按函数聚合指标。
3. 加 SQLite 持久化函数元数据和代码版本。
4. 增加 Prometheus metrics，统一暴露 gateway、agent、scaler 指标。
5. 实现 Node.js runtime，验证 Runtime API 抽象是否足够通用。
6. 把 Kubernetes 后端从 `kubectl` shell out 升级为 client-go，并用 Service/EndpointSlice 替代本地 port-forward。
