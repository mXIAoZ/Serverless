# Scripts Guide

这个文档专门介绍项目根目录下的几个运维/验证脚本：

- `start.sh`
- `stop.sh`
- `test.sh`

它们不是业务组件，但对这个学习项目非常重要，因为大部分端到端体验都靠它们串起来。

## 脚本总览

### `start.sh`

负责：

- 构建二进制
- 构建 runtime 镜像
- 清理旧进程和旧实例
- 启动 gateway / scalersvc / logdaemon
- k8s 模式下额外部署 log collector DaemonSet
- 等待健康检查通过

### `stop.sh`

负责：

- 停掉本地服务进程
- 删除运行中的函数实例
- k8s 模式下清理 log collector DaemonSet

### `test.sh`

负责：

- 端到端部署示例函数
- 依次验证调用、指标、队列、扩缩容、日志
- 同时覆盖 Docker 和 Kubernetes 两种后端路径

## `start.sh` 详细说明

入口文件：`start.sh`

### 核心环境变量

- `FAAS_BACKEND`
  - 默认 `docker`
  - 可选 `k8s` / `kubernetes`
- `K8S_NAMESPACE`
  - 默认 `default`
- `GATEWAY_ADDR`
  - 会传给容器内 runtime-agent

### 整体流程

#### 1. 构建二进制

会构建：

- `bin/gateway`
- `bin/scalersvc`
- `bin/logdaemon`
- `bin/logdaemon-linux`
- `bin/runtime-server-linux`
- `bin/runtime-agent-linux`

这里有两个关键点：

- `logdaemon-linux` 是为了放进 minikube node 给 collector 用。
- runtime 二进制是交叉编译成 Linux arm64，供容器/节点环境运行。

#### 2. 构建 runtime 镜像

执行：

- `docker build -t faas-runtime:latest .`

k8s 模式下还会进一步：

- `minikube image load faas-runtime:latest`

这样新创建的函数 Pod 和 collector Pod 都能直接复用镜像。

#### 3. 清理旧状态

会清理三部分：

##### 本地端口进程

- `:8080`
- `:9200`
- `:9300`

##### Kubernetes 后端

- 删除带 `faas.managed-by=local-faas` label 的函数 Pod
- 删除旧的 `faas-log-collector` 资源
- 清理旧 `kubectl port-forward` 进程

##### Docker 后端

- 删除带 `faas.function` label 的容器

### 4. 启动主服务

#### gateway

Docker 模式下：

- `FAAS_BACKEND=docker`
- `GATEWAY_ADDR` 默认 `host.docker.internal:8080`

Kubernetes 模式下：

- `FAAS_BACKEND=k8s`
- `GATEWAY_ADDR` 默认 `host.minikube.internal:8080`
- `PATH` 里补上 `$HOME/.local/bin`，方便找到本地安装的 `kubectl` / `minikube`

#### scalersvc

统一通过：

- `GATEWAY_INTERNAL_ADDR=localhost:8080`

启动本地 scaler。

#### logdaemon

Docker 模式：

- `LOGDAEMON_MODE=docker`

Kubernetes 模式：

- 先部署 collector DaemonSet
- 再本地启动：
  - `LOGDAEMON_MODE=proxy`
  - `K8S_NAMESPACE=...`
  - `GATEWAY_INTERNAL_ADDR=localhost:8080`

### 5. k8s collector 部署

`start_k8s_log_collector()` 是 `start.sh` 里最特别的一段。

它会：

1. 把 `bin/logdaemon-linux` 复制到本机 `/tmp/faas-bin/logdaemon`
2. 用 `minikube ssh` 确保 node 上 `/tmp/faas-bin` 存在
3. 用 `minikube cp` 把 Linux 二进制同步到 minikube node
4. 用 Python 把 `k8s-logdaemon.yaml` 模板里的 `__K8S_NAMESPACE__` 替换掉
5. `kubectl apply -f /tmp/faas-logdaemon.yaml`
6. `kubectl rollout status daemonset/faas-log-collector`

### 6. 健康检查

脚本最后会循环等待：

- `http://localhost:8080/health`
- `http://localhost:9300/health`
- `http://localhost:9200/health`（内部 logdaemon readiness）

全部成功后才打印服务地址。

## `stop.sh` 详细说明

入口文件：`stop.sh`

### 核心职责

它的目标不是“优雅下线所有运行态”，而是“把本地实验环境尽快收回到干净状态”。

### 执行流程

#### 1. 停本地服务进程

依次读取这些 pid 文件并 kill：

- `/tmp/faas-gateway.pid`
- `/tmp/faas-scalersvc.pid`
- `/tmp/faas-logdaemon.pid`

#### 2. 停运行中的函数实例

##### k8s 模式

- 删除带 `faas.managed-by=local-faas` label 的函数 Pod
- 删除 `faas-log-collector` 相关资源
- 清理 `kubectl port-forward` 进程

##### Docker 模式

- 删除带 `faas.function` label 的容器

### `cleanup_k8s_log_collector()`

这个 helper 会在 `/tmp/faas-logdaemon.yaml` 存在时执行：

- `kubectl delete -f /tmp/faas-logdaemon.yaml --ignore-not-found=true`

也就是说，collector 的资源清理依赖于 start 阶段渲染出来的临时 manifest。

## `test.sh` 详细说明

入口文件：`test.sh`

### 测试目标

它不是单元测试，而是一个“平台功能冒烟 + 集成验证”脚本。

它覆盖的链路包括：

- gateway 对外 API
- scheduler 冷启动和热复用
- runtime-agent 指标上报
- scalersvc 的 p99 / queue 扩缩容判断
- gateway 日志查询入口与内部 logdaemon 采集链路

### 核心环境变量

- `FAAS_BACKEND`
- `K8S_NAMESPACE`
- `GATEWAY` 默认 `http://localhost:8080`
- `SCALER` 默认 `http://localhost:9300`
- `LOGS` 默认跟随 gateway：`http://localhost:8080`
- `FUNC` 默认 `hello`
- `QUEUE_FUNC` 默认 `hello-queue`

### 测试阶段

#### 1. 健康检查

验证：

- gateway
- scalersvc
- logdaemon

#### 2. 注册函数

调用：

- `POST /functions/hello`

若已注册，会复用已有函数。

#### 3. 上传代码

- 把 `runtime/examples/python/handler.py` 打成 zip
- 上传到 `PUT /functions/hello/code`

#### 4. 冷启动调用

- 第一次调用 `POST /invoke/hello`
- 记录延迟
- 验证返回 `statusCode == 200`

#### 5. 热调用

连续调用几次，观察延迟下降，验证实例复用生效。

#### 6. 查看 queue 状态

调用：

- `GET /queues/hello`

#### 7. 查看 scale 状态

调用：

- `GET /scale/hello`

#### 8. 等待 agent 指标上报

默认等待 12 秒，让 runtime-agent 有时间把 metrics 发给 gateway，再由 gateway 转给 scalersvc。

#### 9. 注入高 p99 指标

测试脚本会手动向 scalersvc 注入一条高 p99 指标，用来验证：

- `p99 > 阈值` 是否能触发 `scale-up`

当前这里仍有一点时序敏感性：如果后续新的正常指标很快覆盖掉旧决策，断言可能会偶发受到影响。

#### 10. 队列 backlog 触发扩容

这是现在比较重要的一段：

- 单独注册 `hello-queue`
- 上传同一个 handler
- 用 Python 并发请求制造排队
- 断言 queue backlog 出现
- 再观察 scalersvc 是否给出：
  - `action=scale-up`
  - `reason` 含 `queue=`

#### 11. 日志查询

##### Docker 模式

通过 gateway 调用：

- `GET /logs/{func}`

##### Kubernetes 模式

- 测试会轮询 gateway 的 `GET /logs/{func}?tail=10`
- 断言至少能看到 runtime/invoke/bootstrap 相关日志

这里现在不再死盯 `Hello, Alice!`，因为当前可见日志更多是函数 Pod stdout 的完整输出，而不一定总是恰好包含 handler 的返回内容。

## 常见环境变量一览

### 启动/停止脚本相关

- `FAAS_BACKEND`
- `K8S_NAMESPACE`
- `GATEWAY_ADDR`
- `MONGO_URI`
  - 为空时使用内存模式
  - 非空时 gateway/scalersvc 会把函数元信息、指标和扩缩容状态写入外部 MongoDB
- `MONGO_DB`
  - 默认 `faas`
- `MONGO_TIMEOUT_MS`
  - 默认 `3000`

### 测试脚本相关

- `MAX_REPLICAS`
  - 常用于测试 queue 扩容时给系统留 headroom
- `FUNC`
- `QUEUE_FUNC`

## 典型使用方式

### Docker 后端

```bash
./start.sh
./test.sh
./stop.sh
```

### 使用外部 MongoDB 持久化

```bash
MONGO_URI=mongodb://localhost:27017 MONGO_DB=faas ./start.sh
./test.sh
./stop.sh
```

`start.sh` 不会自动启动 MongoDB；需要你提前启动本机或外部 MongoDB 服务。未设置 `MONGO_URI` 时，gateway 和 scalersvc 继续使用内存状态。

### Kubernetes 后端

```bash
minikube start
PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./start.sh
PATH="$PATH:$HOME/.local/bin" MAX_REPLICAS=10 FAAS_BACKEND=k8s ./test.sh
PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./stop.sh
```

## 排障建议

### `minikube: command not found`

原因：`start.sh` / `test.sh` / `stop.sh` 所在 shell 找不到 minikube。

做法：

```bash
PATH="$PATH:$HOME/.local/bin" FAAS_BACKEND=k8s ./start.sh
```

### queue 扩容测试没触发

常见原因：

- `MAX_REPLICAS` 太小，系统已经到上限，没法再 scale-up
- 请求不够慢，来不及形成 backlog

建议：

- 用 `MAX_REPLICAS=10`
- 保留 `sleep_ms` 负载

### k8s 日志查询为空

优先检查：

- collector DaemonSet 是否 ready
- `kubectl logs -n <ns> -l app=faas-log-collector`
- `curl 'http://localhost:8080/logs/hello?tail=20'`

当前架构下，collector 部署和排障仍依赖本机 `kubectl`。日志查询路径由 host proxy 通过 gateway 实例元数据定位 node collector，但 proxy 到 collector 仍通过 Kubernetes exec API 进入 collector Pod；如果集群 RBAC 不允许 `pods/exec`，日志链路会失败。

### runtime 相关日志正常但 handler 输出不明显

这是当前实现的正常现象之一。collector/proxy 现在采到的是函数 Pod stdout 全量流，测试里更稳定的断言对象是：

- `[invoke]`
- `[runtime-api]`
- `[bootstrap]`

## 当前限制

- `start.sh` / `stop.sh` 都带有较强“本地实验环境清理”假设，不适合直接照搬到共享环境。
- `test.sh` 是集成冒烟脚本，不是可重复、完全无竞态的 CI 级测试套件。
- k8s 模式当前仍依赖：
  - `kubectl`
  - `minikube`
  - Kubernetes `pods/exec` 权限用于 logdaemon proxy 查询 collector
  - 本地 MinIO port-forward 等开发环境辅助进程
- collector 的部署依赖本机临时文件 `/tmp/faas-logdaemon.yaml` 与 `/tmp/faas-bin`。
