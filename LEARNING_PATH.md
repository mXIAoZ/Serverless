# Learning Path

这个文档按“先建立整体模型，再深入组件实现，最后回看端到端行为”的顺序，给你一条比较顺手的阅读路径。

如果你的目标是理解这个项目，而不是立刻改代码，建议不要一上来就从 `scheduler.go` 或 `runtime-server` 埋头读起。先把系统边界、组件关系、调用链路建起来，后面读源码会快很多。

## 一条推荐路径

### 第 1 步：先看总览

先读：

- [`README.md`](README.md)

重点看这些部分：

- 当前能力
- 当前架构图
- 请求调用链路
- 指标与扩缩容链路
- 日志采集链路
- 文档索引

你应该先回答这几个问题：

1. 这个项目一共有哪几个主进程？
2. 一次调用从 `curl` 到用户 handler，大概经过哪些组件？
3. Docker 和 k8s 两条后端的差异主要落在哪些层？
4. metrics、autoscaling、logs 分别是谁负责？

如果这一步没建立好，后面读源码会容易“只看见局部函数，不知道它为什么存在”。

## 第 2 步：看入口层，先搞清请求怎么进来

先读：

- [`gateway/README.md`](gateway/README.md)
- [`gateway/main.go`](gateway/main.go)

这一步的目标是搞清楚：

- 哪些是外部接口
- 哪些是内部接口
- `queue`、`router`、`scheduler` 是怎么串起来的

阅读时建议关注：

- `/functions/*`
- `/invoke/*`
- `/queues/*`
- `/containers/*/metrics`
- `/internal/*`

你应该形成一个清晰印象：gateway 不是“只做路由”，它其实是控制面的拼接层。

## 第 3 步：看 queue，理解为什么不会无限冷启动

接着读：

- [`gateway/queue/queue.go`](gateway/queue/queue.go)

目标：

- 理解这个项目的背压点为什么放在 gateway 入口，而不是放在容器内。
- 理解 per-function queue 与 `maxInflight` / `maxQueue` 的作用。

重点看：

- `Manager.Invoke()`
- `Status()`
- `functionQueue.sem`

你应该能回答：

1. 什么情况下返回 `429 queue full`？
2. 什么情况下返回 `503 queue timeout`？
3. 为什么一个函数打满不会直接把另一个函数拖死？

## 第 4 步：看 router，理解单次调用路径

接着读：

- [`gateway/router/router.go`](gateway/router/router.go)

目标：

- 理解“一次同步调用”最短路径。

重点看：

- `Router.Invoke()`

这里你会发现 router 做的事情很纯：

- 查函数配置
- 读 body
- 取实例
- 转发到 agent
- 释放实例

这一步之后，你应该知道：

- 真正的“函数生命周期管理”不在 router，而在 scheduler。

## 第 5 步：看 scheduler，理解平台为什么能冷启动/复用/回收

接着读：

- [`gateway/scheduler/scheduler.go`](gateway/scheduler/scheduler.go)
- [`gateway/scheduler/backend.go`](gateway/scheduler/backend.go)

这是整个项目里最值得慢慢读的一部分之一。

阅读目标：

- 函数注册信息如何保存在内存里，以及配置 Mongo 时如何持久化
- 上传代码如何替换旧实例
- 实例池怎么组织
- idle / busy 状态如何流转
- scaler 和 log proxy 依赖哪些元数据

重点看：

- `FunctionConfig`
- `Acquire()`
- `Release()`
- `UploadCode()`
- `ColdStartOne()`
- `RemoveIdle()`
- `reaper()`
- `Instances()`

你应该能回答：

1. 为什么热调用会更快？
2. 上传新代码后为什么会停掉旧实例？
3. 为什么 `NodeName` 要存在 scheduler 里？

## 第 6 步：分叉阅读后端实现

到这里你已经懂“平台想做什么”，接下来再看“不同后端怎么实现”。

### 先看 Docker 路径

读：

- [`gateway/scheduler/docker_backend.go`](gateway/scheduler/docker_backend.go)

目标：

- 理解最简单的冷启动路径。

重点看：

- Docker Engine API 创建容器
- 端口映射到 `9001`
- `FUNCTION_HANDLER`
- `GATEWAY_ADDR`
- `waitReady()`

### 再看 Kubernetes 路径

读：

- [`gateway/scheduler/k8s_backend.go`](gateway/scheduler/k8s_backend.go)

目标：

- 理解当前 k8s 方案为什么更像“本地实验形态”，而不是生产 FaaS datapath。

重点看：

- MinIO presigned URL + initContainer 代码分发
- hostPath fallback
- Pod manifest 构造
- client-go 创建/等待 Pod
- SPDY port-forward
- `podNodeName()`

你应该意识到：

- gateway 仍运行在宿主机
- 函数 Pod 在 minikube
- 两边靠 Pod port-forward 接起来，代码优先走 MinIO/initContainer，缺少对象存储时才回退 hostPath

## 第 7 步：看容器内部运行时，理解用户代码怎么真正执行

接着读：

- [`runtime/README.md`](runtime/README.md)
- [`runtime/entrypoint.sh`](runtime/entrypoint.sh)
- [`runtime/cmd/agent/main.go`](runtime/cmd/agent/main.go)
- [`runtime/cmd/runtime/main.go`](runtime/cmd/runtime/main.go)
- [`runtime/bootstrap/python3_bootstrap.py`](runtime/bootstrap/python3_bootstrap.py)
- [`runtime/cmd/go-bootstrap/main.go`](runtime/cmd/go-bootstrap/main.go)
- [`runtime/bootstrap/nodejs_bootstrap.js`](runtime/bootstrap/nodejs_bootstrap.js)
- [`runtime/bootstrap/java/JavaBootstrap.java`](runtime/bootstrap/java/JavaBootstrap.java)

建议阅读顺序不要改，按上面这个顺序最好。

### 为什么先看 `entrypoint.sh`

因为它最快帮你建立“容器里其实有两个平台进程 + 一个语言进程”的模型：

- `runtime-server`
- `runtime-agent`
- Python bootstrap
- Go bootstrap
- Node.js bootstrap
- Java bootstrap

### 然后看 `runtime-agent`

你会先看到平台入口：

- `/invoke`
- `/health`
- `/metrics`

然后理解：

- 为什么 metrics 在这里采
- 为什么 health 也是这里代理

### 再看 `runtime-server`

这是 Runtime API 真正的实现。

重点理解：

- `/invoke`
- `/runtime/invocation/next`
- `/response`
- `/error`
- `queue`
- `inflight`
- `notify`

### 最后看语言 bootstrap

此时你已经知道 Runtime API 长什么样，再回头看 bootstrap，就会很顺：

- Python/Node.js：动态加载用户 handler。
- Go：执行用户提供的 `/function/bootstrap`。
- Java：通过 `ClassName::methodName` 反射调用静态方法。

共同主线都是：

- 拉事件
- 调 handler 或用户 bootstrap
- 回传结果

如果反过来先看 bootstrap，容易只看到一段语言进程轮询，不知道它跟平台整体怎么接上。

## 第 8 步：看 autoscaling，理解系统如何根据压力做反应

接着读：

- [`scalersvc/README.md`](scalersvc/README.md)
- [`scalersvc/main.go`](scalersvc/main.go)

这一步的目标是理解“为什么系统会多拉实例或少拉实例”。

建议重点看：

- `policy`
- `evaluateLoop()`
- `evaluate()`
- `aggregateMetrics()`
- `queueStatus()`
- `handleStatus()`

你应该能回答：

1. 什么指标会触发 `scale-up`？
2. queue backlog 是怎么接入 scaler 判断的？
3. 为什么指标必须按函数过滤，而不能全局混在一起？

## 第 9 步：看日志系统，理解 Docker 与 k8s 为什么走了两条路

接着读：

- [`logdaemon/README.md`](logdaemon/README.md)
- [`logdaemon/main.go`](logdaemon/main.go)
- [`logdaemon/shared.go`](logdaemon/shared.go)
- [`logdaemon/docker_source.go`](logdaemon/docker_source.go)
- [`logdaemon/k8s_collector.go`](logdaemon/k8s_collector.go)

建议顺序：

1. `main.go`
2. `shared.go`
3. `docker_source.go`
4. `k8s_collector.go`

### 先看 `main.go`

目的是先记住：

- `docker`
- `collector`
- `proxy`

这三个模式分别做什么。

### 再看 `shared.go`

这是日志系统最核心的公共层：

- ring buffer
- gateway 的 `/logs/*` 用户入口
- logdaemon 内部的 `/logs/*` 与 `/local/logs/*`
- proxy 查询
- SSE stream

### 然后看 Docker 采集路径

你会看到最直接的一条链路：

- Docker event
- attach logs
- 解析 multiplexed frame
- 写 ring

### 最后看 k8s collector

你会理解：

- 为什么要通过 in-cluster API 跟随 Pod logs
- 为什么 collector 只关心本 node Pod
- 为什么 proxy 需要 scheduler 暴露 `nodeName`

## 第 10 步：最后看脚本，把系统从“读懂组件”变成“读懂整个运行闭环”

最后读：

- [`SCRIPTS.md`](SCRIPTS.md)
- [`start.sh`](start.sh)
- [`test.sh`](test.sh)
- [`stop.sh`](stop.sh)

这一步的目标是：

- 把之前看到的组件，重新按“系统启动 / 系统测试 / 系统停止”三个阶段串起来。

尤其建议重点看：

- `start_k8s_log_collector()`
- `test.sh` 的 queue backlog 段
- `test.sh` 的日志验证段

这会帮助你把：

- gateway
- runtime
- scaler
- logdaemon
- minikube

全部串回到一个真实执行路径里。

## 如果你只想先抓主线

如果你现在时间不多，最短路径建议是：

1. [`README.md`](README.md)
2. [`gateway/main.go`](gateway/main.go)
3. [`gateway/scheduler/scheduler.go`](gateway/scheduler/scheduler.go)
4. [`runtime/cmd/agent/main.go`](runtime/cmd/agent/main.go)
5. [`runtime/cmd/runtime/main.go`](runtime/cmd/runtime/main.go)
6. [`scalersvc/main.go`](scalersvc/main.go)
7. [`logdaemon/shared.go`](logdaemon/shared.go)
8. [`test.sh`](test.sh)

这条线能让你最快抓住“调用、扩缩容、日志”三条主链路。

## 如果你接下来要改代码

### 想改调用路径

优先看：

- [`gateway/router/router.go`](gateway/router/router.go)
- [`runtime/cmd/agent/main.go`](runtime/cmd/agent/main.go)
- [`runtime/cmd/runtime/main.go`](runtime/cmd/runtime/main.go)

### 想改冷启动/容器生命周期

优先看：

- [`gateway/scheduler/scheduler.go`](gateway/scheduler/scheduler.go)
- [`gateway/scheduler/docker_backend.go`](gateway/scheduler/docker_backend.go)
- [`gateway/scheduler/k8s_backend.go`](gateway/scheduler/k8s_backend.go)

### 想改 autoscaling

优先看：

- [`scalersvc/main.go`](scalersvc/main.go)
- [`gateway/main.go`](gateway/main.go)
- [`gateway/queue/queue.go`](gateway/queue/queue.go)

### 想改日志系统

优先看：

- [`logdaemon/shared.go`](logdaemon/shared.go)
- [`logdaemon/k8s_collector.go`](logdaemon/k8s_collector.go)
- [`gateway/scheduler/scheduler.go`](gateway/scheduler/scheduler.go)
- [`gateway/main.go`](gateway/main.go)

## 阅读时最值得盯住的几个问题

建议你边看边不断问自己：

1. 这一层是在处理“平台控制逻辑”，还是“用户函数执行逻辑”？
2. 这个组件持有的是事实状态，还是只是转发器？
3. 这个功能在 Docker 和 k8s 下是不是同一实现？如果不是，差异点在哪里？
4. 当前实现为了学习做了哪些简化？如果往生产走，最先会卡在哪里？

如果你一直带着这几个问题读，建立整体心智模型会快很多。
