# LogDaemon Component

`logdaemon/` 负责这个项目里的日志采集和内部查询。用户不直接访问 logdaemon；用户日志 API 由 gateway 暴露为 `GET /logs/{func}`，gateway 再代理到内部 `:9200` logdaemon。logdaemon 内部根据运行模式切成三种实现：

- `docker`：本地直接从 Docker socket 采集函数容器日志。
- `collector`：在 Kubernetes node 上运行，采集本 node 上函数 Pod 的日志。
- `proxy`：运行在宿主机，接收 gateway 转发的 `/logs/{func}`，内部把查询路由到对应 collector。

这让日志系统在 Docker 和 k8s 两条后端上都能工作，同时由 gateway 对用户保持统一的查询接口。

## 目录结构

```text
logdaemon/
├── main.go
├── shared.go
├── docker_source.go
└── k8s_collector.go
```

## 组件职责

`logdaemon` 的职责分成两层：

1. 采集层
   - Docker 下 attach 容器 stdout/stderr。
   - k8s 下跟随 node-local Pod logs。
2. 查询层
   - 维护 per-function ring buffer。
   - 把日志落盘到 `/tmp/faas-logs`。
   - 暴露查询 API 和 SSE stream API。

## 运行模式

入口在 `logdaemon/main.go`。

### 模式选择

- `LOGDAEMON_MODE=docker`
- `LOGDAEMON_MODE=collector`
- `LOGDAEMON_MODE=proxy`

如果没显式设置：

- `FAAS_BACKEND=k8s` 时默认是 `proxy`
- 其他情况默认是 `docker`

### 启动行为

#### docker 模式

- `go d.collectExisting(ctx)`
- `go d.watchEvents(ctx)`

#### collector 模式

- `go d.runCollector(ctx)`

#### proxy 模式

- 初始化 `d.proxy = newProxyClient()`
- 不直接采集日志，只负责查询转发

## Internal HTTP API

这些接口都由 `shared.go` 中的 `serveHTTP()` 注册，供 gateway 和 logdaemon 内部组件使用，不作为用户直接访问的公共 API。

### gateway 代理接口

- `GET /logs/{funcName}`
- `GET /logs/{funcName}/stream`
- `GET /health`

### collector 本地接口

- `GET /local/logs/{funcName}`
- `GET /local/logs/{funcName}/stream`

其中 `/local/*` 只给 proxy 或调试使用，`/logs/*` 只作为 gateway 的上游接口；两者都不是给用户直接访问的稳定接口。

## 核心数据结构

### `LogEntry`

每一行日志的统一表示：

- `Time`
- `Function`
- `Stream`
- `Line`

### `ring`

每个函数都有一个固定大小 ring buffer。

重要字段：

- `entries [ringSize]LogEntry`
- `head`
- `count`
- `subs []chan LogEntry`

它提供三类能力：

- `push()`：写入新日志
- `tail()`：读取尾部 N 条日志
- `subscribe()` / `unsubscribe()`：给 SSE 订阅用

### `daemon`

主对象：

- `rings map[string]*ring`
- `files map[string]*os.File`
- `proxy *proxyClient`

### `proxyClient`

只在 k8s proxy 模式使用：

- `gatewayAddr`
- `namespace`
- `httpClient`

它负责：

- 通过 gateway 获取函数实例与 node 信息
- 找到对应 collector
- 把多个 collector 的结果合并排序

## 查询层实现

### `serveHTTP()`

`serveHTTP(mode)` 是整个查询层入口。

它把请求分成两类：

#### `/logs/*`

- gateway 代理用户 `GET /logs/*` 到这里。
- 在 `proxy` 模式下，会走 `entriesFor()` -> `proxy.fetchLogs()`。
- 在其他模式下，直接从本地 ring buffer 读。

#### `/local/logs/*`

- 只从本地 ring buffer 读。
- 主要用于 collector 暴露本 node 日志给 proxy 查询。

### `entriesFor()`

逻辑分支非常重要：

- 如果当前是 `proxy` 模式：
  - 调 `proxy.fetchLogs()`
  - 拿到结果后也会 `d.write(e)` 缓存在宿主机本地 ring/file 中
- 否则：
  - 调 `localEntries()` 从本地 ring 直接返回

### `handleStream()`

- 在非 proxy 模式下，直接订阅本地 ring。
- 在 proxy 模式下，目前实现是轮询型 fan-in：
  - 每 2 秒 `fetchLogs(funcName, 20, "")`
  - 按时间去重增量输出

这意味着 k8s 下 SSE 现在是“轮询聚合版”，不是长连接直通 collector 的高效实现。

## 写入层实现

### `ringFor()`

第一次访问某个函数时会：

- 创建 ring
- 打开 `/tmp/faas-logs/{funcName}.log`
- 缓存在 `daemon.files`

### `write()`

每收到一条日志：

1. 写进对应函数 ring buffer。
2. 如果文件句柄存在，再以 JSONL 的形式 append 到磁盘。

这让查询既能命中内存，也保留简单落盘痕迹，便于调试。

## Docker 模式详解

实现位于 `logdaemon/docker_source.go`。

### `dockerDo()`

通过 Unix socket `/var/run/docker.sock` 直接访问 Docker API。

### `watchEvents()`

- 监听 `/events`。
- 只关心带 `faas.function` label 的容器。
- `start` 时触发 `collectLogs()`。
- `die` 时只打印日志，不做特殊清理。

### `collectExisting()`

启动时列出当前已在运行的函数容器，并补挂上日志采集。这样 logdaemon 重启后不会丢掉已有容器的 attach 动作。

### `collectLogs()`

它处理 Docker 日志流的核心点是：

- 读取 Docker multiplexed 日志帧头
- 区分 stdout / stderr
- 尝试从行首解析 RFC3339Nano 时间戳
- 组装成 `LogEntry`
- 调 `d.write()`

这部分是 Docker 模式的真正数据来源。

## Kubernetes collector 模式详解

实现位于 `logdaemon/k8s_collector.go`。

### 设计目标

collector 运行在每个 k8s node 上，只负责采集“本 node 上的函数 Pod”。

它不做全局聚合，不做跨节点查询。

### `collectorState`

重要字段：

- `namespace`
- `nodeName`
- `client`
- `baseURL`
- `seen`

### `newCollectorState()`

初始化时会：

1. 从环境变量读取：
   - `K8S_NAMESPACE`
   - `MY_NODE_NAME`
2. 从 ServiceAccount 挂载目录读取：
   - token
   - CA 证书
3. 构造一个带 Bearer Token 和自定义 Root CA 的 `http.Client`
4. 把 base URL 固定成 `https://kubernetes.default.svc`

也就是说，collector 不依赖容器里有 `kubectl`，而是直接走 in-cluster Kubernetes API。

### `runCollector()`

- 每 2 秒调用一次 `syncPods()`。
- 它不是 watch/cache 模型，而是简单轮询。

### `listLocalFunctionPods()`

通过 Kubernetes API 请求：

- `GET /api/v1/namespaces/{ns}/pods?labelSelector=faas.managed-by=local-faas`

拿到 Pod 列表后提取：

- Pod 名
- `faas.function` label
- `spec.nodeName`
- `status.phase`

### `syncPods()`

- 过滤出：
  - `NodeName == MY_NODE_NAME`
  - `Phase == Running`
  - `Function != ""`
- 每个 Pod 只 attach 一次，靠 `seen map[string]struct{}` 去重。
- 对首次看到的 Pod，后台启动 `followPod()`。

### `followPod()`

通过 Pod logs API 持续跟随：

- `GET /api/v1/namespaces/{ns}/pods/{pod}/log?follow=true&timestamps=true`

每一行日志：

- 尝试解析行首时间戳
- 默认 stream 记成 `stdout`
- 写入 `d.write(LogEntry)`

当前实现没有区分容器原始 stderr；对于学习项目来说已经够用，但语义上是有损的。

## Kubernetes proxy 模式详解

主要实现位于 `logdaemon/shared.go` 的 `proxyClient`。

### 目标

用户请求 gateway：

- `GET /logs/{funcName}`

但内部实际流程变成：

1. 找到这个函数有哪些实例。
2. 找到这些实例分别在哪个 node。
3. 找到这些 node 上对应的 collector Pod。
4. 向对应 collector 拉 `/local/logs/{funcName}`。
5. 合并、排序、裁剪 tail。

### `instances()`

调用 gateway：

- `GET /internal/instances/{funcName}`

获取实例列表，特别是 `nodeName`。

### `collectorsByNode()`

通过 in-cluster Kubernetes API 发现 collector Pod，并建立：

- `nodeName -> collectorPod`

### `fetchCollectorLogs()`

这是 proxy 聚合 collector 日志的路径。

host proxy 先通过 gateway 的实例元数据判断函数实例所在 node，再通过 Kubernetes exec API 进入对应 node collector Pod，在 Pod 内请求 `http://127.0.0.1:9200/local/logs/{func}` 并取回结果。collector 在集群内使用 Kubernetes API 读取 Pod 日志，并通过本地 HTTP 接口返回 ring buffer 中的结果。

也就是说：当前已经是“collector + proxy”架构，但 proxy 到 collector 仍依赖 Kubernetes `pods/exec` 子资源，不是直连 collector HTTP 的生产级数据面。

### `fetchLogs()`

- 按实例收集对应 node 的 collector。
- 避免同一个 collector 被重复请求。
- 把多个 collector 返回的日志拼到一起。
- 按 `Time` 排序。
- 最后应用 `tail`。

## 关键环境变量

### 所有模式共用

- `FAAS_BACKEND`
- `LOGDAEMON_MODE`

### proxy 模式

- `GATEWAY_INTERNAL_ADDR`
- `K8S_NAMESPACE`

### collector 模式

- `K8S_NAMESPACE`
- `MY_NODE_NAME`

### Docker 模式

- 默认使用 `/var/run/docker.sock`

## 源码阅读建议

建议按顺序看：

1. `logdaemon/main.go`
   - 先看模式切换。
2. `logdaemon/shared.go`
   - 看内部 HTTP API、ring buffer、proxy 逻辑。
3. `logdaemon/docker_source.go`
   - 看 Docker 采集路径。
4. `logdaemon/k8s_collector.go`
   - 看 in-cluster API collector 路径。

## 当前限制

- k8s proxy 到 collector 仍依赖 Kubernetes `pods/exec` 子资源，不是真正的高效生产日志数据面。
- collector 是 polling attach 模型，不是 watch/cache。
- collector 当前把 Pod log 统一记成 `stdout`，没有保留 stdout/stderr 原始区分。
- `/local/logs/*` 和 `/logs/*` 都是内存 ring buffer 优先，没有完整持久化与重放语义。
- proxy 模式下 SSE 目前是轮询聚合，不是多路实时 fan-in。
