# ScalerSvc Component

`scalersvc/` 是一个独立的自动扩缩容进程。它不直接管理容器或 Pod，而是通过 HTTP 调 gateway 的内部接口来观察状态、做决策、执行扩缩容。

它实现的是一个非常简化但已经有完整闭环的 autoscaling loop：

- 接收 runtime-agent 上报的指标。
- 周期性拉取函数列表、busy/idle、queue 状态。
- 计算 utilization / p99 / error rate。
- 根据阈值决定 `scale-up`、`scale-down` 或 `none`。
- 调 gateway 的 `/internal/scale-*` 接口执行动作。

## 目录结构

```text
scalersvc/
├── main.go
├── store.go
├── memory_store.go
└── mongo_store.go
```

## 组件职责

`scalersvc` 主要做四件事：

1. 保存每个容器最近一次上报的指标。
2. 维护每个函数最近一次扩缩容决策。
3. 每 5 秒运行一次评估循环。
4. 在配置 Mongo 时持久化最新指标、状态和决策。
5. 暴露状态查询接口，便于调试和测试。

## 监听接口

默认监听 `:9300`。

### `POST /metrics/{containerID}`

接收 gateway 转发过来的 runtime-agent 指标。

body 对应 `ContainerMetrics`。

### `GET /scale/{funcName}`

返回某个函数当前扩缩容视图，包括：

- busy / idle
- queued
- total
- policy
- last decision
- 当前缓存的 metrics 快照

### `GET /health`

返回 `ok`。

## 核心数据结构

### `ContainerMetrics`

与 runtime-agent 上报格式一致：

- `ContainerID`
- `Timestamp`
- `InvocationCount`
- `SuccessCount`
- `ErrorCount`
- `P50LatencyMs`
- `P95LatencyMs`
- `P99LatencyMs`
- `MemoryBytes`
- `CPUUsageUs`

### `policy`

代表当前扩缩容阈值：

- `ScaleUpUtilPct`
- `ScaleUpP99Ms`
- `ScaleUpErrPct`
- `ScaleUpQueueLen`
- `ScaleDownUtilPct`
- `ScaleDownP99Ms`
- `IdleMinutes`

### `ScaleDecision`

记录一次决策：

- `Time`
- `FuncName`
- `Action`
- `Reason`
- `Busy`
- `Idle`

### `ScaleStatus`

用于 `/scale/{funcName}` 返回：

- `FuncName`
- `Busy`
- `Idle`
- `Queued`
- `Total`
- `MaxReplicas`
- `MinReplicas`
- `Policy`
- `LastDecision`
- `Metrics`

### `scaler`

进程内核心对象，包含：

- `gatewayAddr`
- `pol`
- `metrics map[string]ContainerMetrics`
- `decisions map[string]*ScaleDecision`
- `maxReplicas`
- `minReplicas`
- `store ScaleStore`，默认内存实现，设置 `MONGO_URI` 后使用 Mongo 实现

## 运行流程

入口在 `scalersvc/main.go`。

启动时：

1. 读取 `GATEWAY_INTERNAL_ADDR`。
2. 读取策略和副本阈值环境变量。
3. 创建 `newScaler(gatewayAddr)`。
4. `newScaler()` 内部会启动 `evaluateLoop()` 后台循环。
5. 暴露 HTTP 接口。

## 评估循环

### `evaluateLoop()`

- 每 5 秒 tick 一次。
- 先通过 `functionNames()` 从 gateway 拉 `/internal/functions`。
- 对每个函数调用 `evaluate(funcName)`。

### `evaluate()`

这个方法是 scalersvc 的核心。

它依次做：

1. `stats(funcName)`
   - 取 busy / idle。
2. `queueStatus(funcName)`
   - 取当前 queue backlog。
3. `aggregateMetrics(funcName)`
   - 聚合这个函数名下所有实例最近 30 秒的指标。
4. 计算：
   - `total = busy + idle`
   - `util = busy / total`
   - `p99`
   - `errPct`
5. 根据 policy 生成：
   - `action`
   - `reason`
6. 更新 `decisions[funcName]` 并通过 `ScaleStore` 保存决策与状态。
7. 若是 `scale-up` 或 `scale-down`，调用 gateway 内部接口执行。

## 当前策略细节

### scale-up 条件

满足任意一个就扩容：

- `queued >= ScaleUpQueueLen`
- `util >= ScaleUpUtilPct`
- `p99 > ScaleUpP99Ms`
- `errPct > ScaleUpErrPct`

同时还要满足：

- 已有实例数 `total > 0`
- `total < maxReplicas`

### scale-down 条件

必须同时满足：

- `idle > minReplicas`
- `queued == 0`
- `util < ScaleDownUtilPct`
- `p99 == 0` 或 `p99 < ScaleDownP99Ms`

注意这里的真正删除动作仍由 gateway scheduler 决定；scheduler 还会额外检查 idle 时间是否足够长。

## 与 gateway 的协作方式

scalersvc 不直接操作容器，而是依赖 gateway 暴露的内部 HTTP 接口。

### 读状态

- `/internal/functions`
- `/internal/stats/{funcName}`
- `/internal/containers/{funcName}`
- `/internal/queue/{funcName}`

### 执行动作

- `POST /internal/scale-up/{funcName}`
- `POST /internal/scale-down/{funcName}`

## 为什么 `aggregateMetrics()` 要按函数过滤

一个容易踩坑的点是：容器指标不能全局混合。

当前实现会先调用 `containerIDs(funcName)` 从 gateway 拉到“这个函数当前有哪些实例”，然后只聚合这些实例最近 30 秒的数据。这样避免出现：

- `hello-queue` 的队列压测，被 `hello` 的旧指标污染。
- 一个函数的高 p99 导致另一个函数被误判扩容。

这是当前实现里非常重要的边界。

## `handleMetrics()` 逻辑

- 从 URL 中提取 `containerID`。
- body 解码为 `ContainerMetrics`。
- 如果 body 里没有 `ContainerID`，就用路径里的 ID。
- 覆盖写入 `metrics[m.ContainerID] = m`。

这是“最后一次上报覆盖旧值”的模型，而不是时间序列存储。

## `/scale/{funcName}` 的价值

这个接口非常适合测试和调试，因为它不仅告诉你当前副本数，还会告诉你：

- 为什么最近一次决策是 `scale-up` / `scale-down` / `none`
- 当前 policy 是什么
- 当前 metrics 快照里有哪些实例数据

`test.sh` 就大量依赖了这个接口做断言。

## 关键环境变量

### 地址相关

- `GATEWAY_INTERNAL_ADDR`
  - 默认 `localhost:8080`
- `SCALER_LISTEN`
  - 默认 `:9300`
- `MONGO_URI`
  - 为空时使用进程内内存存储
  - 非空时把最新 metrics、status 和 decisions 写入 Mongo
- `MONGO_DB`
  - 默认 `faas`
- `MONGO_TIMEOUT_MS`
  - Mongo 操作超时，默认 `2000`

### 副本阈值

- `MAX_REPLICAS`
- `MIN_REPLICAS`

### scale-up 阈值

- `SCALE_UP_UTIL_PCT`
- `SCALE_UP_P99_MS`
- `SCALE_UP_ERR_PCT`
- `SCALE_UP_QUEUE_LEN`

### scale-down 阈值

- `SCALE_DOWN_UTIL_PCT`
- `SCALE_DOWN_P99_MS`
- `IDLE_MINUTES`

注意：`IDLE_MINUTES` 当前更多是配置语义，真正是否删除 idle 实例，还受 gateway scheduler 中 `RemoveIdle()` 的 2 分钟判断影响。

## 源码阅读建议

建议按顺序阅读：

1. `main()`
   - 看进程如何启动、日志如何打印。
2. `newScaler()`
   - 看状态如何从 `ScaleStore` 加载，以及评估循环如何建立。
3. `evaluateLoop()` 与 `evaluate()`
   - 看策略主逻辑。
4. `aggregateMetrics()`
   - 看按函数过滤指标的边界。
5. `handleMetrics()` 与 `handleStatus()`
   - 看外部接口如何读写状态。

## 当前限制

- Mongo 持久化只保存最新 metrics/status 和决策记录，不是完整时间序列系统。
- `metrics` 快照是全量返回的，还没有按函数过滤到接口层。
- 决策是固定阈值规则，没有 cooldown、窗口平滑或预测式扩容。
- queue、util、p99、err 的优先级是手写顺序，不是可编排策略引擎。
- 如果 `total == 0`，当前策略不会主动从 0 扩到 1，通常要靠第一次真实调用触发冷启动。
