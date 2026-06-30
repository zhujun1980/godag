# GoDAG

[English](README.md) | **简体中文**

GoDAG 是一个用 Go 编写的轻量级 DAG（有向无环图）工作流执行引擎。

你可以用 YAML 描述任务图、用 Go 实现各个任务，然后执行整个图。引擎提供**自动依赖解析、并行调度、节点级/图级超时、容错降级、条件分支，以及 Airflow 风格的触发规则**。它面向低延迟在线服务（例如推荐/检索链路）——在严格的延迟预算下，需要并发执行大量相互独立的步骤。

## 特性

- **声明式图** —— 用 YAML 定义节点与边，用 Go 实现逻辑。
- **并行调度** —— 相互独立的就绪节点并发执行；下游节点在其依赖满足后立即启动。
- **超时控制** —— 基于 `context`，支持图级和节点级超时。
- **容错降级** —— 出错和/或超时时回退到 `Failover()` 路径，可选择与主 `Execute()` 并行执行。
- **条件分支** —— 分支节点决定打开哪条（些）下游路径，未选中的路径被跳过。
- **触发条件** —— 10 种 Airflow 风格规则（`all_success`、`one_failed`、`none_failed_min_one_success` 等），根据上游状态控制节点何时运行。
- **可插拔节点** —— 通过工厂注册表按名称注册节点类型。

## 应用场景

GoDAG 是一个**面向在线、低延迟服务的进程内请求级任务编排引擎**。它最适合这样的场景：单次请求（或单次执行）内需要在严格的延迟预算下运行大量相互依赖的步骤，同时需要并行、节点级超时和优雅降级。

**适合：**

- **在线推荐 / 检索 / 搜索链路** —— 即自带的 [`examples/recsys.yaml`](/examples/recsys.yaml)：限流 → 降级分支 → 召回 → 特征 → 过滤 → 排序 → 重排 → 输出。特征并行拉取，慢特征通过 `parallel_failover` + `failover_on_timeout` 走兜底，分支节点在高负载时降级到热榜或兜底链路。
- **服务编排 / BFF 聚合** —— 扇出调用多个下游（用户、商品、库存、价格、风控），它们之间既有依赖又有并行；非关键调用出错时降级为缓存/默认值，而不是让整个请求失败。
- **风控 / 反欺诈决策流** —— 并行运行评分规则与模型，再用触发条件组合结论（`one_failed` 任一硬规则命中即拒绝，`none_failed_min_one_success` 用于软规则），并分支到 *放行 / 人工审核 / 拒绝*。
- **请求级特征计算** —— 多源取数 → 清洗 → 衍生 → 组装，慢特征降级而不阻塞主路径。
- **带兜底的多模型推理（含 LLM/RAG）** —— 例如并行召回 → 重排 → 主模型，当主模型变慢或报错时，`parallel_failover` 切换到更快的小模型或缓存答案。

**不适合**（超出引擎当前范围）：

- **跨进程 / 分布式调度** —— 它是进程内库，没有持久化、worker 集群或持久化重试（它不是 Airflow / Temporal）。
- **长时间运行的批处理 / ETL**（分钟到小时级）—— 其超时模型和全内存设计面向毫秒级任务。
- **状态持久化、断点续跑、或 cron 式定时调度。**
- **动态图** —— 图在 `LoadGraph` 时即固定；运行期不能增删节点（分支只是跳过或激活已声明的节点）。

## 安装

```bash
go get github.com/zhujun1980/godag
```

```go
import dag "github.com/zhujun1980/godag"
```

包名为 `dag`。需要 Go 1.23+。

## 核心概念

| 类型 | 说明 |
|------|------|
| `DAG` | 编译后的图：节点及其入边/出边。由 `LoadGraph` 构建。 |
| `Node` | 每个任务实现的接口。嵌入 `BaseNode` 即可继承默认实现与 YAML 配置。 |
| `NodeFactory` | `func(context.Context) (Node, error)` —— 按名称注册，用于根据图中的 `kind` 实例化节点。 |
| `ExecutionState` | 运行图后的结果：各节点结果、各节点状态、以及整体图状态。 |
| `NodeResult` | 单个节点的输出（`Result`、`Err`、耗时、容错信息）。 |
| `TerminationState` | `TERMINATE_SUCCESS`、`TERMINATE_FAILED` 或 `TERMINATE_SKIPPED`。 |

## 快速开始

### 1. 实现一个节点

嵌入 `BaseNode` 并重写 `Execute`（可选地重写 `Failover`、`OnFinished`、`Init`）。结构体的 YAML 标签会映射到节点的 `spec`。

```go
package main

import (
	"context"
	"fmt"

	dag "github.com/zhujun1980/godag"
)

type GreetNode struct {
	dag.BaseNode `yaml:",inline"`
	Data         string `yaml:"my-data"`
}

func (n *GreetNode) Execute(ctx context.Context, state *dag.ExecutionState) *dag.NodeResult {
	// 如果需要，可以读取上游结果：
	//   up := state.GetResult("some-upstream-node")
	return dag.Result("hello from " + n.Data)
}
```

### 2. 注册节点类型

```go
dag.RegisterNodeFactory("example/greet", func(ctx context.Context) (dag.Node, error) {
	return &GreetNode{}, nil
})
```

注册名（`example/greet`）就是你在 YAML 中通过 `kind` 引用的名字。`core/basenode` 已内置注册，它只返回一个默认结果。

### 3. 用 YAML 描述图

```yaml
name: hello-graph
timeout: 800ms

nodes:
  a:
    kind: example/greet
    spec:
      my-data: A
  b:
    kind: example/greet
    spec:
      my-data: B
  c:
    kind: example/greet
    spec:
      my-data: C

edges:
  a:
    - c
  b:
    - c
```

这里 `a` 和 `b` 没有依赖，会并行执行；`c` 在两者都成功后运行。

### 4. 加载并执行

```go
graph, err := dag.LoadGraph(context.Background(), strings.NewReader(yamlText))
if err != nil {
	log.Fatal(err)
}

state, err := graph.Execute(context.Background())
if err != nil {
	// err 非 nil 表示图超时
	log.Printf("graph timed out: %v", err)
}

fmt.Println("graph state:", state.GraphState) // TERMINATE_SUCCESS / TERMINATE_FAILED
fmt.Println("c result:", state.GetResult("c").Result)
fmt.Println("c node state:", state.GetState("c"))
fmt.Println("elapsed:", state.Dura)
```

## YAML 配置参考

### 图（Graph）

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 图名称。 |
| `timeout` | duration 字符串 | 图整体超时（如 `800ms`、`2s`）。可选；默认近似无限大。 |
| `nodes` | map | `节点名 → { kind, spec }`。 |
| `edges` | map | `节点名 → [下游节点名]`。定义依赖关系（`from → to`）。 |

### 节点 `spec`（由 `BaseNode` 提供的字段）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | string | map 的 key | 可选的显式节点名。 |
| `timeout` | duration 字符串 | 无 | 节点级超时（如 `70ms`）。 |
| `failover_on_error` | bool | `false` | 当 `Execute()` 返回错误时调用 `Failover()`。 |
| `failover_on_timeout` | bool | `false` | 当节点超时时调用 `Failover()`。 |
| `parallel_failover` | bool | `false` | 让 `Failover()` 与 `Execute()` 并行执行。必须与 `failover_on_error` 和/或 `failover_on_timeout` 一起使用。 |
| `is_branch` | bool | `false` | 将该节点标记为分支节点（见[分支](#分支)）。 |
| `branchs` | map | 无 | `分支值 → [下游节点名]`。当 `is_branch: true` 时必填。 |
| `trigger_condition` | string | `all_success` | 该节点何时运行（见[触发条件](#触发条件)）。 |

你可以在节点结构体上用 YAML 标签添加自定义字段（如上面的 `my-data`），它们会从同一个 `spec` map 中解码。

## 触发条件

节点的触发条件根据其**上游**节点的终止状态决定它是否运行。通过节点 `spec` 中的 `trigger_condition` 设置。

| 取值 | 运行条件 |
|------|----------|
| `all_success` *（默认）* | 所有上游都成功。 |
| `all_failed` | 所有上游都失败。 |
| `all_skipped` | 所有上游都被跳过。 |
| `all_done` | 所有上游都已结束（成功、失败或跳过）。 |
| `one_failed` | 至少一个上游失败。 |
| `one_success` | 至少一个上游成功。 |
| `one_done` | 至少一个上游成功或失败。 |
| `none_failed` | 没有上游失败（全部成功或被跳过）。 |
| `none_failed_min_one_success` | 没有上游失败**且**至少一个成功。 |
| `none_skipped` | 没有上游被跳过（全部成功或失败）。 |

## 分支

分支节点在运行时决定激活哪条（些）下游路径。设置 `is_branch: true` 并提供 `branchs`。该节点的 `Execute()` 返回一个**字符串**（通过 `dag.Result("...")`）来选择匹配的分支键；只有该键下列出的节点会被打开，其余分支目标被标记为 `TERMINATE_SKIPPED`。

```yaml
nodes:
  router:
    kind: example/router
    spec:
      is_branch: true
      branchs:
        "200":
          - serve_ok
        "500":
          - serve_error
  serve_ok:   { kind: example/handler, spec: }
  serve_error:{ kind: example/handler, spec: }

edges:
  router:
    - serve_ok
    - serve_error
```

```go
func (n *RouterNode) Execute(ctx context.Context, state *dag.ExecutionState) *dag.NodeResult {
	return dag.Result("200") // 打开 "serve_ok"，跳过 "serve_error"
}
```

加载期强制校验的规则：

- 每个分支目标都必须声明为分支节点的下游边。
- `is_branch: true` 的节点必须声明非空的 `branchs`；非分支节点不得声明 `branchs`。

## 容错与超时

每个节点都可以通过 `Failover()` 定义回退行为：

- **`failover_on_error`** —— 如果 `Execute()` 返回的 `NodeResult` 带有非 nil 的 `Err`，则调用 `Failover()` 并使用其结果。
- **`failover_on_timeout`** —— 如果到达节点的 context 截止时间，则调用 `Failover()`。
- **`parallel_failover`** —— `Failover()` 从一开始就与 `Execute()` 并发执行，这样在主路径失败或超时时，回退已经在进行中。请保持 `Failover()` 快速返回；超时时，如果并行的 failover 还没产出结果，节点将返回超时错误。

当使用了 failover 结果时，原始的主结果会被保留：

```go
res := state.GetResult("my-node")
if res.IsFailover {
	fmt.Println("served from failover:", res.Result)
	fmt.Println("original error:", res.ResultOnErr.Err)
}
```

超时会组合生效：节点使用 `min(节点超时, 剩余图超时)`。如果**图**超时触发，`Execute` 会迅速返回并带上 context 错误；返回前会先排空仍在执行的节点 goroutine，因此返回的 `state` 可以安全读取。

## 读取结果

`Execute` 返回一个 `*ExecutionState`：

```go
state, err := graph.Execute(ctx)

state.GraphState              // TERMINATE_SUCCESS 或 TERMINATE_FAILED
state.Dura                    // 总耗时（wall-clock）

r := state.GetResult("node")  // *NodeResult
r.Result                      // any —— 节点通过 dag.Result(...) 返回的内容
r.Err                         // error，节点失败时
r.Dura                        // 节点耗时
r.IsFailover                  // 若由 Failover() 提供结果则为 true
r.ResultOnErr                 // IsFailover 为 true 时的原始结果

state.GetState("node")        // TERMINATE_SUCCESS / TERMINATE_FAILED / TERMINATE_SKIPPED
```

如果任意**叶子**节点（没有出边的节点）以 `TERMINATE_FAILED` 结束，则整个图被视为失败（`TERMINATE_FAILED`）；否则为 `TERMINATE_SUCCESS`。

## 执行语义

1. `LoadGraph` 解析 YAML，通过工厂实例化每个节点，校验分支/容错约束，并计算每个节点的依赖（入度）。
2. `Execute` 启动所有入度为 `0` 的节点，每个节点在自己的 goroutine 中运行。
3. 当一个节点结束时，引擎递减其下游节点的依赖计数，并评估每个下游的触发条件、分支归属和上游状态，决定它是变为就绪还是被跳过。
4. 新就绪的节点被调度执行；如此循环，直到没有节点剩余。

### 并发注意事项

兄弟节点是**并行**执行的。如果你的节点共享可变状态（例如通过 `context` 传递的请求对象），你需要自行同步对它的访问。引擎保证对自身 `ExecutionState` 的访问安全、无竞态，但它不会为你的节点实现所拥有的数据加锁。

## 示例：推荐系统

[`examples/recsys.yaml`](/examples/recsys.yaml) 提供了一个更完整的图，建模推荐「顶部导航」链路（限流 → 降级分支 → 召回 → 特征 → 过滤 → 排序 → 重排 → 输出）：

![Recommendation System](/examples/recsys.svg)

## 测试

```bash
go test ./...            # 运行测试套件
go test -race ./...      # 带数据竞态检测器运行
```

## 许可证

见 [LICENSE](/LICENSE)。
