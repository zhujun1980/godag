# GoDAG

**English** | [简体中文](README.zh-CN.md)

GoDAG is a lightweight DAG (Directed Acyclic Graph) workflow execution engine written in Go.

It lets you describe a graph of tasks in YAML, register your own task implementations in Go, and execute the graph with **automatic dependency resolution, parallel scheduling, per-node/graph timeouts, fault-tolerant failover, conditional branching, and Airflow-style trigger rules**. It is designed for low-latency online services (e.g. recommendation / retrieval pipelines) where many independent steps must run concurrently under a strict latency budget.

## Features

- **Declarative graphs** — define nodes and edges in YAML, implement logic in Go.
- **Parallel scheduling** — independent ready nodes run concurrently; downstream nodes start as soon as their dependencies are satisfied.
- **Timeouts** — both graph-level and per-node timeouts via `context`.
- **Failover** — fall back to a `Failover()` path on error and/or timeout, optionally running it in parallel with the main `Execute()`.
- **Conditional branching** — a branch node selects which downstream path(s) to open; unselected paths are skipped.
- **Trigger conditions** — 10 Airflow-style rules (`all_success`, `one_failed`, `none_failed_min_one_success`, …) control when a node runs based on the state of its upstreams.
- **Pluggable nodes** — register node types by name through a factory registry.

## Use Cases

GoDAG is an **in-process, request-scoped task orchestration engine** for online,
latency-sensitive services. It fits best when a single request (or execution)
needs to run many interdependent steps under a tight latency budget, with
parallelism, per-node timeouts, and graceful degradation.

**Well suited for:**

- **Online recommendation / retrieval / search pipelines** — the bundled
  [`examples/recsys.yaml`](/examples/recsys.yaml): rate limit → downgrade branch
  → recall → features → filter → rank → rerank → output. Features are fetched in
  parallel, slow ones fall back via `parallel_failover` + `failover_on_timeout`,
  and a branch node degrades to a hot-list or emergency path under load.
- **Service orchestration / BFF aggregation** — fan out to multiple downstreams
  (user, product, inventory, price, risk) that have both dependencies and
  parallelism; degrade non-critical calls to cached/default values on error
  instead of failing the whole request.
- **Risk control / fraud-decision flows** — run scoring rules and models in
  parallel, then combine them with trigger conditions (`one_failed` to reject on
  any hard-rule hit, `none_failed_min_one_success` for soft rules) and branch to
  *allow / review / reject*.
- **Request-time feature computation** — multi-source fetch → clean → derive →
  assemble, with slow features degrading without blocking the main path.
- **Multi-model inference with fallback (incl. LLM/RAG)** — e.g. parallel recall
  → rerank → primary model, with `parallel_failover` switching to a faster small
  model or a cached answer when the primary is slow or errors.

**Not designed for** (out of the engine's current scope):

- **Cross-process / distributed scheduling** — it is an in-process library with
  no persistence, worker cluster, or durable retries (it is not Airflow /
  Temporal).
- **Long-running batch / ETL** (minutes to hours) — the timeout model and
  fully-in-memory design target millisecond-scale work.
- **State persistence, resume-from-checkpoint, or cron-style scheduling.**
- **Dynamic graphs** — the graph is fixed at `LoadGraph` time; nodes cannot be
  added or removed at runtime (branching only skips or activates already-declared
  nodes).

## Installation

```bash
go get github.com/zhujun1980/godag
```

```go
import dag "github.com/zhujun1980/godag"
```

The package name is `dag`. Requires Go 1.23+.

## Core Concepts

| Type | Description |
|------|-------------|
| `DAG` | A compiled graph: nodes plus their incoming/outgoing edges. Built by `LoadGraph`. |
| `Node` | The interface every task implements. Embed `BaseNode` to inherit defaults and YAML config. |
| `NodeFactory` | `func(context.Context) (Node, error)` — registered by name and used to instantiate nodes from a graph's `kind`. |
| `ExecutionState` | The result of running a graph: per-node results, per-node states, and the overall graph state. |
| `NodeResult` | A single node's output (`Result`, `Err`, duration, failover info). |
| `TerminationState` | `TERMINATE_SUCCESS`, `TERMINATE_FAILED`, or `TERMINATE_SKIPPED`. |

## Quick Start

### 1. Implement a node

Embed `BaseNode` and override `Execute` (and optionally `Failover`, `OnFinished`, `Init`). Your struct's YAML tags map to the node `spec`.

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
	// Read an upstream result if you need it:
	//   up := state.GetResult("some-upstream-node")
	return dag.Result("hello from " + n.Data)
}
```

### 2. Register the node type

```go
dag.RegisterNodeFactory("example/greet", func(ctx context.Context) (dag.Node, error) {
	return &GreetNode{}, nil
})
```

The registry name (`example/greet`) is what you reference as `kind` in YAML. `core/basenode` is registered out of the box and simply returns a default result.

### 3. Describe the graph in YAML

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

Here `a` and `b` have no dependencies and run in parallel; `c` runs once both succeed.

### 4. Load and execute

```go
graph, err := dag.LoadGraph(context.Background(), strings.NewReader(yamlText))
if err != nil {
	log.Fatal(err)
}

state, err := graph.Execute(context.Background())
if err != nil {
	// non-nil err means the graph timed out
	log.Printf("graph timed out: %v", err)
}

fmt.Println("graph state:", state.GraphState) // TERMINATE_SUCCESS / TERMINATE_FAILED
fmt.Println("c result:", state.GetResult("c").Result)
fmt.Println("c node state:", state.GetState("c"))
fmt.Println("elapsed:", state.Dura)
```

## YAML Configuration Reference

### Graph

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Graph name. |
| `timeout` | duration string | Overall graph timeout (e.g. `800ms`, `2s`). Optional; defaults to effectively unbounded. |
| `nodes` | map | `node_name → { kind, spec }`. |
| `edges` | map | `node_name → [downstream_node_names]`. Defines dependencies (`from → to`). |

### Node `spec` (fields provided by `BaseNode`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | the map key | Optional explicit node name. |
| `timeout` | duration string | none | Per-node timeout (e.g. `70ms`). |
| `failover_on_error` | bool | `false` | Call `Failover()` if `Execute()` returns an error. |
| `failover_on_timeout` | bool | `false` | Call `Failover()` if the node times out. |
| `parallel_failover` | bool | `false` | Run `Failover()` concurrently with `Execute()`. Must be combined with `failover_on_error` and/or `failover_on_timeout`. |
| `is_branch` | bool | `false` | Mark this node as a branch node (see [Branching](#branching)). |
| `branchs` | map | none | `branch_value → [downstream_node_names]`. Required iff `is_branch: true`. |
| `trigger_condition` | string | `all_success` | When this node should run (see [Trigger Conditions](#trigger-conditions)). |

You can add your own fields with YAML tags on your node struct (like `my-data` above); they are decoded from the same `spec` map.

## Trigger Conditions

A node's trigger condition decides whether it runs, based on the terminal states of its **upstream** nodes. Set it via `trigger_condition` in the node `spec`.

| Value | Runs when… |
|-------|------------|
| `all_success` *(default)* | All upstreams succeeded. |
| `all_failed` | All upstreams failed. |
| `all_skipped` | All upstreams were skipped. |
| `all_done` | All upstreams finished (success, failed, or skipped). |
| `one_failed` | At least one upstream failed. |
| `one_success` | At least one upstream succeeded. |
| `one_done` | At least one upstream succeeded or failed. |
| `none_failed` | No upstream failed (all succeeded or were skipped). |
| `none_failed_min_one_success` | No upstream failed **and** at least one succeeded. |
| `none_skipped` | No upstream was skipped (all succeeded or failed). |

## Branching

A branch node decides at runtime which downstream path(s) to activate. Set `is_branch: true` and provide `branchs`. The node's `Execute()` returns a **string** (via `dag.Result("...")`) that selects the matching branch key; only the nodes listed under that key are opened, and the other branch targets are marked `TERMINATE_SKIPPED`.

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
	return dag.Result("200") // opens "serve_ok", skips "serve_error"
}
```

Rules enforced at load time:

- Every branch target must be declared as a downstream edge of the branch node.
- A node with `is_branch: true` must declare non-empty `branchs`; a non-branch node must not declare `branchs`.

## Failover & Timeouts

Each node may define fallback behavior through `Failover()`:

- **`failover_on_error`** — if `Execute()` returns a `NodeResult` with a non-nil `Err`, `Failover()` is invoked and its result is used.
- **`failover_on_timeout`** — if the node's context deadline is reached, `Failover()` is invoked.
- **`parallel_failover`** — `Failover()` runs concurrently with `Execute()` from the start, so a fallback is already in flight when the primary path fails or times out. Keep `Failover()` fast; on timeout, if the parallel failover has not produced a result yet, the node returns the timeout error.

When a failover result is used, the original primary result is preserved:

```go
res := state.GetResult("my-node")
if res.IsFailover {
	fmt.Println("served from failover:", res.Result)
	fmt.Println("original error:", res.ResultOnErr.Err)
}
```

Timeouts compose: a node uses `min(node timeout, remaining graph timeout)`. If the **graph** timeout fires, `Execute` returns promptly with the context error; in-flight node goroutines are drained before returning so the returned `state` is safe to read.

## Reading Results

`Execute` returns an `*ExecutionState`:

```go
state, err := graph.Execute(ctx)

state.GraphState              // TERMINATE_SUCCESS or TERMINATE_FAILED
state.Dura                    // total wall-clock duration

r := state.GetResult("node")  // *NodeResult
r.Result                      // any — whatever the node returned via dag.Result(...)
r.Err                         // error, if the node failed
r.Dura                        // node duration
r.IsFailover                  // true if served from Failover()
r.ResultOnErr                 // original result when IsFailover is true

state.GetState("node")        // TERMINATE_SUCCESS / TERMINATE_FAILED / TERMINATE_SKIPPED
```

The graph is considered failed (`TERMINATE_FAILED`) if any **leaf** node (a node with no outgoing edges) ended in `TERMINATE_FAILED`; otherwise it is `TERMINATE_SUCCESS`.

## Execution Semantics

1. `LoadGraph` parses the YAML, instantiates every node via its factory, validates branch/failover constraints, and computes the dependency (in-degree) of each node.
2. `Execute` starts all nodes whose in-degree is `0`, each in its own goroutine.
3. When a node finishes, the engine decrements its downstreams' dependency counts and evaluates each downstream's trigger condition, branch membership, and upstream states to decide whether it becomes ready or skipped.
4. Newly-ready nodes are scheduled; this repeats until no nodes remain.

### Concurrency note

Sibling nodes run **in parallel**. If your nodes share mutable state (for example a request object passed through `context`), you are responsible for synchronizing access to it. The engine guarantees safe, race-free access to its own `ExecutionState`, but it does not lock data owned by your node implementations.

## Example: Recommendation System

A more complete graph modeling a recommendation "top-nav" pipeline (rate limiting → downgrade branching → recall → features → filter → rank → rerank → output) is provided in [`examples/recsys.yaml`](/examples/recsys.yaml):

![Recommendation System](/examples/recsys.svg)

## Testing

```bash
go test ./...            # run the suite
go test -race ./...      # run with the data-race detector
```

## License

See [LICENSE](/LICENSE).
