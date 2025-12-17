package dag

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	MaxNodeTimeoutDuration = time.Duration(1<<63 - 1) // 290 years
)

var (
	nodeFactorys = map[string]NodeFactory{}

	_ = LogFatalIfError(RegisterNodeFactory("core/basenode", func(ctx context.Context) (Node, error) {
		return &BaseNode{}, nil
	}))

	SuccessfulResult = Result(0)
)

func LogFatalIfError(err error) error {
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

type defaultFailoverResult struct{}

type defaultResult struct{}

type NodeResult struct {
	Result      any
	Err         error
	Dura        time.Duration
	IsFailover  bool
	ResultOnErr *NodeResult
}

func Result(ret any) *NodeResult {
	return &NodeResult{
		Result: ret,
	}
}

func ErrorResult(err error) *NodeResult {
	return &NodeResult{
		Err: err,
	}
}

func (ret *NodeResult) IsDefaultFailover() bool {
	_, ok := ret.Result.(defaultFailoverResult)
	return ok
}

func (ret *NodeResult) IsDefaultResult() bool {
	_, ok := ret.Result.(defaultResult)
	return ok
}

type TriggerCondition int

const (
	// (default): All upstream tasks have succeeded
	ALL_SUCCESS TriggerCondition = iota
	// All upstream tasks are in a failed state
	ALL_FAILED
	// All upstream tasks are in a skipped state
	ALL_SKIPPED
	// All upstream tasks are done with their execution
	ALL_DONE
	// At least one upstream task has failed (does not wait for all upstream tasks to be done)
	ONE_FAILED
	// At least one upstream task has succeeded (does not wait for all upstream tasks to be done)
	ONE_SUCCESS
	// At least one upstream task succeeded or failed
	ONE_DONE
	// All upstream tasks have not failed - that is, all upstream tasks have succeeded or been skipped
	NONE_FAILED
	// All upstream tasks have not failed, and at least one upstream task has succeeded.
	NONE_FAILED_MIN_ONE_SUCCESS
	// No upstream task is in a skipped state - that is, all upstream tasks are in a success or failed state
	NONE_SKIPPED
)

var (
	triggerConditionMap = map[string]TriggerCondition{
		"all_success":                 ALL_SUCCESS,
		"all_failed":                  ALL_FAILED,
		"all_skipped":                 ALL_SKIPPED,
		"all_done":                    ALL_DONE,
		"one_failed":                  ONE_FAILED,
		"one_success":                 ONE_SUCCESS,
		"one_done":                    ONE_DONE,
		"none_failed":                 NONE_FAILED,
		"none_failed_min_one_success": NONE_FAILED_MIN_ONE_SUCCESS,
		"none_skipped":                NONE_SKIPPED,
	}
)

type TerminationState int

const (
	TERMINATE_SUCCESS TerminationState = iota
	TERMINATE_FAILED
	TERMINATE_SKIPPED
)

type RunningState int

const (
	NODE_NOT_STARTED RunningState = iota
	NODE_RUNNING
	NODE_FINISHED
)

type Node interface {
	Init(ctx context.Context, name string) error

	Name() string

	// 是否在失败时进行容错
	FailoverOnError() bool

	// 是否在超时时进行容错
	FailoverOnTimeout() bool

	// 是否并行执行容错流程
	ParallelFailover() bool

	// 是否是分支节点
	IsBranch() bool

	// 获取所有的分支名称
	GetBranchNames() []string

	// 获取指定分支的节点列表
	GetBranchs(name string) []string

	// 创建附带当前节点超时时间的上下文
	WithContext(parent context.Context) (context.Context, context.CancelFunc)

	// 执行当前节点
	Execute(ctx context.Context, state *ExecutionState) *NodeResult

	// 执行容错流程
	Failover(ctx context.Context, state *ExecutionState) *NodeResult

	// 执行完成后的处理
	OnFinished(ctx context.Context, state *ExecutionState, result *NodeResult)

	// 获取触发条件
	GetTriggerCondition() TriggerCondition
}

type BaseNode struct {
	NoName                string              `yaml:"name"`
	Timeout               string              `yaml:"timeout,omitempty"`
	NeedFailoverOnTimeout bool                `yaml:"failover_on_timeout,omitempty"`
	NeedFailoverOnError   bool                `yaml:"failover_on_error,omitempty"`
	NeedParallelFailover  bool                `yaml:"parallel_failover,omitempty"`
	IsBranchNode          bool                `yaml:"is_branch,omitempty"`
	Branchs               map[string][]string `yaml:"branchs,omitempty"`
	TriggerConditionConf  string              `yaml:"trigger_condition,omitempty"`
	TriggerCondition      TriggerCondition
	TimeoutDuration       time.Duration
}

func (node *BaseNode) Name() string {
	return node.NoName
}

func (node *BaseNode) FailoverOnTimeout() bool {
	return node.NeedFailoverOnTimeout
}

func (node *BaseNode) FailoverOnError() bool {
	return node.NeedFailoverOnError
}

func (node *BaseNode) ParallelFailover() bool {
	return node.NeedParallelFailover
}

func (node *BaseNode) IsBranch() bool {
	return node.IsBranchNode
}

func (node *BaseNode) GetBranchNames() []string {
	var names []string
	for n := range node.Branchs {
		names = append(names, n)
	}
	return names
}

func (node *BaseNode) GetBranchs(name string) []string {
	return node.Branchs[name]
}

func (node *BaseNode) Init(ctx context.Context, name string) error {
	if node.Timeout != "" {
		if duration, err := time.ParseDuration(node.Timeout); err == nil {
			node.TimeoutDuration = duration
		} else {
			return fmt.Errorf("parse timeout `%s` failed: %s", node.Timeout, err)
		}
	}
	if node.NoName == "" {
		node.NoName = name
	}
	if node.TriggerConditionConf != "" {
		if _, ok := triggerConditionMap[strings.ToLower(node.TriggerConditionConf)]; !ok {
			return fmt.Errorf("trigger condition `%s` not exists", node.TriggerConditionConf)
		}
		node.TriggerCondition = triggerConditionMap[strings.ToLower(node.TriggerConditionConf)]
	} else {
		node.TriggerCondition = ALL_SUCCESS
	}
	return nil
}

func (node *BaseNode) WithContext(parent context.Context) (context.Context, context.CancelFunc) {
	if node.Timeout == "" {
		return parent, func() {}
	}
	// fmt.Printf("time %s, [Graph] BaseNode `%s`- `%v` is running 2\n", time.Now().UTC().Format("15:04:05"), node.Timeout, node.TimeoutDuration)
	return context.WithTimeout(parent, node.TimeoutDuration)
}

func (node *BaseNode) Execute(parent context.Context, state *ExecutionState) *NodeResult {
	return &NodeResult{
		Result: defaultResult{},
		Err:    nil,
	}
}

func (node *BaseNode) Failover(parent context.Context, state *ExecutionState) *NodeResult {
	return &NodeResult{
		Result: defaultFailoverResult{},
		Err:    nil,
	}
}

func (node *BaseNode) OnFinished(parent context.Context, state *ExecutionState, result *NodeResult) {
}

func (node *BaseNode) GetTriggerCondition() TriggerCondition {
	return node.TriggerCondition
}

type NodeFactory func(context.Context) (Node, error)

func RegisterNodeFactory(name string, factory NodeFactory) error {
	if _, ok := nodeFactorys[name]; ok {
		return fmt.Errorf("factory `%s` already exists", name)
	}
	nodeFactorys[name] = factory
	return nil
}

func CreateNode(ctx context.Context, name string) (node Node, err error) {
	f, ok := nodeFactorys[name]
	if !ok {
		err = fmt.Errorf("factory `%s` not exists", name)
		return
	}
	node, err = f(ctx)
	return
}

type DAG struct {
	Name     string
	Timeout  time.Duration
	Symbols  map[string]int
	Nodes    []Node
	Outgoing [][]int
	Incoming [][]int
}

type ExecutionState struct {
	Readys      []int
	GraphState  TerminationState
	Dependances []int
	Results     []*NodeResult
	States      []TerminationState
	Running     []RunningState
	Dura        time.Duration
	Symbols     map[string]int
	Done        chan int
}

func (state *ExecutionState) GetResult(name string) *NodeResult {
	return state.Results[state.Symbols[name]]
}

func (state *ExecutionState) GetState(name string) TerminationState {
	return state.States[state.Symbols[name]]
}

func (state *ExecutionState) BranchResult(idx int) string {
	if v, ok := state.Results[idx].Result.(string); ok {
		return v
	}
	return ""
}

func NewExecutionState(g *DAG) (state *ExecutionState, err error) {
	readys := make([]int, 0)
	dependances := make([]int, len(g.Nodes))
	results := make([]*NodeResult, len(g.Nodes))
	states := make([]TerminationState, len(g.Nodes))
	running := make([]RunningState, len(g.Nodes))
	done := make(chan int, len(g.Nodes))

	// 计算依赖
	for _, wlist := range g.Outgoing {
		for _, w := range wlist {
			dependances[w]++
		}
	}

	// 当前 readys
	for v, dep := range dependances {
		if dep == 0 {
			readys = append(readys, v)
		}
	}

	state = &ExecutionState{
		Readys:      readys,
		Dependances: dependances,
		Results:     results,
		States:      states,
		Running:     running,
		Symbols:     g.Symbols,
		Done:        done,
	}
	return
}

func (g *DAG) runNode(ctx context.Context, idx int, state *ExecutionState) {
	done := make(chan int, 2)
	var result *NodeResult
	var failoverResult *NodeResult
	newctx, cancel := g.Nodes[idx].WithContext(ctx)

	defer func() {
		cancel()
		g.Nodes[idx].OnFinished(newctx, state, state.Results[idx])
	}()

	go func() {
		result = g.Nodes[idx].Execute(newctx, state)
		done <- 1
	}()

	if g.Nodes[idx].ParallelFailover() {
		go func() {
			failoverResult = g.Nodes[idx].Failover(newctx, state)
			done <- 2
		}()
	}

loopSelect:
	for {
		select {
		case <-newctx.Done():
			// 此时 Execute() 可能没有返回，也可能返回但有错误，而且并行的 Failover() 还没有结束
			if g.Nodes[idx].FailoverOnTimeout() {
				// 调用 failover 方法进行容错
				if g.Nodes[idx].ParallelFailover() {
					if failoverResult != nil {
						state.Results[idx] = failoverResult
					} else {
						// 并行的 Failover() 也超时了，直接返回错误
						// 说明实现的不合理，Failover() 应该快速返回结果
						state.Results[idx] = ErrorResult(newctx.Err())
					}
				} else {
					state.Results[idx] = g.Nodes[idx].Failover(newctx, state)
				}
				state.Results[idx].IsFailover = true
				if result != nil {
					state.Results[idx].ResultOnErr = result
				} else {
					state.Results[idx].ResultOnErr = ErrorResult(newctx.Err())
				}
			} else {
				// 不处理超时
				if result != nil {
					// Execute() 已经返回但出错，且并行的 Failover() 在超时时间达到后还没有结束
					// Failover() 设计不合理，或遇到异常
					// 正常情况下 Failover() 应快速返回结果
					state.Results[idx] = result
				} else {
					state.Results[idx] = ErrorResult(newctx.Err())
				}
			}
			break loopSelect
		case <-done:
			if result != nil {
				if result.Err != nil && g.Nodes[idx].FailoverOnError() {
					// 调用 failover 方法进行容错
					if g.Nodes[idx].ParallelFailover() {
						if failoverResult == nil {
							// Failover() 还没有返回，等待结果
							// 这是有可能的：比如 Execute() 遇到一个错误可能比 Failover() 更快结束
							continue
						}
					} else {
						failoverResult = g.Nodes[idx].Failover(newctx, state)
					}
					state.Results[idx] = failoverResult
					state.Results[idx].IsFailover = true
					state.Results[idx].ResultOnErr = result // 保存 Execute() 的结果
				} else {
					state.Results[idx] = result
				}
				break loopSelect
			}
		}
	}
}

func (g *DAG) Execute(ctx context.Context) (state *ExecutionState, err error) {
	startts := time.Now().UTC()
	state, err = NewExecutionState(g)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, g.Timeout)
	defer func() {
		cancel()
		state.Dura = time.Now().UTC().Sub(startts)
	}()

	running := 0
	for {
		if len(state.Readys) == 0 {
			break
		}
		for _, vidx := range state.Readys {
			go func(idx int) {
				start := time.Now().UTC()
				state.Running[idx] = NODE_RUNNING
				g.runNode(ctx, idx, state)
				state.Running[idx] = NODE_FINISHED
				state.Results[idx].Dura = time.Now().UTC().Sub(start)
				state.Done <- idx
			}(vidx)
			running++
		}
		state.Readys = nil
		for running > 0 {
			select {
			case <-ctx.Done():
				// 图超时
				return state, ctx.Err()
			case finishedIdx := <-state.Done:
				running--
				// 节点执行完毕，设置状态
				if state.Results[finishedIdx].Err != nil {
					// 执行失败
					state.States[finishedIdx] = TERMINATE_FAILED
				} else {
					// 执行成功
					state.States[finishedIdx] = TERMINATE_SUCCESS
				}
				g.onNodeDone(finishedIdx, state)
			}
			if len(state.Readys) > 0 {
				break
			}
		}
	}

	// 设置图状态，有一个叶子节点失败，则图失败
	for idx, outgoing := range g.Outgoing {
		if len(outgoing) == 0 && state.States[idx] == TERMINATE_FAILED {
			state.GraphState = TERMINATE_FAILED
			return
		}
	}
	state.GraphState = TERMINATE_SUCCESS
	return
}

func (g *DAG) onNodeDone(finishedIdx int, state *ExecutionState) {
	// fmt.Printf("node %s finished\n", g.Nodes[finishedIdx].Name())
	branchsOpened := make([]int, 0)

	if g.Nodes[finishedIdx].IsBranch() && state.States[finishedIdx] == TERMINATE_SUCCESS {
		name := state.BranchResult(finishedIdx)
		branchs := g.Nodes[finishedIdx].GetBranchs(name)
		for _, branch := range branchs {
			branchsOpened = append(branchsOpened, state.Symbols[branch])
		}
	}

	for _, widx := range g.Outgoing[finishedIdx] {
		// downstream 的依赖数减 1
		state.Dependances[widx]--
		if state.Running[widx] != NODE_NOT_STARTED {
			continue
		}
		canRun := g.nextRound(state, widx, state.Dependances[widx] == 0, g.Nodes[finishedIdx].IsBranch(), branchsOpened)
		// fmt.Printf("nextRound: %s %t %d\n", g.Nodes[widx].Name(), canRun, state.Dependances[widx])
		if canRun {
			state.Readys = append(state.Readys, widx)
		} else if !canRun && state.Dependances[widx] == 0 {
			// skipped nodes
			state.States[widx] = TERMINATE_SKIPPED
			g.onNodeDone(widx, state)
		}
	}
}

/*
 * 当前节点结束后（done），评估受它影响的 downstream 节点的执行状态
 * 决定一个节点是否运行取决于 4 个因素
 * 1. 它自身的 TriggerCondition 设置
 * 2. 它的依赖数是否为 0
 * 3. 它是否在一个打开的 branch 中
 * 4. 它所有的 upstream 节点的状态
 */
func (g *DAG) nextRound(state *ExecutionState, idx int, free bool, isBranch bool, branchsOpened []int) bool {
	var canRun bool
	triggerCondition := g.Nodes[idx].GetTriggerCondition()
	upstreams := g.Incoming[idx]

	if triggerCondition == ALL_SUCCESS {
		selected := !isBranch
		if isBranch {
			for _, branch := range branchsOpened {
				if branch == idx {
					selected = true
					break
				}
			}
		}
		canRun = free && selected
		for _, upstream := range upstreams {
			canRun = canRun && state.States[upstream] == TERMINATE_SUCCESS
		}
	} else if triggerCondition == ALL_FAILED {
		canRun = free
		for _, upstream := range upstreams {
			canRun = canRun && state.States[upstream] == TERMINATE_FAILED
		}
	} else if triggerCondition == ALL_SKIPPED {
		canRun = free
		for _, upstream := range upstreams {
			canRun = canRun && state.States[upstream] == TERMINATE_SKIPPED
		}
	} else if triggerCondition == ALL_DONE {
		canRun = free
		for _, upstream := range upstreams {
			canRun = canRun && (state.States[upstream] == TERMINATE_SUCCESS || state.States[upstream] == TERMINATE_FAILED || state.States[upstream] == TERMINATE_SKIPPED)
		}
	} else if triggerCondition == ONE_FAILED {
		canRun = false
		for _, upstream := range upstreams {
			canRun = canRun || state.States[upstream] == TERMINATE_FAILED
		}
	} else if triggerCondition == ONE_SUCCESS {
		canRun = false
		for _, upstream := range upstreams {
			canRun = canRun || state.States[upstream] == TERMINATE_SUCCESS
		}
	} else if triggerCondition == ONE_DONE {
		canRun = false
		for _, upstream := range upstreams {
			canRun = canRun || (state.States[upstream] == TERMINATE_SUCCESS || state.States[upstream] == TERMINATE_FAILED)
		}
	} else if triggerCondition == NONE_FAILED {
		canRun = free
		for _, upstream := range upstreams {
			canRun = canRun && state.States[upstream] != TERMINATE_FAILED
		}
	} else if triggerCondition == NONE_FAILED_MIN_ONE_SUCCESS {
		noneFailed := free
		for _, upstream := range upstreams {
			noneFailed = noneFailed && state.States[upstream] != TERMINATE_FAILED
		}
		minSuccess := false
		for _, upstream := range upstreams {
			minSuccess = minSuccess || state.States[upstream] == TERMINATE_SUCCESS
		}
		canRun = noneFailed && minSuccess
	} else if triggerCondition == NONE_SKIPPED {
		canRun = free
		for _, upstream := range upstreams {
			canRun = canRun && state.States[upstream] != TERMINATE_SKIPPED
		}
	}

	return canRun
}
