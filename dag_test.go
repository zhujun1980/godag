package dag

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	yaml "gopkg.in/yaml.v2"
)

const (
	graphYAML = `
name: test-graph-1

nodes:
  my-vec-1:
    kind: NodeMock
    spec:
      is_branch:
      timeout: 10ms
      failover_on_timeout: true
      parallel_failover: true
      my-data: mock-data-1
      trigger_condition: All_failed
  my-vec-2:
    kind: NodeMock
    spec:
      is_branch: false
      name: test-vec-2
      timeout: 10s
      my-data: mock-data-2
  my-vec-3:
    kind: NodeMock
    spec:
      is_branch: false
      my-data: mock-data-3
      name:
      failover_on_timeout: false
      failover_on_error: false
      trigger_condition:
  my-vec-4:
    kind: NodeMock2
    spec:
      timeout: 10s
      failover_on_error: true
      my-data: mock-data-4

edges:
  my-vec-1:
    - my-vec-2
    - my-vec-3
  my-vec-2:
    - my-vec-3
`
)

var (
	_ = LogFatalIfError(RegisterNodeFactory("NodeMock", func(ctx context.Context) (Node, error) {
		return &NodeMock{}, nil
	}))
	_ = LogFatalIfError(RegisterNodeFactory("NodeMock2", func(ctx context.Context) (Node, error) {
		return &NodeMock2{}, nil
	}))
)

type myContextKey string

const requestDataKey myContextKey = "request"

type myRequestData struct {
	Data string
}

type NodeMock struct {
	BaseNode `yaml:"omitempty,inline"`
	Data     string `yaml:"my-data"`
}

type NodeMock2 struct {
	BaseNode `yaml:"omitempty,inline"`
	Data     string `yaml:"my-data"`
}

func (node *NodeMock) Init(ctx context.Context, name string) error {
	if err := node.BaseNode.Init(ctx, name); err != nil {
		return err
	}
	// node.Data += "|z"
	return nil
}

func (node *NodeMock) Execute(ctx context.Context, state *ExecutionState) *NodeResult {
	fmt.Printf("time %s, Main Mock %s started, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)

	if node.Name() == "my-vec-7" {
		fmt.Printf("time %s, Main Mock %s failed\n", time.Now().UTC().Format("15:04:05"), node.Data)
		return ErrorResult(fmt.Errorf("mock error 7"))
	}

	if node.Name() == "my-vec-8" {
		time.Sleep(time.Second * 5)
		fmt.Printf("time %s, Main Mock %s timeout\n", time.Now().UTC().Format("15:04:05"), node.Data)
		return Result(node.Data)
	}

	if node.Name() == "my-vec-9" {
		fmt.Printf("time %s, Main Mock %s branch\n", time.Now().UTC().Format("15:04:05"), node.Name())
		return Result("200")
	}

	if node.Name() == "my-vec-10" {
		fmt.Printf("time %s, Main Mock %s branch\n", time.Now().UTC().Format("15:04:05"), node.Name())
		return Result("500")
	}

	if node.Name() == "my-vec-11" {
		fmt.Printf("time %s, Main Mock %s branch\n", time.Now().UTC().Format("15:04:05"), node.Name())
		return Result(nil)
	}

	if node.Name() == "my-vec-12" {
		fmt.Printf("time %s, Main Mock %s branch\n", time.Now().UTC().Format("15:04:05"), node.Name())
		return Result("404")
	}

	if node.Name() == "my-vec-13" {
		fmt.Printf("time %s, Main Mock %s branch\n", time.Now().UTC().Format("15:04:05"), node.Name())
		return Result("not exist")
	}

	time.Sleep(time.Second * 1)

	if node.Data == "mock-data-3" {
		fmt.Printf("time %s, 1: %v, 2: %v\n", time.Now().UTC().Format("15:04:05"),
			state.Results[state.Symbols["my-vec-1"]],
			state.Results[state.Symbols["my-vec-2"]])
	}

	if node.Data == "mock-data-6" {
		fmt.Printf("time %s, Main Mock %s failed\n", time.Now().UTC().Format("15:04:05"), node.Data)
		return ErrorResult(fmt.Errorf("mock error"))
	}

	request := ctx.Value(requestDataKey).(*myRequestData)
	request.Data += " " + node.Data

	deadline, _ := ctx.Deadline()
	fmt.Printf("time %s, Main Mock %s success, deadline %s\n", time.Now().UTC().Format("15:04:05"), node.Data, deadline)
	return Result(node.Data)
}

func (node *NodeMock) Failover(ctx context.Context, state *ExecutionState) *NodeResult {
	fmt.Printf("time %s, Mock %s failover started, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)

	if node.Data == "mock-data-5-failover-slow" {
		time.Sleep(time.Second * 1)

		request := ctx.Value(requestDataKey).(*myRequestData)
		request.Data += " " + node.Data + "-failover"

		fmt.Printf("time %s, Mock %s failover slow finished, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
		return Result(node.Data + "-failover")
	}

	request := ctx.Value(requestDataKey).(*myRequestData)
	request.Data += " " + node.Data + "-failover"

	if node.Data == "mock-data-5" {
		fmt.Printf("time %s, Mock %s failover finished, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
		return Result(node.Data + "-failover")
	}
	if node.Data == "mock-data-5-failover-timeout" {
		time.Sleep(time.Second * 20)
		fmt.Printf("time %s, Mock %s failover timeout finished, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
		return Result(node.Data + "-failover")
	}
	if node.Data == "mock-data-5-failover-failed" {
		fmt.Printf("time %s, Mock %s failover failed, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
		return ErrorResult(fmt.Errorf("%s-failover", node.Data))
	}
	if node.Data == "mock-data-6" {
		fmt.Printf("time %s, Mock %s failover finished, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
		return Result(node.Data + "-failover-err")
	}

	fmt.Printf("time %s, Mock %s failover finished, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
	return Result(node.Data)
}

func (node *NodeMock2) Execute(ctx context.Context, state *ExecutionState) *NodeResult {
	fmt.Printf("time %s, Mock2 %s started, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)

	// time.Sleep(time.Second * 1)

	request := ctx.Value(requestDataKey).(*myRequestData)
	request.Data += " NodeMock2:" + node.Data

	deadline, _ := ctx.Deadline()
	fmt.Printf("time %s, Mock2 %s finished, deadline %s\n", time.Now().UTC().Format("15:04:05"), node.Data, deadline)
	return ErrorResult(errors.New("mock2 error"))
}

func (node *NodeMock2) Failover(ctx context.Context, state *ExecutionState) *NodeResult {
	fmt.Printf("time %s, Mock2 %s failed, timeout `%v`\n", time.Now().UTC().Format("15:04:05"), node.Data, node.TimeoutDuration)
	request := ctx.Value(requestDataKey).(*myRequestData)
	request.Data += " " + node.Data + "-failover"
	return Result(node.Data)
}

func TestDAGConf(t *testing.T) {
	graphConf := &DAGYAMLConfig{}

	err := yaml.Unmarshal([]byte(graphYAML), graphConf)
	if err != nil {
		t.Fatal(err.Error())
	}
	if graphConf.Name != "test-graph-1" {
		t.Fatalf("graphConf.Name is not correct, %s", graphConf.Name)
	}
	if graphConf.Edges["my-vec-1"][0] != "my-vec-2" {
		t.Fatalf("graphConf.Edges is not correct, %s", graphConf.Edges["my-vec-1"][0])
	}
	if graphConf.Edges["my-vec-1"][1] != "my-vec-3" {
		t.Fatalf("graphConf.Edges is not correct, %s", graphConf.Edges["my-vec-1"][1])
	}
	if graphConf.Edges["my-vec-2"][0] != "my-vec-3" {
		t.Fatalf("graphConf.Edges is not correct, %v", graphConf.Edges)
	}
	if len(graphConf.Edges["my-vec-3"]) != 0 {
		t.Fatalf("graphConf.Edges is not correct, %v", graphConf.Edges)
	}
	if len(graphConf.Edges["my-vec-4"]) != 0 {
		t.Fatalf("graphConf.Edges is not correct, %v", graphConf.Edges)
	}
	if len(graphConf.Edges) != 2 {
		t.Fatalf("graphConf.Edges len is not correct, %v", graphConf.Edges)
	}
	if len(graphConf.Nodes) != 4 {
		t.Fatalf("graphConf.Nodes len is not correct, %v", graphConf.Nodes)
	}
	if graphConf.Nodes["my-vec-1"].Kind != "NodeMock" {
		t.Fatalf("graphConf.Nodes kind is not correct, %v", graphConf.Nodes)
	}
	if graphConf.Nodes["my-vec-1"].Spec["my-data"] != "mock-data-1" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-1"].Spec["is_branch"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["my-data"] != "mock-data-2" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["is_branch"] != false {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["is_branch"] != false {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["my-data"] != "mock-data-3" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-3"])
	}
	if graphConf.Nodes["my-vec-4"].Spec["my-data"] != "mock-data-4" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-4"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["timeout"] != "10s" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["timeout"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-3"].Spec["timeout"])
	}
	if graphConf.Nodes["my-vec-1"].Spec["timeout"] != "10ms" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-4"].Spec["timeout"] != "10s" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-4"])
	}
	if graphConf.Nodes["my-vec-1"].Spec["failover_on_timeout"] != true {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-1"].Spec["failover_on_error"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["failover_on_timeout"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["failover_on_error"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["failover_on_timeout"] != false {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-3"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["failover_on_error"] != false {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-3"])
	}
	if graphConf.Nodes["my-vec-4"].Spec["failover_on_timeout"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-4"])
	}
	if graphConf.Nodes["my-vec-4"].Spec["failover_on_error"] != true {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-4"])
	}
	if graphConf.Nodes["my-vec-4"].Spec["is_branch"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-4"])
	}
	if graphConf.Nodes["my-vec-3"].Spec["trigger_condition"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-3"])
	}
	if graphConf.Nodes["my-vec-1"].Spec["trigger_condition"] != "All_failed" {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-1"])
	}
	if graphConf.Nodes["my-vec-2"].Spec["trigger_condition"] != nil {
		t.Fatalf("graphConf.Nodes spec is not correct, %v", graphConf.Nodes["my-vec-2"])
	}

	t.Error("SUCCESS")
}

func TestYAMLDecoder(t *testing.T) {
	loadstart := time.Now().UTC()
	g, err := LoadGraph(context.Background(), bytes.NewReader([]byte(graphYAML)))
	loadend := time.Now().UTC()
	var loadDuration time.Duration = loadend.Sub(loadstart)
	t.Logf("load: %dms", loadDuration.Milliseconds())
	if err != nil {
		t.Fatal(err.Error())
	}
	if g.Name != "test-graph-1" {
		t.Fatalf("name failed: %s", g.Name)
	}
	for idx, vt := range g.Nodes {
		v, ok := vt.(*NodeMock)
		if ok {
			if !strings.HasPrefix(v.Data, "mock-data-") {
				t.Fatalf("Data failed: %v %d", v, idx)
			}
			fmt.Printf("Timeout Parsed: %v %d %s\n", v, idx, v.Timeout)
			if v.Timeout != "" {
				_, err := time.ParseDuration(v.Timeout)
				if err != nil {
					t.Fatalf("Timeout failed: %v %d %s", v, idx, err)
				}
			}
		} else {
			v2, ok := vt.(*NodeMock2)
			if !ok {
				t.Fatalf("Type failed: %v", vt)
			}
			if !strings.HasPrefix(v2.Data, "mock-data-") {
				t.Fatalf("Data failed: %v %d", v2, idx)
			}
			fmt.Printf("Timeout Parsed: %v %d %s\n", v2, idx, v2.Timeout)
			if v2.Timeout != "" {
				_, err := time.ParseDuration(v2.Timeout)
				if err != nil {
					t.Fatalf("Timeout failed: %v %d %s", v2, idx, err)
				}
			}
		}
	}
	fmt.Printf("node: %v\n", g.Nodes[g.Symbols["my-vec-1"]].(*NodeMock).Data)
	fmt.Printf("node: %v\n", g.Nodes[g.Symbols["my-vec-2"]].(*NodeMock).Data)
	fmt.Printf("node: %v\n", g.Nodes[g.Symbols["my-vec-3"]].(*NodeMock).Data)
	fmt.Printf("node: %v\n", g.Nodes[g.Symbols["my-vec-4"]].(*NodeMock2).Data)

	if g.Nodes[g.Symbols["my-vec-1"]].(*NodeMock).Name() != "my-vec-1" {
		t.Fatalf("Name failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].(*NodeMock).Name() != "test-vec-2" {
		t.Fatalf("Name failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].(*NodeMock).Name() != "my-vec-3" {
		t.Fatalf("Name failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].(*NodeMock2).Name() != "my-vec-4" {
		t.Fatalf("Name failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]], 0)
	}

	if g.Nodes[g.Symbols["my-vec-1"]].(*NodeMock).Timeout != "10ms" {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[0], 0)
	}
	if g.Nodes[g.Symbols["my-vec-1"]].(*NodeMock).TimeoutDuration != time.Duration(10*time.Millisecond) {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]].(*NodeMock), 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].(*NodeMock).Timeout != "10s" {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[1], 1)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].(*NodeMock).TimeoutDuration != time.Duration(10*time.Second) {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]].(*NodeMock), 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].(*NodeMock).Timeout != "" {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 2)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].(*NodeMock).TimeoutDuration != time.Duration(0) {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]].(*NodeMock), 0)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].(*NodeMock2).Timeout != "10s" {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]], 3)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].(*NodeMock2).TimeoutDuration != time.Duration(10*time.Second) {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]].(*NodeMock), 0)
	}
	if g.Nodes[g.Symbols["my-vec-1"]].FailoverOnError() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-1"]].FailoverOnTimeout() != true {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-1"]].GetTriggerCondition() != ALL_FAILED {
		t.Fatalf("Condition failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].GetTriggerCondition() != ALL_SUCCESS {
		t.Fatalf("Condition failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].FailoverOnError() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].FailoverOnTimeout() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].GetTriggerCondition() != ALL_SUCCESS {
		t.Fatalf("Condition failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].FailoverOnError() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].FailoverOnTimeout() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].FailoverOnError() != true {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].FailoverOnTimeout() != false {
		t.Fatalf("Timeout failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-1"]].IsBranch() != false {
		t.Fatalf("IsBranch failed: %v %d", g.Nodes[g.Symbols["my-vec-1"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-2"]].IsBranch() != false {
		t.Fatalf("IsBranch failed: %v %d", g.Nodes[g.Symbols["my-vec-2"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-3"]].IsBranch() != false {
		t.Fatalf("IsBranch failed: %v %d", g.Nodes[g.Symbols["my-vec-3"]], 0)
	}
	if g.Nodes[g.Symbols["my-vec-4"]].IsBranch() != false {
		t.Fatalf("IsBranch failed: %v %d", g.Nodes[g.Symbols["my-vec-4"]], 0)
	}

	// is leaf?
	if g.Outgoing[g.Symbols["my-vec-4"]] != nil {
		t.Fatalf("Edges failed: %v %d", g.Outgoing[g.Symbols["my-vec-4"]], 0)
	}
	if g.Outgoing[g.Symbols["my-vec-3"]] != nil {
		t.Fatalf("Edges failed: %v %d", g.Outgoing[g.Symbols["my-vec-3"]], 0)
	}
	if g.Outgoing[g.Symbols["my-vec-2"]] == nil {
		t.Fatalf("Edges failed: %v %d", g.Outgoing[g.Symbols["my-vec-2"]], 0)
	}
	if g.Outgoing[g.Symbols["my-vec-1"]] == nil {
		t.Fatalf("Edges failed: %v %d", g.Outgoing[g.Symbols["my-vec-1"]], 0)
	}

	if len(g.Incoming[g.Symbols["my-vec-1"]]) != 0 {
		t.Fatalf("Edges failed: %v %d", g.Incoming[g.Symbols["my-vec-1"]], 0)
	}
	if len(g.Incoming[g.Symbols["my-vec-2"]]) != 1 {
		t.Fatalf("Edges failed: %v %d", g.Incoming[g.Symbols["my-vec-2"]], 0)
	}
	if len(g.Incoming[g.Symbols["my-vec-3"]]) != 2 {
		t.Fatalf("Edges failed: %v %d", g.Incoming[g.Symbols["my-vec-3"]], 0)
	}
	if len(g.Incoming[g.Symbols["my-vec-4"]]) != 0 {
		t.Fatalf("Edges failed: %v %d", g.Incoming[g.Symbols["my-vec-4"]], 0)
	}

	for vn, idx := range g.Symbols {
		if vn == "my-vec-4" {
			vt := g.Nodes[idx].(*NodeMock2)
			if vt.Data[len(vt.Data)-1] != vn[len(vn)-1] {
				t.Fatalf("Symbol failed: %d %s %s", idx, vt.Data, vn)
			}
		} else {
			vt := g.Nodes[idx].(*NodeMock)
			// fmt.Printf("Symbol: %s %d %s\n", vn, idx, vt.Data)
			if vt.Data[len(vt.Data)-1] != vn[len(vn)-1] {
				t.Fatalf("Symbol failed: %d %s %s", idx, vt.Data, vn)
			}
		}
	}
	for idx, widxs := range g.Outgoing {
		_, ok := g.Nodes[idx].(*NodeMock)
		if ok {
			for _, widx := range widxs {
				_, ok := g.Nodes[widx].(*NodeMock)
				if !ok {
					t.Fatalf("edges target failed: %d %d", idx, widx)
				}
			}
		} else {
			_, ok := g.Nodes[idx].(*NodeMock2)
			if !ok {
				t.Fatalf("edges source failed: %d", idx)
			}
			for _, widx := range widxs {
				_, ok := g.Nodes[widx].(*NodeMock)
				if !ok {
					t.Fatalf("edges target failed: %d %d", idx, widx)
				}
			}
		}
	}
	fmt.Printf("Edges: %v\n", g.Outgoing)
	if g.Nodes[g.Outgoing[g.Symbols["my-vec-1"]][0]].(*NodeMock).Data != "mock-data-2" {
		t.Fatalf("edges failed: %d", g.Symbols["my-vec-1"])
	}
	if g.Nodes[g.Outgoing[g.Symbols["my-vec-1"]][1]].(*NodeMock).Data != "mock-data-3" {
		t.Fatalf("edges failed: %d", g.Symbols["my-vec-1"])
	}
	if g.Nodes[g.Outgoing[g.Symbols["my-vec-2"]][0]].(*NodeMock).Data != "mock-data-3" {
		t.Fatalf("edges failed: %d", g.Symbols["my-vec-2"])
	}
	if len(g.Outgoing) != 4 {
		t.Fatalf("edges 2 failed: %v", g.Outgoing)
	}
	if len(g.Outgoing[g.Symbols["my-vec-3"]]) != 0 {
		t.Fatalf("edges 3 failed: %v", g.Outgoing)
	}
	if len(g.Outgoing[g.Symbols["my-vec-4"]]) != 0 {
		t.Fatalf("edges 4 failed: %v", g.Outgoing)
	}

	t.Logf("graph: %v", g)

	t.Error("SUCCESS")
}

func TestNodeResult(t *testing.T) {
	r := Result("aaa")
	if r.Result.(string) != "aaa" {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if r.Err != nil {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if r.IsDefaultFailover() {
		t.Fatalf("NodeResult failed: %v", r)
	}

	r = ErrorResult(fmt.Errorf("aaa"))
	if r.Err.Error() != "aaa" {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if r.Result != nil {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if r.IsDefaultFailover() {
		t.Fatalf("NodeResult failed: %v", r)
	}

	node := BaseNode{}
	r = node.Failover(context.Background(), nil)
	if r.Err != nil {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if r.Result == nil {
		t.Fatalf("NodeResult failed: %v", r)
	}
	if !r.IsDefaultFailover() {
		t.Fatalf("NodeResult failed: %v", r)
	}

	t.Error("SUCCESS")
}

func TestDAGExecution(t *testing.T) {
	graph, _ := LoadGraph(context.Background(), bytes.NewReader([]byte(graphYAML)))

	req := &myRequestData{Data: "Go"}
	ctx := context.WithValue(context.Background(), requestDataKey, req)

	state, err := graph.Execute(ctx)
	// time.Sleep(10 * time.Second)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}
	for idx, r := range state.States {
		fmt.Printf("NodeState %d: %s, %d\n", idx, graph.Nodes[idx].Name(), r)
	}

	for idx, r := range state.Results {
		fmt.Printf("NodeResult %d: %v %v\n", idx, r, r.ResultOnErr)
	}

	if state.Results[state.Symbols["my-vec-1"]].Err != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-1"]])
	}
	if state.Results[state.Symbols["my-vec-1"]].Result != "mock-data-1" {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-1"]])
	}
	if state.Results[state.Symbols["my-vec-1"]].IsFailover != true {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-1"]])
	}
	if !os.IsTimeout(state.Results[state.Symbols["my-vec-1"]].ResultOnErr.Err) {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-1"]])
	}

	if state.Results[state.Symbols["my-vec-2"]].Err != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-2"]])
	}
	if state.Results[state.Symbols["my-vec-2"]].Result != "mock-data-2" {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-2"]])
	}
	if state.Results[state.Symbols["my-vec-2"]].IsFailover != false {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-2"]])
	}
	if state.Results[state.Symbols["my-vec-2"]].ResultOnErr != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-2"]])
	}

	if state.Results[state.Symbols["my-vec-3"]].Err != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-3"]])
	}
	if state.Results[state.Symbols["my-vec-3"]].Result != "mock-data-3" {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-3"]])
	}
	if state.Results[state.Symbols["my-vec-3"]].IsFailover != false {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-3"]])
	}
	if state.Results[state.Symbols["my-vec-3"]].ResultOnErr != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-3"]])
	}

	if state.Results[state.Symbols["my-vec-4"]].Err != nil {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-4"]])
	}
	if state.Results[state.Symbols["my-vec-4"]].Result != "mock-data-4" {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-4"]])
	}
	if state.Results[state.Symbols["my-vec-4"]].IsFailover != true {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-4"]])
	}
	if state.Results[state.Symbols["my-vec-4"]].ResultOnErr.Err.Error() != "mock2 error" {
		t.Fatalf("NodeResult failed: %v", state.Results[state.Symbols["my-vec-4"]])
	}

	t.Logf("execute: %dms", state.Dura.Milliseconds())

	t.Logf("%v", state)

	t.Errorf("SUCCESS: %s", req.Data)
}

func runGraph(graphYaml string) (*ExecutionState, *myRequestData, error) {
	graph, err := LoadGraph(context.Background(), bytes.NewReader([]byte(graphYaml)))
	if err != nil {
		return nil, nil, err
	}
	req := &myRequestData{Data: "Go"}
	ctx := context.WithValue(context.Background(), requestDataKey, req)
	state, err := graph.Execute(ctx)
	return state, req, err
}

func TestDAGExecution2(t *testing.T) {
	cnt := 1
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      my-data: mock-data-5
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-5"]].Result != "mock-data-5" {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	// parallel_failover 配置必须和 failover_on_timeout 或 failover_on_error 配置一起使用
	cnt = 2
	_, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      parallel_failover: true
      my-data: mock-data-5
`)))
	if err == nil {
		t.Fatalf("%d: %s\n", cnt, "parallel_failover 配置必须和 failover_on_timeout 或 failover_on_error 配置一起使用")
	}

	// trigger_condition 错误
	_, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      parallel_failover: true
      my-data: mock-data-5
      trigger_condition: All_failed1
`)))
	if !strings.Contains(err.Error(), "trigger condition `All_failed1` not exists") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	_, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: aaaaa
      my-data: mock-data-5
`)))
	if !strings.Contains(err.Error(), "parse timeout `aaaaa` failed") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	ggggg, err := LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout:
      my-data: mock-data-5
`)))
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if ggggg.Timeout != MaxNodeTimeoutDuration {
		t.Fatalf("%d: %s\n", cnt, "expected timeout to be MaxNodeTimeoutDuration")
	}
	if ggggg.Nodes[0].(*NodeMock).Timeout != "" {
		t.Fatalf("%d: %s\n", cnt, "expected timeout to be empty")
	}

	ggggg, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2
timeout:

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout:
      my-data: mock-data-5
`)))
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if ggggg.Timeout != MaxNodeTimeoutDuration {
		t.Fatalf("%d: %s\n", cnt, "expected timeout to be MaxNodeTimeoutDuration")
	}

	ggggg, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2
timeout: 100ms

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout:
      my-data: mock-data-5
`)))
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if ggggg.Timeout != time.Duration(100*time.Millisecond) {
		t.Fatalf("%d: %s\n", cnt, "expected timeout to be 100ms")
	}
	if ggggg.Nodes[0].(*NodeMock).Timeout != "" {
		t.Fatalf("%d: %s\n", cnt, "expected timeout to be empty")
	}
	if ggggg.Nodes[0].(*NodeMock).TimeoutDuration != time.Duration(0) {
		t.Fatalf("%d: %s\n", cnt, "expected timeout duration to be 0")
	}

	ggggg, err = LoadGraph(context.Background(), bytes.NewReader([]byte(`
name: test-graph-2
timeout: aaaaaa

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout:
      my-data: mock-data-5
`)))
	if strings.Contains(err.Error(), "parse timeout `aaaaaaa` failed") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if ggggg != nil {
		t.Fatalf("%d: %s\n", cnt, "expected ggggg to be nil")
	}

	// 超时并不 failover 测试
	cnt = 3
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err = runGraph(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10ms
      my-data: mock-data-5
`)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if !os.IsTimeout(state.Results[state.Symbols["my-vec-5"]].Err) {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
	}
	if state.GraphState != TERMINATE_FAILED {
		t.Fatalf("expected graph state to be TERMINATE_FAILED, got %d\n", state.GraphState)
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	// failover_on_timeout 测试
	cnt = 4
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err = runGraph(`
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10ms
      failover_on_timeout: true
      my-data: mock-data-5
`)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-5"]].Result != "mock-data-5-failover" {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	// failover_on_error 测试
	cnt = 5
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err = runGraph(`
name: test-graph-2

nodes:
  my-vec-6:
    kind: NodeMock
    spec:
      timeout: 10s
      failover_on_error: true
      my-data: mock-data-6
`)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-6"]].Result != "mock-data-6-failover-err" {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-6"]])
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("expected graph state to be TERMINATE_SUCCESS, got %d\n", state.GraphState)
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	// 出错并不 failover 测试
	cnt = 6
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err = runGraph(`
name: test-graph-2

nodes:
  my-vec-6:
    kind: NodeMock
    spec:
      timeout: 10s
      my-data: mock-data-6
`)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-6"]].Err.Error() != "mock error" {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-6"]])
	}
	if state.GraphState != TERMINATE_FAILED {
		t.Fatalf("expected graph state to be TERMINATE_FAILED, got %d\n", state.GraphState)
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	// 并行 failover 测试，正常情况
	cnt = 7
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := ""
	tmpl = `
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: true
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err = runGraph(fmt.Sprintf(tmpl, data))
		if err != nil {
			t.Fatalf("%d: %s\n", cnt, err.Error())
		}
		if state.Results[state.Symbols["my-vec-5"]].Result != data {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      parallel_failover: true
      failover_on_timeout: false
      failover_on_error: true
      my-data: %s
`
	for _, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed"} {
		state, req, err = runGraph(fmt.Sprintf(tmpl, data))
		if err != nil {
			t.Fatalf("%d: %s\n", cnt, err.Error())
		}
		if state.Results[state.Symbols["my-vec-5"]].Result != data {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 10s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: false
      my-data: %s
`
	for _, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed"} {
		state, req, err = runGraph(fmt.Sprintf(tmpl, data))
		if err != nil {
			t.Fatalf("%d: %s\n", cnt, err.Error())
		}
		if state.Results[state.Symbols["my-vec-5"]].Result != data {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-5:
    kind: core/basenode
    spec:

  my-vec-4:
    kind: NodeMock
    spec:
      timeout: 10s
      my-data: mock-data-4

edges:
  my-vec-5:
    - my-vec-4
`
	state, req, err = runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if !state.Results[state.Symbols["my-vec-5"]].IsDefaultResult() {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-5"]])
	}
	if state.Results[state.Symbols["my-vec-4"]].Result != "mock-data-4" {
		t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-4"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGExecution3(t *testing.T) {
	// 并行 failover 测试，主流程报错
	cnt := 8
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-7:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: true
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))
		if data == "mock-data-5" || data == "mock-data-5-failover-slow" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_SUCCESS {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			t.Logf("%d: %v - %s", cnt, state, req.Data)
		} else if data == "mock-data-5-failover-timeout" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-7"]].Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Err.Error() != "mock error 7" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
		} else if data == "mock-data-5-failover-failed" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].Err.Error() != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Err.Error() != "mock error 7" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution4(t *testing.T) {
	// 并行 failover 测试，主流程报错, failover_on_timeout=false
	cnt := 8
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-7:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: false
      failover_on_error: true
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))
		if data == "mock-data-5" || data == "mock-data-5-failover-slow" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_SUCCESS {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			t.Logf("%d: %v - %s", cnt, state, req.Data)
		} else if data == "mock-data-5-failover-timeout" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].Err.Error() != "mock error 7" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
		} else if data == "mock-data-5-failover-failed" {
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-7"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].Err.Error() != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if !state.Results[state.Symbols["my-vec-7"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
			if state.Results[state.Symbols["my-vec-7"]].ResultOnErr.Err.Error() != "mock error 7" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
			}
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution5(t *testing.T) {
	// 并行 failover 测试，主流程报错, failover_on_error=false
	cnt := 8
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-7:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: false
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))
		if err != nil {
			t.Fatalf("%d: %s %s\n", cnt, data, err.Error())
		}
		if state.GraphState != TERMINATE_FAILED {
			t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
		}
		if state.Results[state.Symbols["my-vec-7"]].Result != nil {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
		}
		if state.Results[state.Symbols["my-vec-7"]].Err.Error() != "mock error 7" {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
		}
		if state.Results[state.Symbols["my-vec-7"]].IsFailover {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
		}
		if state.Results[state.Symbols["my-vec-7"]].ResultOnErr != nil {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-7"]])
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution6(t *testing.T) {
	// 并行 failover 测试，主流程超时
	cnt := 9
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-8:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: true
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))
		if data == "mock-data-5" || data == "mock-data-5-failover-slow" {
			// failover 成功
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_SUCCESS {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].Err != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		} else if data == "mock-data-5-failover-timeout" {
			// failover 也 timeout
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		} else if data == "mock-data-5-failover-failed" {
			// failover 报错
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].Err.Error() != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution7(t *testing.T) {
	// 并行 failover 测试，主流程超时, failover_on_timeout=false
	cnt := 9
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-8:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: false
      failover_on_error: true
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))

		if err != nil {
			t.Fatalf("%d: %s\n", cnt, err.Error())
		}
		if state.GraphState != TERMINATE_FAILED {
			t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
		}
		if state.Results[state.Symbols["my-vec-8"]].Result != nil {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
		}
		if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].Err) {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
		}
		if state.Results[state.Symbols["my-vec-8"]].IsFailover {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
		}
		if state.Results[state.Symbols["my-vec-8"]].ResultOnErr != nil {
			t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
		}

		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution8(t *testing.T) {
	// 并行 failover 测试，主流程超时, failover_on_error=false
	cnt := 9
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-8:
    kind: NodeMock
    spec:
      timeout: 2s
      parallel_failover: true
      failover_on_timeout: true
      failover_on_error: false
      my-data: %s
`
	for ii, data := range []string{"mock-data-5", "mock-data-5-failover-timeout", "mock-data-5-failover-failed", "mock-data-5-failover-slow"} {
		fmt.Printf("=============== %d - %d: start ===============\n", cnt, ii)
		state, req, err := runGraph(fmt.Sprintf(tmpl, data))
		if data == "mock-data-5" || data == "mock-data-5-failover-slow" {
			// failover 成功
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_SUCCESS {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].Err != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		} else if data == "mock-data-5-failover-timeout" {
			// failover timeout
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		} else if data == "mock-data-5-failover-failed" {
			// failover failed
			if err != nil {
				t.Fatalf("%d: %s\n", cnt, err.Error())
			}
			if state.GraphState != TERMINATE_FAILED {
				t.Fatalf("%d: GraphState failed: %v", cnt, state.GraphState)
			}
			if state.Results[state.Symbols["my-vec-8"]].Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].Err.Error() != data+"-failover" {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !state.Results[state.Symbols["my-vec-8"]].IsFailover {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Result != nil {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
			if !os.IsTimeout(state.Results[state.Symbols["my-vec-8"]].ResultOnErr.Err) {
				t.Fatalf("%d: NodeResult failed: %v", cnt, state.Results[state.Symbols["my-vec-8"]])
			}
		}
		t.Logf("%d: %v - %s", cnt, state, req.Data)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution9(t *testing.T) {
	// Graph Timeout
	cnt := 10
	fmt.Printf("=============== %d: start ===============\n", cnt)

	tmpl := `
name: test-graph-2
timeout: 100ms

nodes:
  my-vec-5:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-5
`
	state, req, err := runGraph(tmpl)
	if !os.IsTimeout(err) {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")

	// time.Sleep(10 * time.Second)
}

func TestDAGExecution10(t *testing.T) {
	// Graph Branch
	cnt := 10
	fmt.Printf("=============== %d - 1: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      my-data: mock-data-9
`
	_, _, err := runGraph(tmpl)
	if !strings.Contains(err.Error(), "branchs must not be empty") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
      my-data: mock-data-9
`
	_, _, err = runGraph(tmpl)
	if !strings.Contains(err.Error(), "branchs must not be empty") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: false
      branchs:
        200:
        500:
        404:
      my-data: mock-data-9
`
	_, _, err = runGraph(tmpl)
	if !strings.Contains(err.Error(), "branchs must not be empty") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: false
      branchs:
      my-data: mock-data-9
`
	state, req, err := runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-9"]].Result != "200" {
		t.Fatalf("%d: %v\n", cnt, state.Results[state.Symbols["my-vec-9"]])
	}

	t.Logf("%d: %v - %s", cnt, state, req.Data)

	tmpl = `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
        500:
          - end
        404:
      my-data: mock-data-9
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-9:
    - end
`
	graph, err := LoadGraph(context.Background(), bytes.NewReader([]byte(tmpl)))
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	for _, name := range []string{"200", "500", "404"} {
		found := false
		for _, nn := range graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchNames() {
			if name == nn {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%d: branch `%s` not exists\n", cnt, name)
		}
	}

	for _, name := range []string{"200", "404"} {
		if graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs(name) != nil {
			t.Fatalf("%d: branch `%s` not exists\n", cnt, name)
		}
	}
	if len(graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("500")) != 1 {
		t.Fatalf("%d: branch `%s` not exists\n", cnt, "500")
	}
	if graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("500")[0] != "end" {
		t.Fatalf("%d: branch `%s` not exists\n", cnt, "500")
	}

	t.Logf("%d: 200 %v", cnt, graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("200"))
	t.Logf("%d: 500 %v", cnt, graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("500"))
	t.Logf("%d: 404 %v", cnt, graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("404"))

	t.Logf("%d: 111111 %v", cnt, graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("111111"))
	t.Logf("%d: empty %v", cnt, graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs(""))

	for _, bb := range graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("200") {
		t.Logf("%d: %v", cnt, bb)
	}
	for _, bb := range graph.Nodes[graph.Symbols["my-vec-9"]].GetBranchs("") {
		t.Logf("%d: %v", cnt, bb)
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution11(t *testing.T) {
	// Graph Branch
	cnt := 11
	fmt.Printf("=============== %d - 1: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
        500:
          - end
        404:
      my-data: mock-data-9
`
	_, err := LoadGraph(context.Background(), bytes.NewReader([]byte(tmpl)))
	if !strings.Contains(err.Error(), "branch end in node my-vec-9 not found") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	tmpl = `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
        500:
          - end
        404:
      my-data: mock-data-9
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-9:
`
	_, err = LoadGraph(context.Background(), bytes.NewReader([]byte(tmpl)))
	if !strings.Contains(err.Error(), "branch end isn't downstream of node my-vec-9") {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	t.Errorf("SUCCESS")
}

func TestDAGExecution12(t *testing.T) {
	// Graph Branch
	cnt := 12
	fmt.Printf("=============== %d - 1: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-9:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
          - my-vec-9-1
          - my-vec-9-2
        500:
          - end
        404:
      my-data: mock-data-9
  my-vec-9-1:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-1
  my-vec-9-2:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-2
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-9:
    - my-vec-9-1
    - my-vec-9-2
    - end
`
	state, req, err := runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["my-vec-9-1"]].Result != "mock-data-9-1" {
		t.Fatalf("%d: %v\n", cnt, state.Results[state.Symbols["my-vec-9-1"]])
	}
	if state.Results[state.Symbols["my-vec-9-2"]].Result != "mock-data-9-2" {
		t.Fatalf("%d: %v\n", cnt, state.Results[state.Symbols["my-vec-9-2"]])
	}
	if state.States[state.Symbols["end"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["end"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	fmt.Printf("=============== %d - 2: start ===============\n", cnt)
	tmpl = `
name: test-graph-2

nodes:
  my-vec-10:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
          - my-vec-9-1
          - my-vec-9-2
        500:
          - end
        404:
      my-data: mock-data-10
  my-vec-9-1:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-1
  my-vec-9-2:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-2
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-10:
    - my-vec-9-1
    - my-vec-9-2
    - end
`
	state, req, err = runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.Results[state.Symbols["end"]].Result != "mock-data-end" {
		t.Fatalf("%d: %v\n", cnt, state.Results[state.Symbols["end"]])
	}
	if state.States[state.Symbols["my-vec-9-1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-1"]])
	}
	if state.States[state.Symbols["my-vec-9-2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-2"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	fmt.Printf("=============== %d - 3: start ===============\n", cnt)
	tmpl = `
name: test-graph-2

nodes:
  my-vec-11:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
          - my-vec-9-1
          - my-vec-9-2
        500:
          - end
        404:
      my-data: mock-data-11
  my-vec-9-1:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-1
  my-vec-9-2:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-2
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-11:
    - my-vec-9-1
    - my-vec-9-2
    - end
`
	state, req, err = runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.States[state.Symbols["end"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["end"]])
	}
	if state.States[state.Symbols["my-vec-9-1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-1"]])
	}
	if state.States[state.Symbols["my-vec-9-2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-2"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGExecution13(t *testing.T) {
	// Graph Branch
	cnt := 13
	fmt.Printf("=============== %d - 1: start ===============\n", cnt)

	tmpl := `
name: test-graph-2

nodes:
  my-vec-12:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
          - my-vec-9-1
          - my-vec-9-2
        500:
          - end
        404:
      my-data: mock-data-9
  my-vec-9-1:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-1
  my-vec-9-2:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-2
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-12:
    - my-vec-9-1
    - my-vec-9-2
    - end
`
	state, req, err := runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}

	if state.States[state.Symbols["end"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["end"]])
	}
	if state.States[state.Symbols["my-vec-9-1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-1"]])
	}
	if state.States[state.Symbols["my-vec-9-2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-2"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	tmpl = `
name: test-graph-2

nodes:
  my-vec-13:
    kind: NodeMock
    spec:
      timeout: 2s
      is_branch: true
      branchs:
        200:
          - my-vec-9-1
          - my-vec-9-2
        500:
          - end
        404:
      my-data: mock-data-9
  my-vec-9-1:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-1
  my-vec-9-2:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-9-2
  end:
    kind: NodeMock
    spec:
      timeout: 2s
      my-data: mock-data-end

edges:
  my-vec-13:
    - my-vec-9-1
    - my-vec-9-2
    - end
`
	state, req, err = runGraph(tmpl)
	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.States[state.Symbols["end"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["end"]])
	}
	if state.States[state.Symbols["my-vec-9-1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-1"]])
	}
	if state.States[state.Symbols["my-vec-9-2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %v\n", cnt, state.States[state.Symbols["my-vec-9-2"]])
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}
