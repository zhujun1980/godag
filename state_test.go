package dag

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	_ = LogFatalIfError(RegisterNodeFactory("TestNode", func(ctx context.Context) (Node, error) {
		return &TestNode{}, nil
	}))
)

type TestNode struct {
	BaseNode `yaml:"omitempty,inline"`
	Data     string `yaml:"data"`
}

func (node *TestNode) Execute(ctx context.Context, state *ExecutionState) *NodeResult {
	fmt.Printf("time %s, %s start\n", time.Now().UTC().Format("15:04:05"), node.Name())
	defer func() {
		fmt.Printf("time %s, %s finished\n", time.Now().UTC().Format("15:04:05"), node.Name())
	}()

	if node.Name() == "test-failed" {
		return ErrorResult(errors.New("test-failed"))
	}
	if node.Name() == "test-timeout" {
		time.Sleep(time.Second * 5)
		return ErrorResult(errors.New("test-timeout"))
	}
	if node.Name() == "test-slow-task" {
		time.Sleep(time.Second * 1)
		return Result("200")
	}
	if node.Name() == "test-branch-500" {
		return Result("500")
	}
	return Result("200")
}

func (node *TestNode) Failover(ctx context.Context, state *ExecutionState) *NodeResult {
	return Result("200")
}

func (node *TestNode) OnFinished(ctx context.Context, state *ExecutionState, result *NodeResult) {
}

func TestDAGState(t *testing.T) {
	cnt := 1
	fmt.Printf("=============== %d: start ===============\n", cnt)

	state, req, err := runGraph(`
name: test-graph
timeout: 10ms

nodes:
  test-timeout:
    kind: TestNode
    spec:
      data: testdata1
`)
	if !os.IsTimeout(err) {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	t.Logf("%d: %v - %s - %s", cnt, state, req.Data, err.Error())

	state, req, err = runGraph(`
name: test-graph

nodes:
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test2:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
      trigger_condition: all_failed
  test5:
    kind: TestNode
    spec:
      data: testdata5
  test6:
    kind: TestNode
    spec:
      data: testdata6
      trigger_condition: all_done
  test7:
    kind: TestNode
    spec:
      data: testdata7
      trigger_condition: all_skipped

edges:
  test1:
    - test3
    - test4
  test2:
    - test3
    - test4
  test4:
    - test5
    - test6
  test5:
    - test7
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test3"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test3\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test5"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test5\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test6"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test6\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test7"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test7\"]].State != TERMINATE_SUCCESS")
	}
	if state.Results[state.Symbols["test3"]].Result != "200" {
		t.Fatalf("%d: %s\n", cnt, "state.Results[state.Symbols[\"test3\"]].Result != \"200\"")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGState2(t *testing.T) {
	cnt := 2
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph

nodes:
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test-failed:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
      trigger_condition: one_failed
  test4:
    kind: TestNode
    spec:
      data: testdata4
      trigger_condition: one_success
  test5:
    kind: TestNode
    spec:
      data: testdata5
  test6:
    kind: TestNode
    spec:
      data: testdata6
      trigger_condition: one_done

edges:
  test1:
    - test3
    - test4
    - test5
    - test6
  test-failed:
    - test3
    - test4
    - test5
    - test6
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test3"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test3\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test5"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test5\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test6"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test6\"]].State != TERMINATE_SUCCESS")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGState3(t *testing.T) {
	// NONE_FAILED
	cnt := 3
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph

nodes:
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test-failed:
    kind: TestNode
    spec:
      data: testdata2
  test2:
    kind: TestNode
    spec:
      data: testdata2
      trigger_condition: none_failed
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
      trigger_condition: none_failed

edges:
  test1:
    - test2
  test-failed:
    - test2
  test2:
    - test4
  test3:
    - test4
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test2\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SUCCESS")
	}
	if state.Results[state.Symbols["test4"]].Result != "200" {
		t.Fatalf("%d: %s\n", cnt, "state.Results[state.Symbols[\"test4\"]].Result != \"200\"")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGState4(t *testing.T) {
	// NONE_FAILED_MIN_ONE_SUCCESS
	cnt := 4
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph

nodes:
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test-failed:
    kind: TestNode
    spec:
      data: testdata2
  test2:
    kind: TestNode
    spec:
      data: testdata2
      trigger_condition: none_failed_min_one_success
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
      trigger_condition: none_failed_min_one_success
  test5:
    kind: TestNode
    spec:
      data: testdata5
      trigger_condition: none_failed_min_one_success
  test6:
    kind: TestNode
    spec:
      data: testdata5
      trigger_condition: none_skipped
  test7:
    kind: TestNode
    spec:
      data: testdata5
      trigger_condition: none_skipped

edges:
  test1:
    - test2
  test-failed:
    - test2
    - test7
  test2:
    - test4
  test3:
    - test4
  test2:
    - test5
  test4:
    - test6
  test5:
    - test6
  test4:
    - test7
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test2\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SUCCESS")
	}
	if state.Results[state.Symbols["test4"]].Result != "200" {
		t.Fatalf("%d: %s\n", cnt, "state.Results[state.Symbols[\"test4\"]].Result != \"200\"")
	}
	if state.States[state.Symbols["test5"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test5\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test6"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test6\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test7"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test7\"]].State != TERMINATE_SKIPPED")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	t.Errorf("SUCCESS")
}

func TestDAGState5(t *testing.T) {
	// Branch
	cnt := 5
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph

nodes:
  test-branch-200:
    kind: TestNode
    spec:
      data: testdata1
      is_branch: true
      branchs:
        200:
          - test1
        500:
          - test2
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test2:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
  test5:
    kind: TestNode
    spec:
      data: testdata5
  test6:
    kind: TestNode
    spec:
      data: testdata5
  test7:
    kind: TestNode
    spec:
      data: testdata5

edges:
  test-branch-200:
    - test1
    - test2
  test1:
    - test7
  test2:
    - test3
  test3:
    - test4
  test4:
    - test7
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test1"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test1\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test2"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test2\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test3"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test3\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test7"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test7\"]].State != TERMINATE_SKIPPED")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)

	state, req, err = runGraph(`
name: test-graph

nodes:
  test-branch-500:
    kind: TestNode
    spec:
      data: testdata1
      is_branch: true
      branchs:
        200:
          - test1
        500:
          - test2
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test2:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
  test5:
    kind: TestNode
    spec:
      data: testdata5
  test6:
    kind: TestNode
    spec:
      data: testdata5
  test7:
    kind: TestNode
    spec:
      data: testdata5

edges:
  test-branch-500:
    - test1
    - test2
  test1:
    - test7
  test2:
    - test3
  test3:
    - test4
  test4:
    - test7
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test1\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test2"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test2\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test3"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test3\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test7"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test7\"]].State != TERMINATE_SKIPPED")
	}

	t.Logf("%d: %v - %s", cnt, state, req.Data)

	state, req, err = runGraph(`
name: test-graph

nodes:
  test-branch-200:
    kind: TestNode
    spec:
      data: testdata1
      is_branch: true
      branchs:
        200:
          - test2
        500:
          - test1
  test1:
    kind: TestNode
    spec:
      data: testdata1
  test2:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
  test4:
    kind: TestNode
    spec:
      data: testdata4
  test5:
    kind: TestNode
    spec:
      data: testdata5
  test6:
    kind: TestNode
    spec:
      data: testdata5
  test7:
    kind: TestNode
    spec:
      data: testdata5
      # trigger_condition: all_done

edges:
  test-branch-200:
    - test1
    - test2
  test1:
    - test7
  test2:
    - test3
  test3:
    - test4
  test4:
    - test7
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test1"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test1\"]].State != TERMINATE_SKIPPED")
	}
	if state.States[state.Symbols["test2"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test2\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test3"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test3\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test4"]] != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test4\"]].State != TERMINATE_SUCCESS")
	}
	if state.States[state.Symbols["test7"]] != TERMINATE_SKIPPED {
		t.Fatalf("%d: %s\n", cnt, "state.States[state.Symbols[\"test7\"]].State != TERMINATE_SUCCESS")
	}

	t.Logf("%d: %v - %s", cnt, state, req.Data)
	t.Errorf("SUCCESS")
}

func TestDAGState6(t *testing.T) {
	// Branch
	cnt := 6
	fmt.Printf("=============== %d: start ===============\n", cnt)
	state, req, err := runGraph(`
name: test-graph

nodes:
  test-slow-task:
    kind: TestNode
    spec:
      data: testdata1
  test2:
    kind: TestNode
    spec:
      data: testdata2
  test3:
    kind: TestNode
    spec:
      data: testdata3
      trigger_condition: one_success
  test4:
    kind: TestNode
    spec:
      data: testdata4

edges:
  test-slow-task:
    - test3
  test2:
    - test3
  test3:
    - test4
`)

	if err != nil {
		t.Fatalf("%d: %s\n", cnt, err.Error())
	}
	if state.GraphState != TERMINATE_SUCCESS {
		t.Fatalf("%d: %s\n", cnt, "state.GraphState != TERMINATE_SUCCESS")
	}
	t.Logf("%d: %v - %s", cnt, state, req.Data)
	t.Errorf("SUCCESS")
}
