package dag

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	yaml "gopkg.in/yaml.v2"
)

type NodeYAMLConfig struct {
	Kind string
	Spec map[string]interface{}
}

type DAGYAMLConfig struct {
	Name    string                     `yaml:"name"`
	Timeout string                     `yaml:"timeout"`
	Nodes   map[string]*NodeYAMLConfig `yaml:"nodes"`
	Edges   map[string][]string        `yaml:"edges"`
}

func LoadNode(ctx context.Context, name string, spec map[string]interface{}, v Node) error {
	var b bytes.Buffer

	err := yaml.NewEncoder(&b).Encode(spec)
	if err != nil {
		return err
	}

	decoder := yaml.NewDecoder(&b)
	err = decoder.Decode(v)
	if err != nil {
		return err
	}

	if err = v.Init(ctx, name); err != nil {
		return err
	}
	return nil
}

func LoadGraph(ctx context.Context, r io.Reader) (g *DAG, err error) {
	conf := &DAGYAMLConfig{}
	decoder := yaml.NewDecoder(r)

	err1 := decoder.Decode(conf)
	if err1 != nil {
		err = fmt.Errorf("decode conf faild: %s", err1.Error())
		return
	}

	symbols := make(map[string]int)
	nodes := make([]Node, len(conf.Nodes))
	outgoing := make([][]int, len(conf.Nodes))
	incoming := make([][]int, len(conf.Nodes))
	idx := 0
	for vn, vconf := range conf.Nodes {
		node, err2 := CreateNode(ctx, vconf.Kind)
		if err2 != nil {
			err = fmt.Errorf("create node faild: %s", err2.Error())
			return
		}
		err2 = LoadNode(ctx, vn, vconf.Spec, node)
		if err2 != nil {
			err = fmt.Errorf("init node faild: %s", err2.Error())
			return
		}

		if node.ParallelFailover() && !node.FailoverOnTimeout() && !node.FailoverOnError() {
			err = fmt.Errorf("node %s parallel failover must be with failover on timeout or error", vn)
			return
		}

		if (node.IsBranch() && len(node.GetBranchNames()) == 0) || (!node.IsBranch() && len(node.GetBranchNames()) > 0) {
			err = fmt.Errorf("node %s branchs must not be empty or node isn't a branch node", vn)
			return
		}

		symbols[vn] = idx
		nodes[idx] = node
		idx += 1
	}

	for vn, vlist := range conf.Edges {
		vidx, ok := symbols[vn]
		if !ok {
			err = fmt.Errorf("node %s not found", vn)
			return
		}
		// vidx -> widx...
		for _, wn := range vlist {
			widx, ok := symbols[wn]
			if !ok {
				err = fmt.Errorf("node %s not found, start at %s", wn, vn)
				return
			}
			outgoing[vidx] = append(outgoing[vidx], widx)
			incoming[widx] = append(incoming[widx], vidx)
		}
	}

	for _, vidx := range symbols {
		if nodes[vidx].IsBranch() {
			for _, branchName := range nodes[vidx].GetBranchNames() {
				for _, branch := range nodes[vidx].GetBranchs(branchName) {
					if bidx, ok := symbols[branch]; !ok {
						err = fmt.Errorf("branch %s in node %s not found", branch, nodes[vidx].Name())
						return
					} else {
						found := false
						for _, widx := range outgoing[vidx] {
							if widx == bidx {
								found = true
								break
							}
						}
						if !found {
							err = fmt.Errorf("branch %s isn't downstream of node %s", branch, nodes[vidx].Name())
							return
						}
					}
				}
			}
		}
	}

	duration := MaxNodeTimeoutDuration
	if conf.Timeout != "" {
		if duration, err1 = time.ParseDuration(conf.Timeout); err1 != nil {
			err = fmt.Errorf("parse timeout `%s` failed: %s", conf.Timeout, err)
			return
		}
	}

	g = &DAG{
		Name:     conf.Name,
		Timeout:  duration,
		Symbols:  symbols,
		Nodes:    nodes,
		Outgoing: outgoing,
		Incoming: incoming,
	}
	return g, nil
}
