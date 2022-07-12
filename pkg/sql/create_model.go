package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createModelNode struct {
	n *tree.CreateModel
}

func (p *planner) CreateModel(ctx context.Context, n *tree.CreateModel) (planNode, error) {
	return &createModelNode{n: n}, nil
}

func (n *createModelNode) startExec(params runParams) error {
	return nil
}

func (*createModelNode) Next(runParams) (bool, error) { return false, nil }
func (*createModelNode) Values() tree.Datums          { return tree.Datums{} }
func (*createModelNode) Close(context.Context)        {}
