package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CreateMLModelRecord creates a machine learning model in system.ml_models
func CreateMLModelRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, ID uint64, modelName string,
) error {
	// Insert into the ml model table and detect collisions.
	if num, err := execCfg.InternalExecutor.ExecEx(
		ctx, "create-ml-model", txn, sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.ml_models (id, name) VALUES ($1, $2)`,
		ID, modelName,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "model \"%d\" already exists", ID)
		}
		return errors.Wrap(err, "inserting new ml model")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}
	return nil
}

// CreateMLModel implements ?
func (p *planner) CreateMLModel(ctx context.Context, ID uint64, name string) error {
	if err := CreateMLModelRecord(ctx, p.ExecCfg(), p.Txn(), ID, name); err != nil {
		return err
	}
	return nil
}
