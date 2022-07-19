package builtins

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"strings"
)

func initMLBuiltins() {
	for k, v := range mlBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var mlBuiltins = map[string]builtinDefinition{
	"znbase_ml.import_model": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types: tree.ArgTypes{
				{"model_name", types.String},
				{"problem_type", types.String},
				{"model_type", types.String},
				{"features", types.StringArray},
				{"target", types.String},
				{"model_content", types.Bytes},
				{"config", types.Jsonb},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// model name (not null)
				if err := requireNonNull(args[0]); err != nil {
					return nil, err
				}
				modelName := string(tree.MustBeDString(args[0]))
				// problem type (not null)
				problemType := string(tree.MustBeDString(args[1]))
				// model type (not null)
				modelType := string(tree.MustBeDString(args[2]))
				// features set (not null)
				var features []string
				for _, s := range tree.MustBeDArray(args[3]).Array {
					features = append(features, string(tree.MustBeDString(s)))
				}
				// target (not null)
				target := string(tree.MustBeDString(args[4]))
				// model content (not null)
				modelContent := []byte(tree.MustBeDBytes(args[5]))

				// model config
				// modelConfig := string(tree.MustBeDStringOrDNull(args[6]))
				modelConfig := tree.MustBeDJSON(args[6]).JSON.String()

				// print
				fmt.Println("model_name: " + modelName + ", problem_type: " + problemType +
					", model_type: " + modelType + ", feature_set: " + strings.Join(features, ",") +
					", target: " + target + ", model_content: " + string(modelContent) + ", config: " + modelConfig)

				// configs
				if modelConfig != "" {
					var config map[string]interface{}
					decoder := json.NewDecoder(
						bytes.NewReader([]byte(modelConfig)))
					decoder.UseNumber()
					decoder.Decode(&config)

					for k, v := range config {
						switch v := v.(type) {
						case string:
							fmt.Println(k, v, "(string)")
						case json.Number:
							if i, err := v.Int64(); err != nil {
								f, _ := v.Float64()
								fmt.Println(k, f, "(float64)")
							} else {
								fmt.Println(k, i, "(int64)")
							}
						default:
							fmt.Println(k, v, "(unknown)")
						}
					}
				}

				if err := ctx.ML.CreateMLModel(ctx.Context, 1, "test_model"); err != nil {
					return nil, err
				}

				return tree.NewDString(modelName), nil
			},
			Info:       "Creates a machine learning model.",
			Volatility: volatility.Volatile,
		},
	),
}
