package parser

import (
	"encoding/json"
	"fmt"

	"github.com/PaesslerAG/jsonpath"
	"github.com/bjackman/falba/internal/falba"
)

type JSONPathExtractor struct {
	resultType falba.ValueType
	expression string
}

func NewJSONPathExtractor(expr string, resultType falba.ValueType) (*JSONPathExtractor, error) {
	return &JSONPathExtractor{
		expression: expr,
		resultType: resultType,
	}, nil
}

func (e *JSONPathExtractor) Extract(artifact *falba.Artifact) ([]falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}
	var obj any
	if err := json.Unmarshal(content, &obj); err != nil {
		return nil, fmt.Errorf("%w: unmarshalling from JSON: %v", ErrParseFailure, err)
	}

	// We'd prefer to pre-compile the JSONPath expression but then evaluating it
	// gies you a gval.Evaluable which I can't be bothered to deal with, I don't
	// know how to get non-scalar objects out of it. So instead we just evaluate
	// it as string "at runtime" which gives us an untyped result we can
	// manually try to squash into the type we want.
	got, err := jsonpath.Get(e.expression, obj)
	if err != nil {
		// I believe this error must mean there's something wrong with the
		// expression, not just that it didn't match anything. So this is fatal.
		return nil, fmt.Errorf("failed to evaluate JSONPath: %v", err)
	}

	var rawValues []any
	switch got := got.(type) {
	case []any:
		rawValues = got
	default:
		rawValues = []any{got}
	}

	var result []falba.Value
	for _, rawVal := range rawValues {
		var val falba.Value
		switch e.resultType {
		case falba.ValueInt:
			// JSON doesn't have proper numeric types so we can't actually enforce
			// that the value is an integer. Just squash it into one.
			switch v := rawVal.(type) {
			case float64:
				val = &falba.IntValue{Value: int64(v)}
			case int:
				val = &falba.IntValue{Value: int64(v)}
			default:
				return nil, fmt.Errorf("%w: JSONPath returned %T, wanted numeric", ErrParseFailure, rawVal)
			}
		case falba.ValueString:
			v, ok := rawVal.(string)
			if !ok {
				return nil, fmt.Errorf("%w: JSONPath returned %T, wanted string", ErrParseFailure, rawVal)
			}
			val = &falba.StringValue{Value: v}
		case falba.ValueFloat:
			v, ok := rawVal.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: JSONPath returned %T, wanted float64", ErrParseFailure, rawVal)
			}
			val = &falba.FloatValue{Value: v}
		case falba.ValueBool:
			v, ok := rawVal.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: JSONPath returned %T, wanted bool", ErrParseFailure, rawVal)
			}
			val = &falba.BoolValue{Value: v}
		default:
			panic("unimplemented")
		}
		result = append(result, val)
	}
	return result, nil
}

func (p *JSONPathExtractor) String() string {
	return fmt.Sprintf("JSONPathParser{%q -> %v}", p.expression, p.resultType)
}

type JSONPPathConfig struct {
	BaseParserConfig
	JSONPath string `json:"jsonpath"`
}

func (c *JSONPPathConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if c.JSONPath == "" {
		return fmt.Errorf("missing/empty 'jsonpath' field")
	}
	return nil
}
