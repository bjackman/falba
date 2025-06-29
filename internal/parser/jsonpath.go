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

func (e *JSONPathExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
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

	var gotVal any
	switch got := got.(type) {
	case []any:
		// JSONPath seems to be weird and annoying when you use its
		// filtering functionality, AFAICS it doesn't have a built-in
		// facility to extract an individual value. So we just allow it to
		// return a slice of length 1.
		if len(got) != 1 {
			return nil, fmt.Errorf("%w: JSONPath returned %d values, expected 1", ErrParseFailure, len(got))
		}
		gotVal = got[0]
	default:
		gotVal = got
	}

	switch e.resultType {
	case falba.ValueInt:
		// JSON doesn't have proper numeric types so we can't actually enforce
		// that the value is an integer. Just squash it into one.
		switch v := gotVal.(type) {
		case float64:
			return &falba.IntValue{Value: int64(v)}, nil
		case int:
			return &falba.IntValue{Value: int64(v)}, nil
		default:
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted numeric", ErrParseFailure, gotVal)
		}
	case falba.ValueString:
		val, ok := gotVal.(string)
		if !ok {
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted string", ErrParseFailure, gotVal)
		}
		return &falba.StringValue{Value: val}, nil
	case falba.ValueFloat:
		val, ok := gotVal.(float64)
		if !ok {
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted float64", ErrParseFailure, gotVal)
		}
		return &falba.FloatValue{Value: val}, nil
	case falba.ValueBool:
		val, ok := gotVal.(bool)
		if !ok {
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted bool", ErrParseFailure, gotVal)
		}
		return &falba.BoolValue{Value: val}, nil
	default:
		panic("unimplemented")
	}
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
