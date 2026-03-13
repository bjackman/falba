package parser

import (
	"fmt"

	"github.com/PaesslerAG/jsonpath"
	"github.com/bjackman/falba/internal/falba"
	"gopkg.in/yaml.v3"
)

type YAMLPathExtractor struct {
	resultType falba.ValueType
	expression string
}

func NewYAMLPathExtractor(expr string, resultType falba.ValueType) (*YAMLPathExtractor, error) {
	return &YAMLPathExtractor{
		expression: expr,
		resultType: resultType,
	}, nil
}

func (e *YAMLPathExtractor) Extract(artifact *falba.Artifact) ([]falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}
	var obj any
	if err := yaml.Unmarshal(content, &obj); err != nil {
		return nil, fmt.Errorf("%w: unmarshalling from YAML: %v", ErrParseFailure, err)
	}

	// yaml.v3 creates map[string]interface{} by default when decoding into any,
	// which is perfectly compatible with jsonpath.
	got, err := jsonpath.Get(e.expression, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate JSONPath on YAML: %v", err)
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
			switch v := rawVal.(type) {
			case float64:
				val = &falba.IntValue{Value: int64(v)}
			case int:
				val = &falba.IntValue{Value: int64(v)}
			default:
				return nil, fmt.Errorf("%w: YAMLPath returned %T, wanted numeric", ErrParseFailure, rawVal)
			}
		case falba.ValueString:
			v, ok := rawVal.(string)
			if !ok {
				return nil, fmt.Errorf("%w: YAMLPath returned %T, wanted string", ErrParseFailure, rawVal)
			}
			val = &falba.StringValue{Value: v}
		case falba.ValueFloat:
			v, ok := rawVal.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: YAMLPath returned %T, wanted float64", ErrParseFailure, rawVal)
			}
			val = &falba.FloatValue{Value: v}
		case falba.ValueBool:
			v, ok := rawVal.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: YAMLPath returned %T, wanted bool", ErrParseFailure, rawVal)
			}
			val = &falba.BoolValue{Value: v}
		default:
			panic("unimplemented")
		}
		result = append(result, val)
	}
	return result, nil
}

func (p *YAMLPathExtractor) String() string {
	return fmt.Sprintf("YAMLPathParser{%q -> %v}", p.expression, p.resultType)
}

type YAMLPathConfig struct {
	BaseParserConfig
	JSONPath string `json:"jsonpath"`
}

func (c *YAMLPathConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if c.JSONPath == "" {
		return fmt.Errorf("missing/empty 'jsonpath' field")
	}
	return nil
}
