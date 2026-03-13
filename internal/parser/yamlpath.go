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

	got, err := jsonpath.Get(e.expression, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate JSONPath on YAML: %v", err)
	}

	return evalJSONPathResult(got, e.resultType, "YAMLPath")
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
