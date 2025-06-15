// Package parser contains logic for parsing metrics and facts from artifacts
package parser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/bjackman/falba/internal/falba"
)

// ParseResult is just  halper to avoid typing out verbose map and slice biz.
type ParseResult struct {
	Facts   map[string]falba.Value
	Metrics []*falba.Metric
}

func newParseResult() *ParseResult {
	return &ParseResult{
		Facts:   map[string]falba.Value{},
		Metrics: []*falba.Metric{},
	}
}

func singleMetricResult(name string, val falba.Value) *ParseResult {
	r := newParseResult()
	r.Metrics = append(r.Metrics, &falba.Metric{Name: name, Value: val})
	return r
}

var ErrParseFailure = errors.New("parse failure")

// An Extractor contains the core logic for reading a value from an artifact.
type Extractor interface {
	fmt.Stringer
	// Parse processes a single Artifact and produces results. If the error
	// returned Is a ErrParseFailure it just means something is unexpected about
	// the Artifact contents, otherwise it means something went completely wrong.
	Extract(artifact *falba.Artifact) (falba.Value, error)
}

// A Parser is a bundle of logic for extracting information from Artifacts.
type Parser struct {
	Name string
	// Only produce metrics for artifacts matching this regexp.
	ArtifactRE *regexp.Regexp
	// The name of the metric that will be produced.
	MetricName string
	// The type of the value that will be produced.
	MetricType falba.ValueType
	Extractor
}

func NewParser(name string, artifactPattern string, metricName string, metricType falba.ValueType, extractor Extractor) (*Parser, error) {
	artifactRE, err := regexp.Compile(artifactPattern)
	if err != nil {
		return nil, fmt.Errorf("compiling artifact regexp pattern %q: %v", artifactPattern, err)
	}

	return &Parser{
		Name:       name,
		ArtifactRE: artifactRE,
		MetricName: metricName,
		MetricType: metricType,
		Extractor:  extractor,
	}, nil
}

// Parse extract facts and metrics from an artifact.
// TODO: This only supports each parser producing a single metric/fact. I'm
// starting to think this is actually a nice simplification. It's less flexible,
// but isn't the whole point of this design that, if you think you wanna gather
// zillions of facts, you are probably wrong? You only need to extract the ones
// you're actually capable of analysing.
func (p *Parser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
	if !p.ArtifactRE.MatchString(artifact.Name) {
		return newParseResult(), nil
	}
	val, err := p.Extractor.Extract(artifact)
	if err != nil {
		return nil, err
	}
	// TODO: Is it OK that we are kinda forgetting the expected type here?
	return singleMetricResult(p.MetricName, val), nil
}

// RegexpExtractor is an extractor that uses regexps provided by the user to
// extract facts and metrics.
type RegexpExtractor struct {
	resultType falba.ValueType
	// Currently this just supports extracting a single metric from an artifact.
	// The regexp must have zero or one capture groups. If there's a capture
	// group, the metric is taken from the submatch, otherwise from the match of
	// the full regexp.
	re *regexp.Regexp
}

func NewRegexpExtractor(pattern string, resultType falba.ValueType) (*RegexpExtractor, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compiling regexp pattern %q: %v", pattern, err)
	}
	if re.NumSubexp() > 1 {
		return nil, fmt.Errorf("regexp %q contained %d sub-expressions, up to 1 is allowed", pattern, re.NumSubexp())
	}
	return &RegexpExtractor{re: re, resultType: resultType}, nil
}

func (e *RegexpExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	matches := e.re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: no matches for %v in %v", ErrParseFailure, e.re, artifact)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w: multple matches for %v in %v, only one is allowed", ErrParseFailure, e.re, artifact)
	}
	match := matches[0][e.re.NumSubexp()]

	val, err := falba.ParseValue(string(match), e.resultType)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseFailure, err)
	}

	return val, nil
}

func (p *RegexpExtractor) String() string {
	return fmt.Sprintf("RegexpExtractor{%v -> %v}", p.re, p.resultType)
}

type JSONPathExtractor struct {
	resultType falba.ValueType
	selector   *gval.Evaluable
}

func NewJSONPathExtractor(expr string, resultType falba.ValueType) (*JSONPathExtractor, error) {
	selector, err := jsonpath.New(expr)
	if err != nil {
		return nil, fmt.Errorf("parsing JSONPath expression: %v", err)
	}
	return &JSONPathExtractor{
		selector:   &selector,
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
	switch e.resultType {
	case falba.ValueInt:
		val, err := e.selector.EvalInt(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return &falba.IntValue{Value: val}, nil
	case falba.ValueString:
		val, err := e.selector.EvalString(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return &falba.StringValue{Value: val}, nil
	case falba.ValueFloat:
		val, err := e.selector.EvalFloat64(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return &falba.FloatValue{Value: val}, nil
	default:
		panic("unimplemented")
	}
}

func (p *JSONPathExtractor) String() string {
	return fmt.Sprintf("JSONPathParser{%v -> %v}", p.selector, p.resultType)
}

type BaseParserConfig struct {
	Type string `json:"type"`
	// Parse the artifact if its path (relative to the artifacts dir) matches
	// this regexp.
	ArtifactRegexp string `json:"artifact_regexp"`
	Metric         struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"metric"`
}

type JSONPPathConfig struct {
	BaseParserConfig
	JSONPath string `json:"jsonpath"`
}

// Config for a parser that just reads a single metric from a file, using its
// entire content.
type SingleMetricConfig struct {
	BaseParserConfig
}

// Read a configuration entry for a single parser and return it.
func FromConfig(rawConfig json.RawMessage, name string) (*Parser, error) {
	// First parse the common fields, this enables us to get the type, then we
	// can subsequently parse all the remaining fields.
	var baseConfig BaseParserConfig
	if err := json.Unmarshal(rawConfig, &baseConfig); err != nil {
		return nil, fmt.Errorf("decoding 'type' for parser: %v", err)
	}

	resultType, err := falba.ParseValueType(baseConfig.Metric.Type)
	if err != nil {
		return nil, fmt.Errorf("parsing metric type: %v", err)
	}

	var extractor Extractor

	switch baseConfig.Type {
	case "single_metric":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config SingleMetricConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		var err error
		extractor, err = NewRegexpExtractor(".+", resultType)
		if err != nil {
			return nil, fmt.Errorf("setting up single-value extractor: %v", err)
		}
	case "jsonpath":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config JSONPPathConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		var err error
		extractor, err = NewJSONPathExtractor(config.JSONPath, resultType)
		if err != nil {
			return nil, fmt.Errorf("setting up JSONPath extractor: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown parser type %q", baseConfig.Type)
	}

	return NewParser(name, baseConfig.ArtifactRegexp, baseConfig.Metric.Name, resultType, extractor)
}
