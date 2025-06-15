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

// A Parser is a bundle of logic for extracting information from Artifacts.
type Parser interface {
	fmt.Stringer
	// Parse processes a single Artifact and produces results. If the error
	// returned Is a ErrParseFailure it just means something is unexpected about
	// the Artifact contents, otherwise it means something went completely wrong.
	Parse(artifact *falba.Artifact) (*ParseResult, error)
}

// Common Parser fields.
// TODO: all these arguments are a mess, this needs to be split up into
// different layers but it's not entirely clear the best way to do that.
// (Basically, parserBase should be a proper type exposed to the rest of Falba,
// but I can't see what the semantics of that type are supposed to be).
// Something like: there is one type responsible for actually parsing the
// content of the artifact, and one responsible for mapping the parsed content
// onto facts and metrics. But I can't quite see how those two types should
// interact.
type parserBase struct {
	Name string
	// Only produce metrics for artifacts matching this regexp.
	ArtifactRE *regexp.Regexp
	// The name of the metric that will be produced.
	MetricName string
	// The type of the value that will be produced.
	MetricType falba.ValueType
}

func newParserBase(name string, artifactPattern string, metricName string, metricType falba.ValueType) (*parserBase, error) {
	artifactRE, err := regexp.Compile(artifactPattern)
	if err != nil {
		return nil, fmt.Errorf("compiling artifact regexp pattern %q: %v", artifactPattern, err)
	}

	return &parserBase{
		Name:       name,
		ArtifactRE: artifactRE,
		MetricName: metricName,
		MetricType: metricType,
	}, nil
}

// RegexpParser is a parser that uses regexps provided by the user to extract
// facts and metrics.
type RegexpParser struct {
	parserBase
	// Currently this just supports extracting a single metric from an artifact.
	// The regexp must have zero or one capture groups. If there's a capture
	// group, the metric is taken from the submatch, otherwise from the match of
	// the full regexp.
	re *regexp.Regexp
}

func NewRegexpParser(name string, artifactPattern string, pattern string, metricName string, metricType falba.ValueType) (*RegexpParser, error) {
	base, err := newParserBase(name, artifactPattern, metricName, metricType)
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compiling regexp pattern %q: %v", pattern, err)
	}
	if re.NumSubexp() > 1 {
		return nil, fmt.Errorf("regexp %q contained %d sub-expressions, up to 1 is allowed", pattern, re.NumSubexp())
	}
	return &RegexpParser{
		parserBase: *base,
		re:         re,
	}, nil
}

func (p *RegexpParser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
	if !p.ArtifactRE.MatchString(artifact.Name) {
		return newParseResult(), nil
	}
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	matches := p.re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: no matches for %v in %v", ErrParseFailure, p.re, artifact)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w: multple matches for %v in %v, only one is allowed", ErrParseFailure, p.re, artifact)
	}
	match := matches[0][p.re.NumSubexp()]

	val, err := falba.ParseValue(string(match), p.MetricType)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseFailure, err)
	}

	return singleMetricResult(p.MetricName, val), nil
}

func (p *RegexpParser) String() string {
	return fmt.Sprintf("RegexpParser{%v -> %v(%q)}", p.re, p.MetricType, p.MetricName)
}

type JSONPathParser struct {
	parserBase
	selector *gval.Evaluable
}

func NewJSONPathParser(name string, artifactPattern string, expr string, metricName string, metricType falba.ValueType) (*JSONPathParser, error) {
	base, err := newParserBase(name, artifactPattern, metricName, metricType)
	if err != nil {
		return nil, err
	}
	selector, err := jsonpath.New(expr)
	if err != nil {
		return nil, fmt.Errorf("parsing JSONPath expression: %v", err)
	}
	return &JSONPathParser{
		parserBase: *base,
		selector:   &selector,
	}, nil
}

func (p *JSONPathParser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
	if !p.ArtifactRE.MatchString(artifact.Name) {
		return newParseResult(), nil
	}
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}
	var obj any
	if err := json.Unmarshal(content, &obj); err != nil {
		return nil, fmt.Errorf("%w: unmarshalling from JSON: %v", ErrParseFailure, err)
	}
	switch p.MetricType {
	case falba.ValueInt:
		val, err := p.selector.EvalInt(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return singleMetricResult(p.MetricName, &falba.IntValue{Value: val}), nil
	case falba.ValueString:
		val, err := p.selector.EvalString(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return singleMetricResult(p.MetricName, &falba.StringValue{Value: val}), nil
	case falba.ValueFloat:
		val, err := p.selector.EvalFloat64(context.Background(), obj)
		if err != nil {
			return nil, fmt.Errorf("%w: evaluating JSONPath as int: %v", ErrParseFailure, err)
		}
		return singleMetricResult(p.MetricName, &falba.FloatValue{Value: val}), nil
	default:
		panic("unimplemented")
	}
}

func (p *JSONPathParser) String() string {
	return fmt.Sprintf("JSONPathParser{%v -> %v(%q)}", p.selector, p.MetricType, p.MetricName)
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
func FromConfig(rawConfig json.RawMessage, name string) (Parser, error) {
	// To allow getting the type to determine which "real" struct to unmarshal
	// as, we first unmarshal to a supertype struct.
	var typedConfig struct {
		Type string
	}
	if err := json.Unmarshal(rawConfig, &typedConfig); err != nil {
		return nil, fmt.Errorf("decoding 'type' for parser: %v", err)
	}

	switch typedConfig.Type {
	case "single_metric":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config SingleMetricConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		t, err := falba.ParseValueType(config.Metric.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		return NewRegexpParser(name, config.ArtifactRegexp, ".+", config.Metric.Name, t)
	case "jsonpath":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config JSONPPathConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		t, err := falba.ParseValueType(config.Metric.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		return NewJSONPathParser(name, config.ArtifactRegexp, config.JSONPath, config.Metric.Name, t)
	default:
		return nil, fmt.Errorf("unknown parser type %q", typedConfig.Type)
	}
}
