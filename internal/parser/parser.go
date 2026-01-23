// Package parser contains logic for parsing metrics and facts from artifacts
package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/unit"
)

// ParseResult is just  halper to avoid typing out verbose map and slice biz.
// TODO: This is wack, still figuring out these details  of the data model, so
// probably this type makes no sense anyway.
type ParseResult struct {
	Facts   map[string]falba.Value
	Metrics []*falba.Metric
}

func emptyParseResult() *ParseResult {
	return &ParseResult{
		Facts:   map[string]falba.Value{},
		Metrics: []*falba.Metric{},
	}
}

var ErrParseFailure = errors.New("parse failure")

// An Extractor contains the core logic for reading a value from an artifact.
type Extractor interface {
	fmt.Stringer
	// Parse processes a single Artifact and produces results. If the error
	// returned Is a ErrParseFailure it just means something is unexpected about
	// the Artifact contents, otherwise it means something went completely wrong.
	Extract(artifact *falba.Artifact) ([]falba.Value, error)
}

type TargetType int

const (
	TargetFact TargetType = iota
	TargetMetric
)

// Describes the thing a parser produces, i.e. a fact or metric.
type ParserTarget struct {
	Name       string
	TargetType TargetType
	ValueType  falba.ValueType
	Unit       *unit.Unit
}

// A Parser is a bundle of logic for extracting information from Artifacts.
type Parser struct {
	Name string
	// Only produce metrics for artifacts matching this regexp.
	ArtifactRE *regexp.Regexp
	Target     *ParserTarget
	Extractor
}

func NewParser(name string, artifactPattern string, target *ParserTarget, extractor Extractor) (*Parser, error) {
	artifactRE, err := regexp.Compile(artifactPattern)
	if err != nil {
		return nil, fmt.Errorf("compiling artifact regexp pattern %q: %v", artifactPattern, err)
	}

	return &Parser{
		Name:       name,
		ArtifactRE: artifactRE,
		Target:     target,
		Extractor:  extractor,
	}, nil
}

// Parse extract facts and metrics from an artifact.
//
// Ahhh right, clarity: Yes, we want the flexibility to output _multiple samples
// of the same metric_. We don't really care about producing multiple different
// facts or metrics, I think.
func (p *Parser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
	if !p.ArtifactRE.MatchString(artifact.Name) {
		return emptyParseResult(), nil
	}
	vals, err := p.Extractor.Extract(artifact)
	if err != nil {
		return nil, err
	}
	// TODO: This interface is a bit wack. Probably wanna refactor it to just
	// force facts parsers to return exactly one value in the type system.
	if len(vals) == 0 {
		return nil, fmt.Errorf("parser %q produced no values (should hve been ErrParseFailure)", p.Name)
	}
	// TODO: Is it OK that we are kinda forgetting the expected type here?
	r := emptyParseResult()
	if p.Target.TargetType == TargetMetric {
		for _, val := range vals {
			r.Metrics = append(r.Metrics, &falba.Metric{Name: p.Target.Name, Value: val, Unit: p.Target.Unit})
		}
	} else {
		if len(vals) != 1 {
			return nil, fmt.Errorf("fact parser %q produced multiple values. This is only allowed for metric parsers", p.Name)
		}
		r.Facts[p.Target.Name] = vals[0]
	}
	return r, nil
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

func (e *RegexpExtractor) Extract(artifact *falba.Artifact) ([]falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	matches := e.re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: no matches for %v in %v", ErrParseFailure, e.re, artifact)
	}
	// TODO: Support multiple matches
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w: multple matches for %v in %v, only one is allowed", ErrParseFailure, e.re, artifact)
	}
	match := matches[0][e.re.NumSubexp()]

	val, err := falba.ParseValue(string(match), e.resultType)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseFailure, err)
	}

	return []falba.Value{val}, nil
}

func (p *RegexpExtractor) String() string {
	return fmt.Sprintf("RegexpExtractor{%v -> %v}", p.re, p.resultType)
}

type BaseParserConfig struct {
	Type string `json:"type"`
	// Parse the artifact if its path (relative to the artifacts dir) matches
	// this regexp.
	ArtifactRegexp string `json:"artifact_regexp"`
	// Specify either the metric to produce, or the fact to produce.
	Metric *struct {
		Name string `json:"name"`
		Type string `json:"type"`
		Unit string `json:"unit"`
	} `json:"metric"`
	Fact *struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"fact"`
}

type ShellvarParserConfig struct {
	BaseParserConfig
	Var string `json:"var"` // Name of the shell variable to extract
}

func (c *ShellvarParserConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if c.Var == "" {
		return fmt.Errorf("missing/empty 'var' field for shellvar parser")
	}
	return nil
}

type CommandParserConfig struct {
	BaseParserConfig
	Args []string `json:"args"` // Command arguments to execute
}

func (c *CommandParserConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if len(c.Args) == 0 {
		return fmt.Errorf("missing/empty 'args' field for command parser")
	}
	return nil
}

// This just checks if the config structure has the right fields, it doesn't
// check if their content is correct.
func (c *BaseParserConfig) ValidateFields() error {
	if c.Type == "" {
		return fmt.Errorf("missing/empty 'type' field")
	}
	if c.ArtifactRegexp == "" {
		return fmt.Errorf("missing/empty 'artifact_regexp' field")
	}
	if (c.Metric != nil) == (c.Fact != nil) {
		return fmt.Errorf("specify exactly one of 'metric' and 'fact'")
	}
	if c.Metric != nil {
		if c.Metric.Name == "" {
			return fmt.Errorf("missing/empty 'metric.name' field")
		}
		if c.Metric.Type == "" {
			return fmt.Errorf("missing/empty 'metric.type' field")
		}
	} else {
		if c.Fact.Name == "" {
			return fmt.Errorf("missing/empty 'fact.name' field")
		}
		if c.Fact.Type == "" {
			return fmt.Errorf("missing/empty 'fact.type' field")
		}
	}
	return nil
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

	var target ParserTarget
	if baseConfig.Metric != nil {
		valueType, err := falba.ParseValueType(baseConfig.Metric.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		u, err := unit.Parse(baseConfig.Metric.Unit)
		if err != nil {
			return nil, fmt.Errorf("parsing unit: %v", err)
		}
		target = ParserTarget{
			TargetType: TargetMetric,
			Name:       baseConfig.Metric.Name,
			ValueType:  valueType,
			Unit:       u,
		}
	} else if baseConfig.Fact != nil {
		if falba.IsReservedFactName(baseConfig.Fact.Name) {
			return nil, fmt.Errorf("fact name %q is reserved (%s)", baseConfig.Fact.Name, falba.GetReservedFactNamesString())
		}
		valueType, err := falba.ParseValueType(baseConfig.Fact.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		target = ParserTarget{
			TargetType: TargetFact,
			Name:       baseConfig.Fact.Name,
			ValueType:  valueType,
		}
	} else {
		return nil, fmt.Errorf("must specify 'fact.type' or 'value.type'")
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
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewRegexpExtractor(".+", target.ValueType)
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
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewJSONPathExtractor(config.JSONPath, target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up JSONPath extractor: %v", err)
		}
	case "shellvar":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config ShellvarParserConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding shellvar parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewShellvarExtractor(config.Var, target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up Shellvar extractor: %v", err)
		}
	case "command":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config CommandParserConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding command parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewCommandExtractor(config.Args, target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up Command extractor: %v", err)
		}
	case "artifact_presence":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config ArtifactPresenceConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding artifact_presence parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		result, err := falba.ValueFromAny(config.Result)
		if err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		extractor = &ArtifactPresenceExtractor{result: result}
	default:
		return nil, fmt.Errorf("unknown parser type %q", baseConfig.Type)
	}

	return NewParser(name, baseConfig.ArtifactRegexp, &target, extractor)
}
