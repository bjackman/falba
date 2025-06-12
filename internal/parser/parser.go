// Package parser contains logic for parsing metrics and facts from artifacts
package parser

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/bjackman/falba/internal/falba"
)

// ParseResult is just  halper to avoid typing out verbose map and slice biz.
type ParseResult struct {
	Facts   map[string]*falba.Value
	Metrics []*falba.Metric
}

func newParseResult() *ParseResult {
	return &ParseResult{
		Facts:   map[string]*falba.Value{},
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
	// Parse processes a single Artifact and produces results. If the error
	// returned Is a ErrParseFailure it just means something is unexpected about
	// the Artifact contents, otherwise it means something went completely wrong.
	Parse(artifact *falba.Artifact) (*ParseResult, error)
}

// RegexpParser is a parser that uses regexps provided by the user to extract
// facts and metrics.
type RegexpParser struct {
	// Currently this just supports extracting a single metric from an artifact.
	// The regexp must have zero or one capture groups. If there's a capture
	// group, the metric is taken from the submatch, otherwise from the match of
	// the full regexp.
	re *regexp.Regexp
	// The name of the metric that will be produced.
	MetricName string
	// The type of the value that will be produced.
	MetricType falba.ValueType
}

func NewRegexpParser(pattern string, metricName string, metricType falba.ValueType) (*RegexpParser, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compiling regexp pattern %q: %v", pattern, err)
	}
	if re.NumSubexp() > 1 {
		return nil, fmt.Errorf("regexp %q contained %d sub-expressions, up to 1 is allowed", pattern, re.NumSubexp())
	}
	return &RegexpParser{re: re, MetricName: metricName, MetricType: metricType}, nil
}

func (p *RegexpParser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
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
