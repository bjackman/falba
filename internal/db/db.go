// Package db contains the logic to glue data together into a database.
package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/parser"
)

// A DB is a collection of results read from a directory. Each entry in the
// directory is of the format $test_name:$test_id. It contains a directory
// called artifacts/ which contains the artifacts.
type DB struct {
	RootDir   string
	Results   []*falba.Result
	FactTypes map[string]falba.ValueType
}

// Serializable returns a representation of the results in the DB that can be
// marshalled as JSON or whatever.
func (d *DB) SerializableResults() []map[string]any {
	var ret []map[string]any
	for _, r := range d.Results {
		ret = append(ret, r.Serializable())
	}
	return ret
}

func isDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func readResult(resultDir string, parsers []*parser.Parser) (*falba.Result, error) {
	resultName := filepath.Base(resultDir)
	testName, resultID, ok := strings.Cut(resultName, ":")
	if !ok || testName == "" || resultID == "" {
		return nil, fmt.Errorf("invalid result name (should be $result_name:$result_id) at %v", resultDir)
	}

	// Find artifacts. At present every leaf file is an artifact. It might make
	// sense to support having a whole directory be a single artifact at some
	// point.
	artifactsDirRel := filepath.Join(resultDir, "artifacts")
	artifactsDir, err := filepath.Abs(artifactsDirRel)
	if err != nil {
		return nil, fmt.Errorf("converting artifacts dir path %v to absolute: %v", artifactsDirRel, err)
	}
	artifacts := []*falba.Artifact{}
	visit := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		isDir, err := isDir(path)
		if err != nil {
			return fmt.Errorf("check if %v is dir: %w", path, err)
		}
		if isDir {
			return nil
		}
		name, err := filepath.Rel(artifactsDir, path)
		if err != nil {
			log.Panicf("Encountered file %q not in artifacts dir %q while walking artifacts dir", path, artifactsDir)
		}
		artifacts = append(artifacts, &falba.Artifact{Name: name, Path: path})
		return nil
	}
	if err := filepath.WalkDir(artifactsDir, visit); err != nil {
		return nil, fmt.Errorf("walking artifacts/ dir: %w", err)
	}

	// Run parsers.

	facts := map[string]falba.Value{}
	metrics := []*falba.Metric{}

	// Remember which parser produced a fact so we can give a nice error message
	// for duplicates.
	factToParser := map[string]string{}

	for _, artifact := range artifacts {
		for _, parzer := range parsers {
			result, err := parzer.Parse(artifact)
			// Parse failures are non-fatal.
			if errors.Is(err, parser.ErrParseFailure) {
				log.Printf("Parser %s failed to parse artifact %v: %v", parzer, artifact, err)
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("parsing %v with %v: %w", artifact, parzer, err)
			}

			// Store facts, checking duplicates.
			for name, fact := range result.Facts {
				if _, ok := facts[name]; ok {
					return nil, fmt.Errorf("parser %s produced fact %q, but that was already produced by parser %s", parzer, name, factToParser[name])
				}
				factToParser[name] = "foo"
				facts[name] = fact
			}

			metrics = append(metrics, result.Metrics...)
		}
	}

	return &falba.Result{
		TestName: testName, ResultID: resultID, Artifacts: artifacts, Metrics: metrics, Facts: facts,
	}, nil

}

// Config file written by the user that tells Falba how to parse data out of the
// artifacts.
type ParsersConfig struct {
	Parsers map[string]json.RawMessage `json:"parsers"`
}

func parseParserConfig(configPath string) ([]*parser.Parser, error) {
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading DB config from %v: %w", configPath, err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(configContent)))
	decoder.DisallowUnknownFields()

	var config ParsersConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decoding DB config: %w", err)
	}
	var parsers []*parser.Parser
	for name, parserConfig := range config.Parsers {
		parser, err := parser.FromConfig(parserConfig, name)
		if err != nil {
			return nil, fmt.Errorf("configuring parser %q: %w", name, err)
		}
		parsers = append(parsers, parser)
	}
	if len(parsers) == 0 {
		return nil, fmt.Errorf("no 'parsers' defined")
	}
	return parsers, nil
}

// Read all the results from a DB directory and parse all their facts and
// metrics.
func ReadDB(rootDir string) (*DB, error) {
	parsers, err := parseParserConfig(filepath.Join(rootDir, "parsers.json"))
	if err != nil {
		return nil, err
	}

	// Ensure parsers produce the same type for each fact and metric.
	// Note that it's not fundamentally forbidden to have two parsers that
	// produce the same output. For metrics that's just totally fine. For facts
	// it will produce an error later if multiple parsers produce a fact for the
	// same result, though.
	// While we're at it, also remember the fact types as they'll be used to
	// construct a results tablellater.
	factTypes := map[string]falba.ValueType{}
	allTypes := map[string]falba.ValueType{}
	for _, p := range parsers {
		if t, ok := allTypes[p.Target.Name]; ok && p.Target.ValueType != t {
			return nil, fmt.Errorf("parser %v produced fact/metric %q of type %v, but another outputs this as %v",
				p, p.Target.Name, p.Target.ValueType, t)
		}
		if p.Target.TargetType == parser.TargetFact {
			factTypes[p.Target.Name] = p.Target.ValueType
		}
	}

	dir, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("opening DB root: %w", err)
	}
	results := []*falba.Result{}
	for _, entry := range dir {
		if entry.Name() == "parsers.json" {
			continue
		}
		resultDir := filepath.Join(rootDir, entry.Name())
		result, err := readResult(resultDir, parsers)
		if err != nil {
			return nil, fmt.Errorf("reading result from %v: %w", resultDir, err)
		}
		results = append(results, result)
	}
	return &DB{RootDir: rootDir, Results: results, FactTypes: factTypes}, nil
}
