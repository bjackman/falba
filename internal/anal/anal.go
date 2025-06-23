// Package anal contains routines for analysis of result data.
package anal

import (
	"bytes"
	"cmp"
	"database/sql"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"text/template"

	"github.com/bjackman/falba/internal/db"
	"github.com/bjackman/falba/internal/falba"
	"github.com/marcboeker/go-duckdb"
)

// Prepared statements aren't flexible enough so we are just gonna be
// vulnerable to SQL injection here.
var filterResultsTemplate = template.Must(template.New("group-by").Parse(`
	CREATE OR REPLACE TABLE filtered_results AS (
		SELECT * FROM results WHERE {{.FilterExpression}}
	);
`))

type filterResultsTemplateArgs struct {
	FilterExpression string
}

func (g *filterResultsTemplateArgs) Execute() (string, error) {
	var b bytes.Buffer
	if err := filterResultsTemplate.Execute(&b, g); err != nil {
		return "", err
	}
	return b.String(), nil
}

func createFilteredResults(sqlDB *sql.DB, filterExpression string) error {
	t := filterResultsTemplateArgs{
		FilterExpression: filterExpression,
	}
	query, err := t.Execute()
	if err != nil {
		return fmt.Errorf("templating group-by query: %v", err)
	}
	_, err = sqlDB.Exec(query)
	return err
}

// This  groups by the fact and finds groups that have more than one distinct
// combination of the other potentially-relevant columns. If any such groups
// exists it picks an arbitrary one of them and returns those distinct
// combinations so they can be shown to the user as an example.
var checkFuncDepTemplate = template.Must(
	template.New("check-groups").Funcs(template.FuncMap{"join": strings.Join}).Parse(`
	-- Figure out the values of the experiment fact that have multiple
	-- subgroups.
	WITH WithMultipleSubGroups AS (
	 	SELECT {{ .ExperimentFact }}
		FROM filtered_results
		GROUP BY {{ .ExperimentFact }}
		-- I guess you can't COUNT-DISTINCT multiple columns, so we have to
		-- squash them into a string somehow...
		HAVING COUNT(DISTINCT test_name || '-' || {{ join .OtherFacts ", " }}) > 1
		-- Just need a single example, don't care which.
		LIMIT 1
	)
	SELECT {{ .ExperimentFact }}, test_name, struct_pack({{ join .OtherFacts ", " }})
	FROM filtered_results JOIN WithMultipleSubgroups USING ({{ .ExperimentFact }})
`))

type checkFuncDepTemplateArgs struct {
	ExperimentFact string
	OtherFacts     []string
}

func (g *checkFuncDepTemplateArgs) Execute() (string, error) {
	var b bytes.Buffer
	if err := checkFuncDepTemplate.Execute(&b, g); err != nil {
		return "", err
	}
	return b.String(), nil
}

// Check that the other potentially-relevant attributes of the results in the
// database are functionally dependent on the experiment fact. This basically
// means that when you group by the experiment fact, all those other columns are
// have the same value in all entries in each group.
//
// Note that the "other potentially-relevant columns" includes the test name
// (since the exact meanings of facts and metrics are assumed to differ between
// tests) but not the result ID (since that's basically just an arbitrary
// grouping of data).
func checkFunctionalDependency(sqlDB *sql.DB, falbaDB *db.DB, experimentFact string) error {
	facts := maps.Clone(falbaDB.FactTypes)
	delete(facts, experimentFact)
	t := checkFuncDepTemplateArgs{
		ExperimentFact: experimentFact,
		OtherFacts:     slices.Collect(maps.Keys(facts)),
	}
	query, err := t.Execute()
	if err != nil {
		return fmt.Errorf("templating query: %v", err)
	}
	rows, err := sqlDB.Query(query)
	if err != nil {
		log.Printf("Failed SQL query: %v", query)
		return fmt.Errorf("executing query: %v", err)
	}
	defer rows.Close()
	processedAnyRows := false
	for rows.Next() {
		var factStr string
		var testName string
		var otherFactsStruct map[string]any
		if err := rows.Scan(&factStr, &testName, &otherFactsStruct); err != nil {
			return fmt.Errorf("scanning rows: %v", err)
		}

		// Hack to print header after we've already got the example problematic
		// fact value from the first row.
		if !processedAnyRows {
			log.Printf("Multiple subgroups for %s = %s:\n", experimentFact, factStr)
			processedAnyRows = true
		}
		log.Printf("\ttest_name = %s non-experiment facts: %s\n", testName, otherFactsStruct)
	}
	if !processedAnyRows {
		return nil
	}
	return fmt.Errorf("fact not a determinant")
}

var groupByTemplate = template.Must(template.New("group-by").Parse(`
	WITH Results AS (
		SELECT r.*, m.{{.MetricColumn}} as metric
		FROM filtered_results r
		INNER JOIN metrics m USING (result_id)
		WHERE metric = '{{.Metric}}'
	)
	SELECT
		-- All rows should have the same test name, as enforced by
		-- checkFunctionalDependency.
		ANY_VALUE(test_name),
		{{.Fact}},
		AVG(CAST(metric AS FLOAT)) AS mean,
		histogram(
			metric,
			equi_width_bins(0, (SELECT MAX(metric) FROM Results),
			65,
			nice := true)
		) AS hist,
		MIN(metric) AS min_val,
		MAX(metric) AS max_val
	FROM Results
	GROUP BY {{.Fact}}
`))

type groupByTemplateArgs struct {
	Fact         string
	Metric       string
	MetricColumn string
}

func (g *groupByTemplateArgs) Execute() (string, error) {
	var b bytes.Buffer
	if err := groupByTemplate.Execute(&b, g); err != nil {
		return "", err
	}
	return b.String(), nil
}

type HistogramBin struct {
	boundary float64 // left-open, right-closed.
	size     uint64  // Number of samples in the bin.
}

type Histogram struct {
	bins        []HistogramBin
	maxBoundary float64
	maxSize     uint64
	TotalSize   uint64
}

// Annoying boilerplate to read the duckdb.Map that gets returned when we call
// histogram() in SQL.
func (h *Histogram) Scan(v any) error {
	dm, ok := v.(duckdb.Map)
	if !ok {
		return fmt.Errorf("invalid type %T for scanning Histogram, expected duckdb.Map", v)
	}

	var bins []HistogramBin
	var maxSize uint64
	var totalSize uint64
	var maxBoundary float64
	for k, v := range dm {
		boundary, ok := k.(float64)
		if !ok {
			return fmt.Errorf("invalid type %T for histogram map key, expected float64", k)
		}
		size, ok := v.(uint64)
		if !ok {
			return fmt.Errorf("invalid type %T for histogram map value, expected uint64", v)
		}
		if bins == nil || boundary > maxBoundary {
			maxBoundary = boundary
		}
		if bins == nil || size > maxSize {
			maxSize = size
		}
		totalSize += size
		bins = append(bins, HistogramBin{boundary: boundary, size: size})
	}
	if bins == nil {
		return fmt.Errorf("empty map, expectedp histogram bins")
	}
	binLess := func(x, y HistogramBin) int {
		return cmp.Compare(x.boundary, y.boundary)
	}
	slices.SortFunc(bins, binLess)
	*h = Histogram{
		bins:        bins,
		maxBoundary: maxBoundary,
		maxSize:     maxSize,
		TotalSize: totalSize,
	}
	return nil
}

// This is the ideal plotting library. You may not like it, but this is what
// peak visualisation looks like.
//
// Return a single-line string of block element characters representing the
// distribution. Doesn't include any axis or anything, just the block elems.
// Width is equal to the number of histogram bins.
func (h *Histogram) PlotUnicode() string {
	blockElems := []rune{' ', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var b strings.Builder
	for _, bin := range h.bins {
		level := int(bin.size/h.maxSize) * (len(blockElems) - 1)
		b.WriteRune(blockElems[level])
	}
	return b.String()
}

// Group represents aggregates about metric values for some collection of
// results.
type MetricGroup struct {
	TestName string
	// Mean of the requested metric for results with the given fact value.
	// Note we're assuming the value is numeric here.
	Mean float64
	Max  float64
	Min  float64
	// Histogram where the map keys are upper-boundaries of the bins.
	Histogram Histogram
}

// Return a map of stringified fact values, to aggregates describing the value
// of the metric in results where the fact has the value from the map key. Note
// the map key should probably be a falba.Value but for now it seems like just
// squashing it into a string is harmless enough. The filterExpression is
// applied across the whole database before any analysis.
func GroupByFact(sqlDB *sql.DB, falbaDB *db.DB, experimentFact string, metric string, filterExpression string) (map[string]*MetricGroup, error) {
	if err := createFilteredResults(sqlDB, filterExpression); err != nil {
		return nil, fmt.Errorf("filtering results: %w", err)
	}

	if err := checkFunctionalDependency(sqlDB, falbaDB, experimentFact); err != nil {
		return nil, fmt.Errorf("checking functional dependency: %w", err)
	}

	metricType, ok := falbaDB.MetricTypes[metric]
	if !ok {
		return nil, fmt.Errorf("no metric %q (have: %v)", metric, slices.Collect(maps.Keys(falbaDB.MetricTypes)))
	}
	if metricType != falba.ValueInt && metricType != falba.ValueFloat {
		return nil, fmt.Errorf("sorry, only implemented for float and int metrics (%v is %v)",
			metric, metricType)
	}
	t := groupByTemplateArgs{
		Fact:         experimentFact,
		Metric:       metric,
		MetricColumn: metricType.MetricsColumn(),
	}
	query, err := t.Execute()
	if err != nil {
		return nil, fmt.Errorf("templating group-by query: %v", err)
	}
	rows, err := sqlDB.Query(query)
	if err != nil {
		log.Printf("Failed SQL query: %v", query)
		return nil, fmt.Errorf("executing group-by query: %v", err)
	}
	defer rows.Close()
	ret := make(map[string]*MetricGroup)
	for rows.Next() {
		var testName string
		// Rows.Scan stringifies stuff so for now it seems  we can get away with
		// just using string vars here. I think the next step up would be to
		// implement sql.Scanner for falba.Value.
		var factStr string
		var groupMean float64
		var groupMax float64
		var groupMin float64
		var histogram Histogram
		if err := rows.Scan(&testName, &factStr, &groupMean, &histogram, &groupMin, &groupMax); err != nil {
			return nil, fmt.Errorf("scanning group-by rows: %v", err)
		}
		ret[factStr] = &MetricGroup{
			TestName:  testName,
			Mean:      groupMean,
			Max:       groupMax,
			Min:       groupMin,
			Histogram: histogram,
		}
	}
	return ret, nil
}
