package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"text/template"

	"github.com/bjackman/falba/internal/db"
	"github.com/spf13/cobra"
)

var (
	cmpFlagMetric string
	cmpFlagFact   string
	cmpFlagFilter string
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

func createFilteredResults(sqlDB *sql.DB) error {
	t := filterResultsTemplateArgs{
		FilterExpression: cmpFlagFilter,
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
	SELECT {{.Fact}}, AVG(CAST({{.MetricColumn}} AS FLOAT)) as mean
	FROM filtered_results
	INNER JOIN metrics USING (result_id)
	WHERE metric = '{{.Metric}}' GROUP BY {{.Fact}}
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

// Group represents aggregates about metric values for some collection of
// results.
type MetricGroup struct {
	// Mean of the requested metric for results with the given fact value.
	// Note we're assuming the value is numeric here.
	Mean float64
}

// Return a map of stringified fact values, to aggregates describing the value
// of the metric in results where the fact has the value from the map key. Note
// the map key should probably be a falba.Value but for now it seems like just
// squashing it into a string is harmless enough.
func groupByFact(sqlDB *sql.DB, falbaDB *db.DB, experimentFact string, metric string) (map[string]*MetricGroup, error) {
	metricType, ok := falbaDB.MetricTypes[cmpFlagMetric]
	if !ok {
		return nil, fmt.Errorf("no metric %q (have: %v)", cmpFlagMetric, slices.Collect(maps.Keys(falbaDB.MetricTypes)))
	}
	t := groupByTemplateArgs{
		Fact:         cmpFlagFact,
		Metric:       cmpFlagMetric,
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
		// Rows.Scan stringifies stuff so for now it seems  we can get away with
		// just using string vars here. I think the next step up would be to
		// implement sql.Scanner for falba.Value.
		var factStr string
		var groupMean float64
		if err := rows.Scan(&factStr, &groupMean); err != nil {
			return nil, fmt.Errorf("scanning group-by rows: %v", err)
		}
		ret[factStr] = &MetricGroup{Mean: groupMean}
	}
	return ret, nil
}

func cmdCmp(cmd *cobra.Command, args []string) error {
	falbaDB, sqlDB, err := setupSQL()
	if err != nil {
		log.Fatalf("Setting up SQL DB: %v", err)
	}

	// Just to produce a nice error message, check the fact exists.
	_, ok := falbaDB.FactTypes[cmpFlagFact]
	if !ok {
		return fmt.Errorf("no fact %q (have: %v)", cmpFlagMetric, slices.Collect(maps.Keys(falbaDB.FactTypes)))
	}

	if err := createFilteredResults(sqlDB); err != nil {
		return fmt.Errorf("filtering results: %w", err)
	}

	if err := checkFunctionalDependency(sqlDB, falbaDB, cmpFlagFact); err != nil {
		return fmt.Errorf("checking functional dependency: %w", err)
	}

	groups, err := groupByFact(sqlDB, falbaDB, cmpFlagFact, cmpFlagMetric)
	if err != nil {
		return fmt.Errorf("grouping by fact: %v", err)
	}
	for factVal, group := range groups {
		log.Printf("%s = %s: μ = %f", cmpFlagFact, factVal, group.Mean)
	}

	return nil
}

var cmpCmd = &cobra.Command{
	Use:   "cmp",
	Short: "Compare distributions of grouped metrics",
	RunE:  cmdCmp,
}

func init() {
	rootCmd.AddCommand(cmpCmd)

	cmpCmd.Flags().StringVarP(&cmpFlagMetric, "metric", "m", "", "Metric to compare")
	cmpCmd.MarkFlagRequired("metric")
	cmpCmd.Flags().StringVarP(&cmpFlagFact, "fact", "f", "", "Fact to group by")
	cmpCmd.MarkFlagRequired("fact")
	cmpCmd.Flags().StringVarP(&cmpFlagFilter, "filter", "w", "TRUE", "Filter for results. SQL boolean expression.")
}
