package cmd

import (
	"bytes"
	"fmt"
	"log"
	"maps"
	"slices"
	"text/template"

	"github.com/spf13/cobra"
)

var (
	cmpFlagMetric string
	cmpFlagFact   string
)

var groupByTemplate = template.Must(template.New("group-by").Parse(`
	SELECT {{.Fact}}, AVG(CAST({{.MetricColumn}} AS FLOAT)) as mean
	FROM results
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

func cmdCmp(cmd *cobra.Command, args []string) error {
	falbaDB, sqlDB, err := setupSQL()
	if err != nil {
		log.Fatalf("Setting up SQL DB: %v", err)
	}

	metricType, ok := falbaDB.MetricTypes[cmpFlagMetric]
	if !ok {
		return fmt.Errorf("no metric %q (have: %v)", cmpFlagMetric, slices.Collect(maps.Keys(falbaDB.MetricTypes)))
	}
	// We don't care about the type but check the fact is legit too, just to
	// avoid confusion.
	_, ok = falbaDB.FactTypes[cmpFlagFact]
	if !ok {
		return fmt.Errorf("no fact %q (have: %v)", cmpFlagMetric, slices.Collect(maps.Keys(falbaDB.FactTypes)))
	}

	// TODO: This should detect if the groups are 'correct', i.e. if there are
	// internal differences in any facts, or test_name. Once that's done we will
	// need to give the user more control in order to whittle down the results.

	// Prepared statements aren't flexible enough so we are just gonna be
	// vulnerable to SQL injection here. The rationale is that since we have
	// done some minimal validation it's hopefully pretty unlikely to do this by
	// accident and get confused.
	t := groupByTemplateArgs{
		Fact:         cmpFlagFact,
		Metric:       cmpFlagMetric,
		MetricColumn: metricType.MetricsColumn(),
	}
	query, err := t.Execute()
	if err != nil {
		return fmt.Errorf("templating group-by query: %v", err)
	}
	rows, err := sqlDB.Query(query)
	if err != nil {
		log.Printf("Failed SQL query: %v", query)
		return fmt.Errorf("executing group-by query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		// Rows.Scan stringifies stuff so for now it seems  we can get away with
		// just using string vars here. I think the next step up would be to
		// implement sql.Scanner for falba.Value.
		var factStr string
		var groupMean float64
		if err := rows.Scan(&factStr, &groupMean); err != nil {
			return fmt.Errorf("scanning group-by rows: %v", err)
		}
		log.Printf("%s: %s - mean %s = %f", cmpFlagFact, factStr, cmpFlagMetric, groupMean)
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
}
