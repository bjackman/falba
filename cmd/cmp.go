package cmd

import (
	"fmt"
	"log"
	"maps"
	"os"
	"slices"

	"github.com/bjackman/falba/internal/anal"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

var (
	cmpFlagMetric string
	cmpFlagFact   string
	cmpFlagFilter string
)

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

	groups, err := anal.GroupByFact(sqlDB, falbaDB, cmpFlagFact, cmpFlagMetric, cmpFlagFilter)
	if err != nil {
		return fmt.Errorf("grouping by fact: %v", err)
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{cmpFlagFact, "mean", "min", "histogram", "max"})
	for factVal, group := range groups {
		t.AppendRow(table.Row{factVal, group.Mean, group.Min, group.Histogram.PlotUnicode(), group.Max})
	}
	t.SetStyle(table.Style{
		Name: "mystyle",
		Box:  table.StyleBoxLight,
		// Needs to be set explicitly for some reason, otherwise the table
		// boxes don't show up.
		Options: table.OptionsDefault,
		Format: table.FormatOptions{
			Header: text.FormatDefault,
			Row:    text.FormatDefault,
		},
	})
	t.SortBy([]table.SortBy{{Name: cmpFlagFact, Mode: table.Asc}})
	t.Render()

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
