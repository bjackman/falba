package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/bjackman/falba/internal/db"
	"github.com/spf13/cobra"
)

var (
	flagResultDB string
	duckDBPath   string = "falba.duckdb"
)

func setupSQL() (*sql.DB, error) {
	falbaDB, err := db.ReadDB(flagResultDB)
	if err != nil {
		return nil, fmt.Errorf("opening Falba DB: %v", err)
	}

	sqlDB, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't open DuckDB: %v", err)
	}

	if err := falbaDB.InsertIntoDuckDB(sqlDB); err != nil {
		return nil, fmt.Errorf("creating results SQL table: %w", err)
	}

	return sqlDB, nil
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "falba",
	Short: "Fully Automated Luxury Benchmark Analysis",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// "Persistent" means flags that are inherited by subcommands. Persistent
	// flags on the root command are global flags.
	rootCmd.PersistentFlags().StringVar(&flagResultDB, "result-db", "", "Path to Falba DB root")
}
