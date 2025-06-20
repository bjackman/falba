package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"syscall"

	"github.com/bjackman/falba/internal/db"
	"github.com/spf13/cobra"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	// At least one letter, followed by alphanumerics and underscores.
	sqlColumnRE = regexp.MustCompile(`[A-Za-z]+[A-Za-z0-9_]*`)

	flagDuckdbCli string

	duckDBPath string = "falba.duckdb"

	createResultsSQL = `
		CREATE OR REPLACE TABLE results
		AS SELECT * FROM read_json(?, format='array')
	`
	createMetricsSQL = `
		CREATE OR REPLACE TABLE metrics
		AS SELECT * FROM read_json(?, format='array')
	`
)

// Er, I can't really explain this function except by translating the whole code
// to English. You'll just have to read it.
func feedJSONToStmt(sqlDB *sql.DB, query string, obj any) error {
	resultsJSON, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshalling to JSON: %w", err)
	}

	f, err := os.CreateTemp("", "results.json")
	if err != nil {
		return fmt.Errorf("creating tempfile for JSON: %w", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(resultsJSON); err != nil {
		return fmt.Errorf("writing results JSON to tempfile: %w", err)
	}
	f.Close()

	stmt, err := sqlDB.Prepare(query)
	if err != nil {
		return fmt.Errorf("preparing SQL statement: %w", err)
	}
	if _, err := stmt.Exec(f.Name()); err != nil {
		return fmt.Errorf("could not create results table: %s", err.Error())
	}
	return nil
}

func createResultsTable(sqlDB *sql.DB, falbaDB *db.DB) error {
	// TODO: Must be a better way to do this than writing it to disk..
	err := feedJSONToStmt(sqlDB, createResultsSQL, falbaDB.ForResultsTable())
	if err != nil {
		return fmt.Errorf("inserting results JSON into SQL DB: %w", err)
	}
	err = feedJSONToStmt(sqlDB, createMetricsSQL, falbaDB.ForMetricsTable())
	if err != nil {
		return fmt.Errorf("inserting results JSON into SQL DB: %w", err)
	}

	return nil
}

func setupSQL() error {
	falbaDB, err := db.ReadDB(flagResultDB)
	if err != nil {
		return fmt.Errorf("opening Falba DB: %v", err)
	}

	sqlDB, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		return fmt.Errorf("couldn't open DuckDB: %v", err)
	}

	if err := createResultsTable(sqlDB, falbaDB); err != nil {
		return fmt.Errorf("creating results SQL table: %w", err)
	}

	return nil
}

// Here we don't use proper error handling because we are going to exec the
// DuckDB CLI so defer etc won't work.
func cmdSQL(cmd *cobra.Command, args []string) {
	if err := setupSQL(); err != nil {
		log.Fatalf("Setting up SQL DB: %v", err)
	}

	// Apparently yhe 'exec' package doesn't actually support exec-ing lol.
	// I got this from https://gobyexample.com/execing-processes
	cliPath, err := exec.LookPath(flagDuckdbCli)
	if err != nil {
		log.Fatalf("Searching $PATH for DuckDB CLI (%q, from --duckdb-cli): %v", flagDuckdbCli, err)
	}
	err = syscall.Exec(cliPath, []string{cliPath, duckDBPath}, os.Environ())
	if err != nil {
		log.Fatalf("exec()ing DuckDB CLI: %v", err)
	}
	// wat
	log.Fatalf("Unexpectedly returned from exec()ing DuckDB CLI")
}

// sqlCmd represents the sql command
var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Drop into a DuckDB SQL prompt.",
	Long: `Creates a DuckDB database and then uses the DuckDB CLI
(https://duckdb.org/docs/stable/clients/cli/overview.html) to drop you into
a SQL REPL where you can explore the Falba data.`,
	Run: cmdSQL,
}

func init() {
	sqlCmd.Flags().StringVar(&flagDuckdbCli, "duckdb-cli", "duckdb",
		"DuckDB CLI executable. Looked up in $PATH")
	rootCmd.AddCommand(sqlCmd)
}
