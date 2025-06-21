package cmd

import (
	"log"
	"os"
	"os/exec"
	"syscall"

	"github.com/spf13/cobra"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	flagDuckdbCli string
)

// Here we don't use proper error handling because we are going to exec the
// DuckDB CLI so defer etc won't work.
func cmdSQL(cmd *cobra.Command, args []string) {
	if _, err := setupSQL(); err != nil {
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
