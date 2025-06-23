package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	importFlagTestName string
)

func importCmdRunE(cmd *cobra.Command, args []string) error {
	artifactPaths := args
	if len(artifactPaths) == 0 {
		return fmt.Errorf("at least one artifact path must be provided")
	}

	if importFlagTestName == "" {
		return fmt.Errorf("--test-name is required")
	}

	// Helper to walk through the files
	// Yields tuples of (current path of file, eventual path of file relative to artifacts/)
	type artifactEntry struct {
		currentPath string
		relativePath string
	}
	var artifactsToProcess []artifactEntry

	for _, inputPath := range artifactPaths {
		info, err := os.Stat(inputPath)
		if err != nil {
			return fmt.Errorf("failed to stat artifact path %s: %w", inputPath, err)
		}

		if info.IsDir() {
			err := filepath.WalkDir(inputPath, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() {
					relPath, err := filepath.Rel(inputPath, path)
					if err != nil {
						return fmt.Errorf("failed to get relative path for %s: %w", path, err)
					}
					artifactsToProcess = append(artifactsToProcess, artifactEntry{currentPath: path, relativePath: relPath})
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to walk directory %s: %w", inputPath, err)
			}
		} else {
			artifactsToProcess = append(artifactsToProcess, artifactEntry{currentPath: inputPath, relativePath: filepath.Base(inputPath)})
		}
	}

	// Figure out the result ID by hashing the artifacts.
	hash := sha256.New()
	for _, entry := range artifactsToProcess {
		f, err := os.Open(entry.currentPath)
		if err != nil {
			return fmt.Errorf("failed to open artifact %s for hashing: %w", entry.currentPath, err)
		}
		defer f.Close()

		// Hash the file content
		fileHash := sha256.New()
		if _, err := io.Copy(fileHash, f); err != nil {
			return fmt.Errorf("failed to hash content of %s: %w", entry.currentPath, err)
		}
		hash.Write(fileHash.Sum(nil))
	}
	hashStr := hex.EncodeToString(hash.Sum(nil))[:12]

	// Copy the artifacts into the database.
	// Ensure flagResultDB is available (from rootCmd)
	if flagResultDB == "" {
		return fmt.Errorf("path to Falba DB root (--result-db) not set")
	}
	resultDir := filepath.Join(flagResultDB, fmt.Sprintf("%s:%s", importFlagTestName, hashStr))
	
	// This must fail if the directory already exists.
	err := os.Mkdir(resultDir, 0755)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("result directory %s already exists", resultDir)
		}
		return fmt.Errorf("failed to create result directory %s: %w", resultDir, err)
	}

	artifactsDir := filepath.Join(resultDir, "artifacts")
	numCopied := 0
	for _, entry := range artifactsToProcess {
		destPath := filepath.Join(artifactsDir, entry.relativePath)
		
		err := os.MkdirAll(filepath.Dir(destPath), 0755)
		if err != nil {
			return fmt.Errorf("failed to create parent directory for %s: %w", destPath, err)
		}

		sourceFile, err := os.Open(entry.currentPath)
		if err != nil {
			return fmt.Errorf("failed to open source artifact %s: %w", entry.currentPath, err)
		}
		defer sourceFile.Close()

		destFile, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("failed to create destination artifact %s: %w", destPath, err)
		}
		defer destFile.Close()

		_, err = io.Copy(destFile, sourceFile)
		if err != nil {
			return fmt.Errorf("failed to copy artifact from %s to %s: %w", entry.currentPath, destPath, err)
		}
		numCopied++
	}

	log.Printf("Imported %d artifacts to %s", numCopied, resultDir)
	return nil
}

var importCmd = &cobra.Command{
	Use:   "import [flags] artifact_path [artifact_path...]",
	Short: "Import a new result into the database.",
	Long: `Add a result to the database. Update the db in memory too.

Files specified directly are added by name to the root of the artifacts
tree. Directories are copied recursively, preserving their structure.`,
	RunE: importCmdRunE,
	Args: cobra.MinimumNArgs(1), // Ensure at least one artifact path is provided
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().StringVarP(&importFlagTestName, "test-name", "t", "", "Name of the test")
	// No need to mark as required here, RunE checks for it. Or use MarkFlagRequired.
}
