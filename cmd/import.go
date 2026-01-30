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

	// Helper to walk through the files. This implements the logic where we
	// treat individial files individually (copying them straight to the root of
	// the artifacts dir), and directories as a special group (maintaining their
	// structure inside the artifacts tree).
	type artifactEntry struct {
		// Where the file currently is.
		currentPath string
		// Where it needs to go, relative to artifacts/
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
					parentDir := filepath.Dir(filepath.Clean(inputPath))
					relPath, err := filepath.Rel(parentDir, path)
					if err != nil {
						return fmt.Errorf("failed to get relative path for %s: %w", path, err)
					}
					artifactsToProcess = append(artifactsToProcess, artifactEntry{
						currentPath:  path,
						relativePath: relPath,
					})
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

	// Calculate result ID.
	hash := sha256.New()
	for _, entry := range artifactsToProcess {
		f, err := os.Open(entry.currentPath)
		if err != nil {
			return fmt.Errorf("failed to open artifact %s for hashing: %w", entry.currentPath, err)
		}
		defer f.Close()

		fileHash := sha256.New()
		if _, err := io.Copy(fileHash, f); err != nil {
			return fmt.Errorf("failed to hash content of %s: %w", entry.currentPath, err)
		}
		hash.Write(fileHash.Sum(nil))
	}
	hashStr := hex.EncodeToString(hash.Sum(nil))[:12]

	resultDir := filepath.Join(flagResultDB, fmt.Sprintf("%s:%s", importFlagTestName, hashStr))

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
	Args: cobra.MinimumNArgs(1),
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().StringVarP(&importFlagTestName, "test-name", "t", "", "Name of the test")
	importCmd.MarkFlagRequired("test-name")
}
