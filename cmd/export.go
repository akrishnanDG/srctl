package cmd

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	exportOutput         string
	exportArchive        string
	exportWithRefs       bool
	exportVersions       string
	exportFilter         string
	exportIncludeDeleted bool
	exportWorkers        int
)

// schemaExport represents a schema to be exported
type schemaExport struct {
	Subject    string
	Version    int
	SchemaID   int
	SchemaType string
	Schema     string
	References []client.SchemaReference
}

var exportCmd = &cobra.Command{
	Use:     "export",
	Short:   "Export schemas to files or archive",
	GroupID: groupBulk,
	Long: `Export schemas from Schema Registry to local files or archive.

Exports schemas organized in directory structure:
  <output>/
    <context>/
      <subject>/
        v<version>.<ext>
        metadata.json

Archive formats:
  • tar.gz (default)
  • zip

Examples:
  # Export all schemas to directory
  srctl export --output ./schemas

  # Export to tar.gz archive
  srctl export --output schemas.tar.gz --archive tar

  # Export to zip archive
  srctl export --output schemas.zip --archive zip

  # Export specific subjects
  srctl export --filter "user-*" --output ./schemas

  # Export with all referenced schemas
  srctl export --with-refs --output ./schemas

  # Export only latest versions
  srctl export --versions latest --output ./schemas

  # Export from specific context
  srctl export --context .mycontext --output ./schemas
  
  # Control parallelism
  srctl export --output ./schemas --workers 50`,
	RunE: runExport,
}

func init() {
	exportCmd.Flags().StringVarP(&exportOutput, "output", "o", "./schema-export", "Output path (directory or archive file)")
	exportCmd.Flags().StringVar(&exportArchive, "archive", "", "Archive format: tar, zip (empty for directory)")
	exportCmd.Flags().BoolVar(&exportWithRefs, "with-refs", false, "Include all referenced schemas")
	exportCmd.Flags().StringVar(&exportVersions, "versions", "all", "Versions to export: all, latest, or comma-separated list")
	exportCmd.Flags().StringVarP(&exportFilter, "filter", "f", "", "Filter subjects by pattern")
	exportCmd.Flags().BoolVar(&exportIncludeDeleted, "deleted", false, "Include soft-deleted subjects")
	exportCmd.Flags().IntVar(&exportWorkers, "workers", 20, "Number of parallel workers for fetching schemas")

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Exporting Schemas")

	// Get subjects
	output.Step("Fetching subjects...")
	subjects, err := c.GetSubjects(exportIncludeDeleted)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	// Apply filter
	if exportFilter != "" {
		subjects = filterSubjects(subjects, exportFilter)
	}

	if len(subjects) == 0 {
		output.Warning("No subjects to export")
		return nil
	}

	output.Info("Found %d subjects to export", len(subjects))

	// Collect schemas using parallel workers
	output.Step("Collecting schemas with %d workers...", exportWorkers)
	schemas := collectExportSchemasParallel(c, subjects, exportWorkers, exportVersions, exportIncludeDeleted)

	// Collect referenced schemas if requested
	if exportWithRefs {
		referencesNeeded := make(map[string]bool)
		for _, s := range schemas {
			for _, ref := range s.References {
				key := fmt.Sprintf("%s:%d", ref.Subject, ref.Version)
				referencesNeeded[key] = true
			}
		}

		if len(referencesNeeded) > 0 {
			output.Step("Fetching %d referenced schemas...", len(referencesNeeded))
			for key := range referencesNeeded {
				parts := strings.Split(key, ":")
				if len(parts) != 2 {
					continue
				}
				subj := parts[0]
				ver := parts[1]

				// Check if already have it
				found := false
				for _, s := range schemas {
					if s.Subject == subj && strconv.Itoa(s.Version) == ver {
						found = true
						break
					}
				}
				if found {
					continue
				}

				schema, err := c.GetSchema(subj, ver)
				if err != nil {
					output.Warning("Could not fetch reference %s: %v", key, err)
					continue
				}

				schemaType := schema.SchemaType
				if schemaType == "" {
					schemaType = "AVRO"
				}

				version, _ := strconv.Atoi(ver)
				schemas = append(schemas, schemaExport{
					Subject:    subj,
					Version:    version,
					SchemaID:   schema.ID,
					SchemaType: schemaType,
					Schema:     schema.Schema,
					References: schema.References,
				})
			}
		}
	}

	output.Info("Collected %d schema versions", len(schemas))

	// Export based on archive type
	switch exportArchive {
	case "tar", "tar.gz", "tgz":
		return exportToTar(schemas, exportOutput)
	case "zip":
		return exportToZip(schemas, exportOutput)
	default:
		return exportToDirectory(schemas, exportOutput)
	}
}

// collectExportSchemasParallel fetches schemas from multiple subjects in parallel for export
func collectExportSchemasParallel(c *client.SchemaRegistryClient, subjects []string, numWorkers int, versionsFilter string, includeDeleted bool) []schemaExport {
	type job struct {
		subject string
	}

	jobs := make(chan job, len(subjects))
	results := make(chan []schemaExport, len(subjects))

	// Progress bar
	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Fetching"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				subjectSchemas := fetchSubjectSchemas(c, j.subject, versionsFilter, includeDeleted)
				results <- subjectSchemas
				bar.Add(1)
			}
		}()
	}

	// Send jobs
	go func() {
		for _, subj := range subjects {
			jobs <- job{subject: subj}
		}
		close(jobs)
	}()

	// Wait and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect all results
	var allSchemas []schemaExport
	for schemas := range results {
		allSchemas = append(allSchemas, schemas...)
	}

	bar.Finish()
	return allSchemas
}

// fetchSubjectSchemas fetches all schema versions for a subject
func fetchSubjectSchemas(c *client.SchemaRegistryClient, subject string, versionsFilter string, includeDeleted bool) []schemaExport {
	var schemas []schemaExport

	versions, err := c.GetVersions(subject, includeDeleted)
	if err != nil {
		return schemas
	}

	// Filter versions
	versionsToExport := filterVersions(versions, versionsFilter)

	for _, v := range versionsToExport {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			// Retry with deleted=true for soft-deleted schemas
			schema, err = c.GetSchemaWithDeleted(subject, strconv.Itoa(v), true)
			if err != nil {
				continue
			}
		}

		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}

		schemas = append(schemas, schemaExport{
			Subject:    subject,
			Version:    v,
			SchemaID:   schema.ID,
			SchemaType: schemaType,
			Schema:     schema.Schema,
			References: schema.References,
		})
	}

	return schemas
}

func filterVersions(versions []int, filter string) []int {
	switch filter {
	case "latest":
		if len(versions) > 0 {
			return []int{versions[len(versions)-1]}
		}
		return nil
	case "all", "":
		return versions
	default:
		// Parse comma-separated list
		var result []int
		for _, v := range strings.Split(filter, ",") {
			v = strings.TrimSpace(v)
			if num, err := strconv.Atoi(v); err == nil {
				for _, ver := range versions {
					if ver == num {
						result = append(result, num)
						break
					}
				}
			}
		}
		return result
	}
}

func exportToDirectory(schemas []schemaExport, outputPath string) error {
	output.Step("Writing to directory: %s", outputPath)

	bar := progressbar.NewOptions(len(schemas),
		progressbar.OptionSetDescription("Exporting"),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	ctx := context
	if ctx == "" {
		ctx = "default"
	}

	for _, s := range schemas {
		// Create directory structure
		dir := filepath.Join(outputPath, ctx, s.Subject)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Determine file extension
		ext := getSchemaExtension(s.SchemaType)

		// Write schema file
		schemaPath := filepath.Join(dir, fmt.Sprintf("v%d.%s", s.Version, ext))
		schemaContent := s.Schema
		if s.SchemaType == "AVRO" {
			// Pretty print JSON
			var parsed interface{}
			if json.Unmarshal([]byte(s.Schema), &parsed) == nil {
				if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
					schemaContent = string(pretty)
				}
			}
		}
		if err := os.WriteFile(schemaPath, []byte(schemaContent), 0644); err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}

		// Write metadata
		metadata := map[string]interface{}{
			"subject":    s.Subject,
			"version":    s.Version,
			"schemaId":   s.SchemaID,
			"schemaType": s.SchemaType,
			"exportedAt": time.Now().UTC().Format(time.RFC3339),
		}
		if len(s.References) > 0 {
			metadata["references"] = s.References
		}

		metadataPath := filepath.Join(dir, fmt.Sprintf("v%d.metadata.json", s.Version))
		metadataBytes, _ := json.MarshalIndent(metadata, "", "  ")
		os.WriteFile(metadataPath, metadataBytes, 0644)

		bar.Add(1)
	}
	bar.Finish()

	output.Success("Exported %d schemas to %s", len(schemas), outputPath)
	return nil
}

func exportToTar(schemas []schemaExport, outputPath string) error {
	if !strings.HasSuffix(outputPath, ".tar.gz") && !strings.HasSuffix(outputPath, ".tgz") {
		outputPath += ".tar.gz"
	}

	output.Step("Creating tar archive: %s", outputPath)

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	bar := progressbar.NewOptions(len(schemas),
		progressbar.OptionSetDescription("Archiving"),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	ctx := context
	if ctx == "" {
		ctx = "default"
	}

	for _, s := range schemas {
		ext := getSchemaExtension(s.SchemaType)

		// Schema file
		schemaPath := filepath.Join(ctx, s.Subject, fmt.Sprintf("v%d.%s", s.Version, ext))
		schemaContent := s.Schema
		if s.SchemaType == "AVRO" {
			var parsed interface{}
			if json.Unmarshal([]byte(s.Schema), &parsed) == nil {
				if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
					schemaContent = string(pretty)
				}
			}
		}

		if err := addToTar(tarWriter, schemaPath, []byte(schemaContent)); err != nil {
			return err
		}

		// Metadata
		metadata := map[string]interface{}{
			"subject":    s.Subject,
			"version":    s.Version,
			"schemaId":   s.SchemaID,
			"schemaType": s.SchemaType,
			"exportedAt": time.Now().UTC().Format(time.RFC3339),
		}
		if len(s.References) > 0 {
			metadata["references"] = s.References
		}
		metadataBytes, _ := json.MarshalIndent(metadata, "", "  ")
		metadataPath := filepath.Join(ctx, s.Subject, fmt.Sprintf("v%d.metadata.json", s.Version))
		if err := addToTar(tarWriter, metadataPath, metadataBytes); err != nil {
			return err
		}

		bar.Add(1)
	}
	bar.Finish()

	output.Success("Created archive with %d schemas: %s", len(schemas), outputPath)
	return nil
}

func addToTar(tw *tar.Writer, name string, content []byte) error {
	header := &tar.Header{
		Name:    name,
		Size:    int64(len(content)),
		Mode:    0644,
		ModTime: time.Now(),
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	_, err := tw.Write(content)
	return err
}

func exportToZip(schemas []schemaExport, outputPath string) error {
	if !strings.HasSuffix(outputPath, ".zip") {
		outputPath += ".zip"
	}

	output.Step("Creating zip archive: %s", outputPath)

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer file.Close()

	zipWriter := zip.NewWriter(file)
	defer zipWriter.Close()

	bar := progressbar.NewOptions(len(schemas),
		progressbar.OptionSetDescription("Archiving"),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	ctx := context
	if ctx == "" {
		ctx = "default"
	}

	for _, s := range schemas {
		ext := getSchemaExtension(s.SchemaType)

		// Schema file
		schemaPath := filepath.Join(ctx, s.Subject, fmt.Sprintf("v%d.%s", s.Version, ext))
		schemaContent := s.Schema
		if s.SchemaType == "AVRO" {
			var parsed interface{}
			if json.Unmarshal([]byte(s.Schema), &parsed) == nil {
				if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
					schemaContent = string(pretty)
				}
			}
		}

		w, err := zipWriter.Create(schemaPath)
		if err != nil {
			return err
		}
		if _, err := w.Write([]byte(schemaContent)); err != nil {
			return err
		}

		// Metadata
		metadata := map[string]interface{}{
			"subject":    s.Subject,
			"version":    s.Version,
			"schemaId":   s.SchemaID,
			"schemaType": s.SchemaType,
			"exportedAt": time.Now().UTC().Format(time.RFC3339),
		}
		if len(s.References) > 0 {
			metadata["references"] = s.References
		}
		metadataBytes, _ := json.MarshalIndent(metadata, "", "  ")
		metadataPath := filepath.Join(ctx, s.Subject, fmt.Sprintf("v%d.metadata.json", s.Version))

		w, err = zipWriter.Create(metadataPath)
		if err != nil {
			return err
		}
		if _, err := w.Write(metadataBytes); err != nil {
			return err
		}

		bar.Add(1)
	}
	bar.Finish()

	output.Success("Created archive with %d schemas: %s", len(schemas), outputPath)
	return nil
}

func getSchemaExtension(schemaType string) string {
	switch strings.ToUpper(schemaType) {
	case "PROTOBUF":
		return "proto"
	case "JSON":
		return "json"
	default:
		return "avsc"
	}
}
