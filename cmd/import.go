package cmd

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	importDryRun        bool
	importSkipExisting  bool
	importCompatibility string
	importTargetContext string
)

var importCmd = &cobra.Command{
	Use:     "import <path>",
	Short:   "Import schemas from files or archive",
	GroupID: groupBulk,
	Long: `Import schemas from a directory structure or archive into Schema Registry.

Expected directory structure:
  <path>/
    <context>/
      <subject>/
        v<version>.avsc (or .proto, .json)
        v<version>.metadata.json (optional)

Archives supported:
  • tar.gz / tgz
  • zip

Import resolves dependencies automatically by ordering schemas with references.

Examples:
  # Import from directory
  srctl import ./schemas

  # Import from archive
  srctl import schemas.tar.gz

  # Dry run - validate without importing
  srctl import ./schemas --dry-run

  # Skip existing subjects
  srctl import ./schemas --skip-existing

  # Import into specific context
  srctl import ./schemas --target-context .production

  # Set compatibility for imported schemas
  srctl import ./schemas --compatibility BACKWARD`,
	Args: cobra.ExactArgs(1),
	RunE: runImport,
}

func init() {
	importCmd.Flags().BoolVar(&importDryRun, "dry-run", false, "Validate import without registering")
	importCmd.Flags().BoolVar(&importSkipExisting, "skip-existing", false, "Skip subjects that already exist")
	importCmd.Flags().StringVar(&importCompatibility, "compatibility", "", "Set compatibility for imported schemas")
	importCmd.Flags().StringVar(&importTargetContext, "target-context", "", "Import into specific context")

	rootCmd.AddCommand(importCmd)
}

type schemaToImport struct {
	Subject    string
	Version    int
	SchemaType string
	Schema     string
	References []client.SchemaReference
	FilePath   string
}

func runImport(cmd *cobra.Command, args []string) error {
	sourcePath := args[0]

	output.Header("Importing Schemas")
	output.Info("Source: %s", sourcePath)

	// Detect source type and read schemas
	var schemas []schemaToImport
	var err error

	stat, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to access source: %w", err)
	}

	if stat.IsDir() {
		schemas, err = readFromDirectory(sourcePath)
	} else if strings.HasSuffix(sourcePath, ".tar.gz") || strings.HasSuffix(sourcePath, ".tgz") {
		schemas, err = readFromTar(sourcePath)
	} else if strings.HasSuffix(sourcePath, ".zip") {
		schemas, err = readFromZip(sourcePath)
	} else {
		return fmt.Errorf("unsupported source format: %s", sourcePath)
	}

	if err != nil {
		return fmt.Errorf("failed to read schemas: %w", err)
	}

	if len(schemas) == 0 {
		output.Warning("No schemas found to import")
		return nil
	}

	output.Info("Found %d schema versions to import", len(schemas))

	// Rewrite subject names if target context is specified
	if importTargetContext != "" {
		rewriteSubjectContexts(schemas, importTargetContext)
		output.Info("Target context: %s (rewriting subject names)", importTargetContext)
	}

	// Sort schemas to handle dependencies (schemas without references first)
	sortSchemasByDependencies(schemas)

	// Get client
	c, err := GetClient()
	if err != nil {
		return err
	}

	// Get existing subjects for skip-existing check
	var existingSubjects map[string]bool
	if importSkipExisting {
		existing, err := c.GetSubjects(false)
		if err == nil {
			existingSubjects = make(map[string]bool)
			for _, s := range existing {
				existingSubjects[s] = true
			}
		}
	}

	// Dry run mode
	if importDryRun {
		return dryRunImport(c, schemas, existingSubjects)
	}

	// Perform import
	return performImport(c, schemas, existingSubjects)
}

func readFromDirectory(rootPath string) ([]schemaToImport, error) {
	var schemas []schemaToImport

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Check if it's a schema file
		ext := filepath.Ext(path)
		if ext != ".avsc" && ext != ".proto" && ext != ".json" {
			return nil
		}

		// Skip metadata files
		if strings.Contains(path, ".metadata.") {
			return nil
		}

		schema, err := parseSchemaFile(rootPath, path)
		if err != nil {
			output.Warning("Skipping %s: %v", path, err)
			return nil
		}

		schemas = append(schemas, schema)
		return nil
	})

	return schemas, err
}

func parseSchemaFile(rootPath, filePath string) (schemaToImport, error) {
	var schema schemaToImport
	schema.FilePath = filePath

	// Read schema content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return schema, err
	}
	schema.Schema = string(content)

	// Determine schema type from extension
	ext := filepath.Ext(filePath)
	switch ext {
	case ".proto":
		schema.SchemaType = "PROTOBUF"
	case ".json":
		// Check if JSON Schema or Avro
		if strings.Contains(schema.Schema, `"$schema"`) {
			schema.SchemaType = "JSON"
		} else {
			schema.SchemaType = "AVRO"
		}
	default:
		schema.SchemaType = "AVRO"
	}

	// Parse path to extract subject and version
	relPath, _ := filepath.Rel(rootPath, filePath)
	parts := strings.Split(relPath, string(filepath.Separator))

	// Expected: context/subject/v<version>.ext
	if len(parts) >= 2 {
		schema.Subject = parts[len(parts)-2]

		// Parse version from filename
		filename := parts[len(parts)-1]
		filename = strings.TrimSuffix(filename, ext)
		if strings.HasPrefix(filename, "v") {
			if v, err := strconv.Atoi(filename[1:]); err == nil {
				schema.Version = v
			}
		}
	}

	// Try to read metadata file
	metadataPath := strings.TrimSuffix(filePath, ext) + ".metadata.json"
	if metadataContent, err := os.ReadFile(metadataPath); err == nil {
		var metadata struct {
			Subject    string                   `json:"subject"`
			Version    int                      `json:"version"`
			SchemaType string                   `json:"schemaType"`
			References []client.SchemaReference `json:"references"`
		}
		if json.Unmarshal(metadataContent, &metadata) == nil {
			if metadata.Subject != "" {
				schema.Subject = metadata.Subject
			}
			if metadata.Version > 0 {
				schema.Version = metadata.Version
			}
			if metadata.SchemaType != "" {
				schema.SchemaType = metadata.SchemaType
			}
			schema.References = metadata.References
		}
	}

	if schema.Subject == "" {
		return schema, fmt.Errorf("could not determine subject name")
	}

	return schema, nil
}

func readFromTar(tarPath string) ([]schemaToImport, error) {
	var schemas []schemaToImport

	file, err := os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)

	schemaFiles := make(map[string]string)   // path -> content
	metadataFiles := make(map[string]string) // path -> content

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		content, err := io.ReadAll(tarReader)
		if err != nil {
			return nil, err
		}

		if strings.Contains(header.Name, ".metadata.") {
			metadataFiles[header.Name] = string(content)
		} else if hasSchemaExtension(header.Name) {
			schemaFiles[header.Name] = string(content)
		}
	}

	// Parse schema files
	for path, content := range schemaFiles {
		schema, err := parseSchemaFromArchive(path, content, metadataFiles)
		if err != nil {
			output.Warning("Skipping %s: %v", path, err)
			continue
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func readFromZip(zipPath string) ([]schemaToImport, error) {
	var schemas []schemaToImport

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	schemaFiles := make(map[string]string)
	metadataFiles := make(map[string]string)

	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			continue
		}

		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}

		if strings.Contains(file.Name, ".metadata.") {
			metadataFiles[file.Name] = string(content)
		} else if hasSchemaExtension(file.Name) {
			schemaFiles[file.Name] = string(content)
		}
	}

	for path, content := range schemaFiles {
		schema, err := parseSchemaFromArchive(path, content, metadataFiles)
		if err != nil {
			output.Warning("Skipping %s: %v", path, err)
			continue
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func hasSchemaExtension(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".avsc" || ext == ".proto" || ext == ".json"
}

// rewriteSubjectContexts rewrites subject names and references to use a new context
func rewriteSubjectContexts(schemas []schemaToImport, targetContext string) {
	// Build mapping from old subject names to new subject names
	rewriteMap := make(map[string]string)

	for i := range schemas {
		oldSubject := schemas[i].Subject
		newSubject := rewriteSubjectContext(oldSubject, targetContext)
		rewriteMap[oldSubject] = newSubject
		schemas[i].Subject = newSubject
	}

	// Rewrite references to use new subject names
	for i := range schemas {
		for j := range schemas[i].References {
			oldRefSubject := schemas[i].References[j].Subject
			if newRefSubject, ok := rewriteMap[oldRefSubject]; ok {
				schemas[i].References[j].Subject = newRefSubject
			} else {
				// Reference to a subject not in our import set - try to rewrite anyway
				schemas[i].References[j].Subject = rewriteSubjectContext(oldRefSubject, targetContext)
			}
		}
	}
}

// rewriteSubjectContext rewrites a single subject name to use a new context
func rewriteSubjectContext(subject string, targetContext string) string {
	// Subject formats:
	// - "subject-name" (no context, default context)
	// - ":.context:subject-name" (context-prefixed)

	baseName := subject

	// Check if subject has a context prefix (:.context:name format)
	if strings.HasPrefix(subject, ":.") {
		// Find the second colon after the context
		afterFirstColon := subject[2:] // Skip ":."
		colonIdx := strings.Index(afterFirstColon, ":")
		if colonIdx != -1 {
			baseName = afterFirstColon[colonIdx+1:] // Extract name after context
		}
	}

	// Create new subject name with target context
	if targetContext == "" || targetContext == "." {
		return baseName
	}

	// Ensure context starts with .
	ctx := targetContext
	if !strings.HasPrefix(ctx, ".") {
		ctx = "." + ctx
	}

	return fmt.Sprintf(":%s:%s", ctx, baseName)
}

func parseSchemaFromArchive(path, content string, metadataFiles map[string]string) (schemaToImport, error) {
	var schema schemaToImport
	schema.FilePath = path
	schema.Schema = content

	ext := filepath.Ext(path)
	switch ext {
	case ".proto":
		schema.SchemaType = "PROTOBUF"
	case ".json":
		if strings.Contains(content, `"$schema"`) {
			schema.SchemaType = "JSON"
		} else {
			schema.SchemaType = "AVRO"
		}
	default:
		schema.SchemaType = "AVRO"
	}

	// Parse path
	parts := strings.Split(path, "/")
	if len(parts) >= 2 {
		schema.Subject = parts[len(parts)-2]

		filename := parts[len(parts)-1]
		filename = strings.TrimSuffix(filename, ext)
		if strings.HasPrefix(filename, "v") {
			if v, err := strconv.Atoi(filename[1:]); err == nil {
				schema.Version = v
			}
		}
	}

	// Try metadata
	metadataPath := strings.TrimSuffix(path, ext) + ".metadata.json"
	if metadataContent, ok := metadataFiles[metadataPath]; ok {
		var metadata struct {
			Subject    string                   `json:"subject"`
			Version    int                      `json:"version"`
			SchemaType string                   `json:"schemaType"`
			References []client.SchemaReference `json:"references"`
		}
		if json.Unmarshal([]byte(metadataContent), &metadata) == nil {
			if metadata.Subject != "" {
				schema.Subject = metadata.Subject
			}
			if metadata.Version > 0 {
				schema.Version = metadata.Version
			}
			if metadata.SchemaType != "" {
				schema.SchemaType = metadata.SchemaType
			}
			schema.References = metadata.References
		}
	}

	if schema.Subject == "" {
		return schema, fmt.Errorf("could not determine subject name")
	}

	return schema, nil
}

func sortSchemasByDependencies(schemas []schemaToImport) {
	// Build a map of subject -> schemas for that subject
	subjectSchemas := make(map[string][]schemaToImport)
	for _, s := range schemas {
		subjectSchemas[s.Subject] = append(subjectSchemas[s.Subject], s)
	}

	// Build dependency graph: subject -> subjects it depends on
	deps := make(map[string]map[string]bool)
	for _, s := range schemas {
		if deps[s.Subject] == nil {
			deps[s.Subject] = make(map[string]bool)
		}
		for _, ref := range s.References {
			deps[s.Subject][ref.Subject] = true
		}
	}

	// Topological sort using Kahn's algorithm
	// Count incoming edges (how many subjects depend on each subject)
	inDegree := make(map[string]int)
	for subj := range subjectSchemas {
		inDegree[subj] = 0
	}
	for _, depSet := range deps {
		for dep := range depSet {
			inDegree[dep]++ // This subject is depended upon
		}
	}

	// Start with subjects that have no dependencies
	var queue []string
	for subj := range subjectSchemas {
		if len(deps[subj]) == 0 {
			queue = append(queue, subj)
		}
	}

	// Sort queue for deterministic output
	sort.Strings(queue)

	var sortedSubjects []string
	for len(queue) > 0 {
		// Take first subject from queue
		subj := queue[0]
		queue = queue[1:]
		sortedSubjects = append(sortedSubjects, subj)

		// For each subject that depends on this one, reduce its in-degree
		for otherSubj, depSet := range deps {
			if depSet[subj] {
				delete(deps[otherSubj], subj)
				if len(deps[otherSubj]) == 0 {
					queue = append(queue, otherSubj)
					sort.Strings(queue) // Keep sorted for determinism
				}
			}
		}
	}

	// If we couldn't sort all subjects, there's a cycle - fall back to simple sort
	if len(sortedSubjects) != len(subjectSchemas) {
		// Circular dependency detected, use simple sort as fallback
		sort.SliceStable(schemas, func(i, j int) bool {
			return len(schemas[i].References) < len(schemas[j].References)
		})
		return
	}

	// Rebuild schemas slice in sorted order
	var result []schemaToImport
	for _, subj := range sortedSubjects {
		// Sort versions within subject
		subjSchemas := subjectSchemas[subj]
		sort.SliceStable(subjSchemas, func(i, j int) bool {
			return subjSchemas[i].Version < subjSchemas[j].Version
		})
		result = append(result, subjSchemas...)
	}

	// Copy back to original slice
	copy(schemas, result)
}

func dryRunImport(c *client.SchemaRegistryClient, schemas []schemaToImport, existingSubjects map[string]bool) error {
	output.Header("Dry Run Results")

	var toImport, toSkip, invalid int

	for _, s := range schemas {
		status := "IMPORT"

		if existingSubjects != nil && existingSubjects[s.Subject] {
			status = "SKIP (exists)"
			toSkip++
		} else if s.Subject == "" {
			status = "INVALID (no subject)"
			invalid++
		} else {
			// Check compatibility if subject exists
			existing, _ := c.GetVersions(s.Subject, false)
			if len(existing) > 0 {
				clientSchema := &client.Schema{
					Schema:     s.Schema,
					SchemaType: s.SchemaType,
					References: s.References,
				}
				compatible, _ := c.CheckCompatibility(s.Subject, clientSchema, "latest")
				if !compatible {
					status = "INCOMPATIBLE"
					invalid++
				} else {
					toImport++
				}
			} else {
				toImport++
			}
		}

		refCount := ""
		if len(s.References) > 0 {
			refCount = fmt.Sprintf(" (refs: %d)", len(s.References))
		}

		fmt.Printf("  %s: %s v%d%s - %s\n",
			getStatusIcon(status), s.Subject, s.Version, refCount, status)
	}

	fmt.Println()
	output.PrintTable(
		[]string{"Status", "Count"},
		[][]string{
			{"To Import", strconv.Itoa(toImport)},
			{"To Skip", strconv.Itoa(toSkip)},
			{"Invalid", strconv.Itoa(invalid)},
		},
	)

	return nil
}

func getStatusIcon(status string) string {
	switch {
	case strings.HasPrefix(status, "IMPORT"):
		return output.Green("✓")
	case strings.HasPrefix(status, "SKIP"):
		return output.Yellow("○")
	default:
		return output.Red("✗")
	}
}

func performImport(c *client.SchemaRegistryClient, schemas []schemaToImport, existingSubjects map[string]bool) error {
	output.Step("Importing schemas...")

	bar := progressbar.NewOptions(len(schemas),
		progressbar.OptionSetDescription("Importing"),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	var imported, skipped, failed int

	for _, s := range schemas {
		// Skip if exists and flag set
		if importSkipExisting && existingSubjects != nil && existingSubjects[s.Subject] {
			skipped++
			bar.Add(1)
			continue
		}

		// Set compatibility if specified
		if importCompatibility != "" {
			_ = c.SetSubjectConfig(s.Subject, importCompatibility)
		}

		// Register schema
		clientSchema := &client.Schema{
			Schema:     s.Schema,
			SchemaType: s.SchemaType,
			References: s.References,
		}

		_, err := c.RegisterSchema(s.Subject, clientSchema)
		if err != nil {
			output.Warning("Failed to import %s v%d: %v", s.Subject, s.Version, err)
			failed++
		} else {
			imported++
		}

		bar.Add(1)
	}

	bar.Finish()

	output.Header("Import Complete")
	output.PrintTable(
		[]string{"Status", "Count"},
		[][]string{
			{"Imported", strconv.Itoa(imported)},
			{"Skipped", strconv.Itoa(skipped)},
			{"Failed", strconv.Itoa(failed)},
		},
	)

	if failed > 0 {
		return fmt.Errorf("%d schemas failed to import", failed)
	}

	return nil
}
