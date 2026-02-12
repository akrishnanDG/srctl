package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	backupOutput   string
	backupSubjects []string
	backupByID     bool
	backupWorkers  int
	backupConfigs  bool
	backupTags     bool
)

var backupCmd = &cobra.Command{
	Use:     "backup",
	Short:   "Backup Schema Registry data",
	GroupID: groupBulk,
	Long: `Create a complete backup of Schema Registry including schemas, configs, and modes.

Backup scope options:
  • Full registry backup (default)
  • Single context backup (--context)
  • Specific subjects (--subjects)
  • Backup using schema IDs (--by-id)

The backup includes:
  • All schema versions with their global IDs
  • Subject configurations (compatibility levels)
  • Subject modes
  • Schema references

Multi-threading:
  • Use --workers to control parallel backup speed (default: 10)

Examples:
  # Full registry backup
  srctl backup --output ./backup

  # Backup with more parallel workers
  srctl backup --output ./backup --workers 50

  # Backup specific context
  srctl backup --context .production --output ./backup

  # Backup specific subjects
  srctl backup --subjects user-events,order-events --output ./backup

  # Backup preserving schema IDs (useful for migration)
  srctl backup --by-id --output ./backup

  # Backup with all subject-level configs
  srctl backup --output ./backup --configs`,
	RunE: runBackup,
}

func init() {
	backupCmd.Flags().StringVarP(&backupOutput, "output", "o", "", "Output directory for backup (required)")
	backupCmd.Flags().StringSliceVar(&backupSubjects, "subjects", nil, "Specific subjects to backup (comma-separated)")
	backupCmd.Flags().BoolVar(&backupByID, "by-id", false, "Include schema ID mapping for exact restoration")
	backupCmd.Flags().IntVar(&backupWorkers, "workers", 10, "Number of parallel workers for backup")
	backupCmd.Flags().BoolVar(&backupConfigs, "configs", true, "Include subject-level configurations")
	backupCmd.Flags().BoolVar(&backupTags, "tags", true, "Include tag definitions and associations")

	backupCmd.MarkFlagRequired("output")
	rootCmd.AddCommand(backupCmd)
}

// BackupManifest contains metadata about the backup
type BackupManifest struct {
	Version     string    `json:"version"`
	CreatedAt   time.Time `json:"createdAt"`
	RegistryURL string    `json:"registryUrl"`
	Context     string    `json:"context,omitempty"`
	Statistics  struct {
		Subjects       int `json:"subjects"`
		Schemas        int `json:"schemas"`
		TotalIDs       int `json:"totalIds"`
		TagDefinitions int `json:"tagDefinitions,omitempty"`
		TagAssignments int `json:"tagAssignments,omitempty"`
	} `json:"statistics"`
	BySchemaID   bool `json:"bySchemaId"`
	IncludesTags bool `json:"includesTags,omitempty"`
}

// TagBackup contains tag definitions and assignments
type TagBackup struct {
	Definitions      []client.Tag          `json:"definitions"`
	Assignments      []TagAssignmentBackup `json:"assignments"`
	TopicAssignments []TopicTagBackup      `json:"topicAssignments,omitempty"`
}

// TagAssignmentBackup stores tag assignment for backup
type TagAssignmentBackup struct {
	Subject  string   `json:"subject"`
	Version  int      `json:"version,omitempty"` // 0 means subject-level
	TagNames []string `json:"tagNames"`
}

// TopicTagBackup stores tag assignment for a Kafka topic
type TopicTagBackup struct {
	Topic    string   `json:"topic"`
	TagNames []string `json:"tagNames"`
}

// SubjectBackup contains all data for a subject
type SubjectBackup struct {
	Subject       string                `json:"subject"`
	Compatibility string                `json:"compatibility,omitempty"`
	Mode          string                `json:"mode,omitempty"`
	Versions      []SchemaVersionBackup `json:"versions"`
}

// SchemaVersionBackup contains a single schema version
type SchemaVersionBackup struct {
	Version    int                      `json:"version"`
	SchemaID   int                      `json:"schemaId"`
	SchemaType string                   `json:"schemaType"`
	Schema     string                   `json:"schema"`
	References []client.SchemaReference `json:"references,omitempty"`
}

// IDMapping maps schema IDs to subjects/versions for restoration
type IDMapping struct {
	SchemaID   int    `json:"schemaId"`
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	SchemaType string `json:"schemaType"`
}

func runBackup(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Schema Registry Backup")

	// Create output directory
	timestamp := time.Now().Format("20060102-150405")
	backupDir := filepath.Join(backupOutput, fmt.Sprintf("sr-backup-%s", timestamp))

	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	output.Info("Backup directory: %s", backupDir)
	output.Info("Workers: %d", backupWorkers)

	// Initialize manifest
	manifest := BackupManifest{
		Version:     "1.0",
		CreatedAt:   time.Now().UTC(),
		RegistryURL: registryURL,
		Context:     context,
		BySchemaID:  backupByID,
	}

	// Get subjects to backup
	var subjects []string
	if len(backupSubjects) > 0 {
		subjects = backupSubjects
		output.Info("Backing up %d specified subjects", len(subjects))
	} else {
		output.Step("Fetching subjects...")
		var err error
		subjects, err = c.GetSubjects(true) // Include deleted for complete backup
		if err != nil {
			return fmt.Errorf("failed to get subjects: %w", err)
		}
		output.Info("Found %d subjects", len(subjects))
	}

	if len(subjects) == 0 {
		output.Warning("No subjects to backup")
		return nil
	}

	// Backup global config
	output.Step("Backing up global configuration...")
	globalConfig, err := c.GetConfig()
	if err == nil && globalConfig != nil {
		configData := map[string]string{
			"compatibility": globalConfig.CompatibilityLevel,
		}
		if configData["compatibility"] == "" {
			configData["compatibility"] = globalConfig.Compatibility
		}
		saveJSON(filepath.Join(backupDir, "global-config.json"), configData)
	}

	// Backup global mode
	globalMode, err := c.GetMode()
	if err == nil && globalMode != nil {
		modeData := map[string]string{"mode": globalMode.Mode}
		saveJSON(filepath.Join(backupDir, "global-mode.json"), modeData)
	}

	// Backup subjects in parallel
	subjectsDir := filepath.Join(backupDir, "subjects")
	os.MkdirAll(subjectsDir, 0755)

	output.Step("Backing up schemas (%d workers)...", backupWorkers)
	backupResults := backupSubjectsParallel(c, subjects, subjectsDir)

	// Aggregate results
	var totalSchemas int
	var idMappings []IDMapping
	allIDs := make(map[int]bool)
	var failedCount int

	for _, r := range backupResults {
		if r.Error != nil {
			failedCount++
			continue
		}
		totalSchemas += r.VersionCount
		if backupByID {
			idMappings = append(idMappings, r.IDMappings...)
			for _, m := range r.IDMappings {
				allIDs[m.SchemaID] = true
			}
		}
	}

	// Save ID mappings if requested
	if backupByID && len(idMappings) > 0 {
		output.Step("Saving schema ID mappings...")
		saveJSON(filepath.Join(backupDir, "id-mappings.json"), idMappings)

		// Also save schemas by ID for direct restoration
		output.Step("Saving schemas by ID...")
		saveSchemasByIDParallel(c, idMappings, backupDir)
	}

	// Backup tags if enabled
	var tagDefCount, tagAssignCount int
	if backupTags {
		output.Step("Backing up tags...")
		tagDefCount, tagAssignCount = backupTagsData(c, subjects, backupDir)
		manifest.IncludesTags = true
	}

	// Update and save manifest
	manifest.Statistics.Subjects = len(subjects) - failedCount
	manifest.Statistics.Schemas = totalSchemas
	manifest.Statistics.TotalIDs = len(allIDs)
	manifest.Statistics.TagDefinitions = tagDefCount
	manifest.Statistics.TagAssignments = tagAssignCount

	saveJSON(filepath.Join(backupDir, "manifest.json"), manifest)

	// Summary
	output.Header("Backup Complete")
	rows := [][]string{
		{"Subjects", strconv.Itoa(manifest.Statistics.Subjects)},
		{"Schema Versions", strconv.Itoa(manifest.Statistics.Schemas)},
		{"Unique Schema IDs", strconv.Itoa(manifest.Statistics.TotalIDs)},
	}
	if backupTags {
		rows = append(rows, []string{"Tag Definitions", strconv.Itoa(tagDefCount)})
		rows = append(rows, []string{"Tag Assignments", strconv.Itoa(tagAssignCount)})
	}
	rows = append(rows, []string{"Failed", strconv.Itoa(failedCount)})
	rows = append(rows, []string{"Location", backupDir})
	output.PrintTable([]string{"Metric", "Value"}, rows)

	// Calculate backup size
	size, _ := getDirSize(backupDir)
	output.Info("Backup size: %s", output.FormatBytes(size))

	return nil
}

// backupResult holds the result of backing up a single subject
type backupResult struct {
	Subject      string
	VersionCount int
	IDMappings   []IDMapping
	Error        error
}

// backupSubjectsParallel backs up subjects in parallel
func backupSubjectsParallel(c *client.SchemaRegistryClient, subjects []string, subjectsDir string) []backupResult {
	jobs := make(chan string, len(subjects))
	results := make(chan backupResult, len(subjects))

	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Backing up"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < backupWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := backupResult{Subject: subj}

				subjectBackup, ids, err := backupSubject(c, subj, backupByID)
				if err != nil {
					result.Error = err
					results <- result
					bar.Add(1)
					continue
				}

				// Save subject backup
				// Use URL encoding for safe filenames (handles /, _, and special chars)
				safeName := url.PathEscape(subj)
				subjectFile := filepath.Join(subjectsDir, safeName+".json")
				if err := saveJSON(subjectFile, subjectBackup); err != nil {
					result.Error = err
					results <- result
					bar.Add(1)
					continue
				}

				result.VersionCount = len(subjectBackup.Versions)
				result.IDMappings = ids
				results <- result
				bar.Add(1)
			}
		}()
	}

	// Send jobs
	go func() {
		for _, subj := range subjects {
			jobs <- subj
		}
		close(jobs)
	}()

	// Wait and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allResults []backupResult
	for r := range results {
		allResults = append(allResults, r)
	}
	bar.Finish()

	return allResults
}

// saveSchemasByIDParallel saves schemas by ID in parallel
func saveSchemasByIDParallel(c *client.SchemaRegistryClient, mappings []IDMapping, backupDir string) {
	schemasDir := filepath.Join(backupDir, "schemas-by-id")
	os.MkdirAll(schemasDir, 0755)

	// Deduplicate by ID
	uniqueIDs := make(map[int]IDMapping)
	for _, m := range mappings {
		uniqueIDs[m.SchemaID] = m
	}

	jobs := make(chan IDMapping, len(uniqueIDs))
	var wg sync.WaitGroup
	var saved int64

	bar := progressbar.NewOptions(len(uniqueIDs),
		progressbar.OptionSetDescription("Saving by ID"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	for i := 0; i < backupWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for mapping := range jobs {
				schema, err := c.GetSchemaByID(mapping.SchemaID)
				if err == nil {
					schemaFile := filepath.Join(schemasDir, fmt.Sprintf("%d.json", mapping.SchemaID))
					saveJSON(schemaFile, map[string]interface{}{
						"schemaId":   mapping.SchemaID,
						"schemaType": mapping.SchemaType,
						"schema":     schema.Schema,
					})
					atomic.AddInt64(&saved, 1)
				}
				bar.Add(1)
			}
		}()
	}

	for _, mapping := range uniqueIDs {
		jobs <- mapping
	}
	close(jobs)
	wg.Wait()
	bar.Finish()
}

// backupTagsData backs up tag definitions and assignments
func backupTagsData(c *client.SchemaRegistryClient, subjects []string, backupDir string) (defCount, assignCount int) {
	tagBackup := TagBackup{
		Definitions: []client.Tag{},
		Assignments: []TagAssignmentBackup{},
	}

	// Get tag definitions
	tags, err := c.GetTags()
	if err != nil {
		output.Warning("Failed to get tag definitions (tags API may not be available): %v", err)
		return 0, 0
	}
	tagBackup.Definitions = tags

	// Get tag assignments for each subject
	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Backing up tags"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	type tagResult struct {
		Assignments []TagAssignmentBackup
	}

	jobs := make(chan string, len(subjects))
	results := make(chan tagResult, len(subjects))

	var wg sync.WaitGroup
	for i := 0; i < backupWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := tagResult{}

				// Get subject-level tags
				subjectTags, err := c.GetSubjectTags(subj)
				if err == nil && len(subjectTags) > 0 {
					var tagNames []string
					for _, t := range subjectTags {
						tagNames = append(tagNames, t.TypeName)
					}
					result.Assignments = append(result.Assignments, TagAssignmentBackup{
						Subject:  subj,
						Version:  0, // 0 = subject-level
						TagNames: tagNames,
					})
				}

				// Get schema-level tags for each version
				versions, err := c.GetVersions(subj, false)
				if err == nil {
					for _, v := range versions {
						schemaTags, err := c.GetSchemaTags(subj, v)
						if err == nil && len(schemaTags) > 0 {
							var tagNames []string
							for _, t := range schemaTags {
								tagNames = append(tagNames, t.TypeName)
							}
							result.Assignments = append(result.Assignments, TagAssignmentBackup{
								Subject:  subj,
								Version:  v,
								TagNames: tagNames,
							})
						}
					}
				}

				results <- result
				bar.Add(1)
			}
		}()
	}

	// Send jobs
	go func() {
		for _, subj := range subjects {
			jobs <- subj
		}
		close(jobs)
	}()

	// Wait and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for r := range results {
		tagBackup.Assignments = append(tagBackup.Assignments, r.Assignments...)
	}
	bar.Finish()

	// Collect topic-level tags
	// Extract unique topic names from subjects (strip -value/-key suffix)
	topicSet := make(map[string]bool)
	for _, subj := range subjects {
		topic := subj
		if strings.HasSuffix(topic, "-value") {
			topic = strings.TrimSuffix(topic, "-value")
		} else if strings.HasSuffix(topic, "-key") {
			topic = strings.TrimSuffix(topic, "-key")
		}
		topicSet[topic] = true
	}

	for topic := range topicSet {
		topicTags, err := c.GetTopicTags(topic)
		if err == nil && len(topicTags) > 0 {
			var tagNames []string
			for _, t := range topicTags {
				tagNames = append(tagNames, t.TypeName)
			}
			tagBackup.TopicAssignments = append(tagBackup.TopicAssignments, TopicTagBackup{
				Topic:    topic,
				TagNames: tagNames,
			})
		}
	}

	// Save tag backup
	saveJSON(filepath.Join(backupDir, "tags.json"), tagBackup)

	totalAssignments := len(tagBackup.Assignments) + len(tagBackup.TopicAssignments)
	return len(tagBackup.Definitions), totalAssignments
}

func backupSubject(c *client.SchemaRegistryClient, subject string, byID bool) (*SubjectBackup, []IDMapping, error) {
	backup := &SubjectBackup{
		Subject: subject,
	}
	var idMappings []IDMapping

	// Get subject config
	config, err := c.GetSubjectConfig(subject, false)
	if err == nil && config != nil {
		backup.Compatibility = config.CompatibilityLevel
		if backup.Compatibility == "" {
			backup.Compatibility = config.Compatibility
		}
	}

	// Get subject mode
	mode, err := c.GetSubjectMode(subject, false)
	if err == nil && mode != nil {
		backup.Mode = mode.Mode
	}

	// Get all versions
	versions, err := c.GetVersions(subject, true)
	if err != nil {
		return nil, nil, err
	}

	for _, v := range versions {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			continue
		}

		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}

		backup.Versions = append(backup.Versions, SchemaVersionBackup{
			Version:    v,
			SchemaID:   schema.ID,
			SchemaType: schemaType,
			Schema:     schema.Schema,
			References: schema.References,
		})

		if byID {
			idMappings = append(idMappings, IDMapping{
				SchemaID:   schema.ID,
				Subject:    subject,
				Version:    v,
				SchemaType: schemaType,
			})
		}
	}

	return backup, idMappings, nil
}

func saveJSON(path string, data interface{}) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, content, 0644)
}

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// Restore command
var restoreCmd = &cobra.Command{
	Use:     "restore <backup-path>",
	Short:   "Restore Schema Registry from backup",
	GroupID: groupBulk,
	Long: `Restore Schema Registry from a backup created by the backup command.

Restoration options:
  • Full restore (creates new subjects)
  • Restore with original schema IDs (if backup was created with --by-id)
  • Restore specific subjects only

Examples:
  # Full restore
  srctl restore ./backup/sr-backup-20240115-120000

  # Restore with original IDs (requires IMPORT mode)
  srctl restore ./backup/sr-backup-20240115-120000 --preserve-ids

  # Restore specific subjects
  srctl restore ./backup/sr-backup-20240115-120000 --subjects user-events

  # Dry run
  srctl restore ./backup/sr-backup-20240115-120000 --dry-run`,
	Args: cobra.ExactArgs(1),
	RunE: runRestore,
}

var (
	restoreDryRun        bool
	restorePreserveID    bool
	restoreSubjects      []string
	restoreTags          bool
	restoreTargetContext string
)

func init() {
	restoreCmd.Flags().BoolVar(&restoreDryRun, "dry-run", false, "Validate restore without making changes")
	restoreCmd.Flags().BoolVar(&restorePreserveID, "preserve-ids", false, "Restore with original schema IDs (requires IMPORT mode)")
	restoreCmd.Flags().StringSliceVar(&restoreSubjects, "subjects", nil, "Restore only specific subjects")
	restoreCmd.Flags().BoolVar(&restoreTags, "tags", true, "Restore tag definitions and associations")
	restoreCmd.Flags().StringVar(&restoreTargetContext, "target-context", "", "Restore into specific context (rewrites subject names)")
	// Note: Restore is sequential to maintain dependency order (schemas must be registered before schemas that reference them)

	rootCmd.AddCommand(restoreCmd)
}

func runRestore(cmd *cobra.Command, args []string) error {
	backupPath := args[0]

	output.Header("Schema Registry Restore")
	output.Info("Source: %s", backupPath)

	// Read manifest
	manifestPath := filepath.Join(backupPath, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest BackupManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("invalid manifest: %w", err)
	}

	output.Info("Backup created: %s", manifest.CreatedAt.Format(time.RFC3339))
	output.Info("Subjects: %d, Schemas: %d", manifest.Statistics.Subjects, manifest.Statistics.Schemas)

	if restorePreserveID && !manifest.BySchemaID {
		return fmt.Errorf("backup was not created with --by-id, cannot preserve schema IDs")
	}

	c, err := GetClient()
	if err != nil {
		return err
	}

	// Set IMPORT mode if preserving IDs
	if restorePreserveID && !restoreDryRun {
		output.Step("Setting registry to IMPORT mode...")
		if err := c.SetMode("IMPORT"); err != nil {
			return fmt.Errorf("failed to set IMPORT mode: %w", err)
		}
		defer func() {
			output.Step("Restoring READWRITE mode...")
			c.SetMode("READWRITE")
		}()
	}

	// Read subject backups
	subjectsDir := filepath.Join(backupPath, "subjects")
	files, err := os.ReadDir(subjectsDir)
	if err != nil {
		return fmt.Errorf("failed to read subjects directory: %w", err)
	}

	var toRestore []string
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		encodedName := strings.TrimSuffix(f.Name(), ".json")
		// URL decode the filename to get the original subject name
		subjectName, err := url.PathUnescape(encodedName)
		if err != nil {
			subjectName = encodedName // Fall back to encoded name if decode fails
		}

		// Filter if specific subjects requested
		if len(restoreSubjects) > 0 {
			found := false
			for _, s := range restoreSubjects {
				if s == subjectName {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		toRestore = append(toRestore, f.Name())
	}

	if restoreDryRun {
		output.Header("Dry Run - Would Restore")
		for _, f := range toRestore {
			encodedName := strings.TrimSuffix(f, ".json")
			subjectName, _ := url.PathUnescape(encodedName)
			fmt.Printf("  %s %s\n", output.Green("→"), subjectName)
		}
		output.Info("\nTotal: %d subjects", len(toRestore))
		return nil
	}

	// Read all backup files first to sort by dependencies
	output.Step("Reading backup files...")
	var backups []SubjectBackup
	for _, f := range toRestore {
		filePath := filepath.Join(subjectsDir, f)
		data, err := os.ReadFile(filePath)
		if err != nil {
			output.Warning("Failed to read %s: %v", f, err)
			continue
		}

		var backup SubjectBackup
		if err := json.Unmarshal(data, &backup); err != nil {
			output.Warning("Failed to parse %s: %v", f, err)
			continue
		}
		backups = append(backups, backup)
	}

	// Rewrite subject names and references if target context specified
	if restoreTargetContext != "" {
		output.Info("Rewriting subjects to context: %s", restoreTargetContext)
		rewriteBackupContexts(backups, restoreTargetContext)
	}

	// Sort backups by dependencies (subjects without references first)
	sortBackupsByDependencies(backups)

	// Perform restore
	output.Step("Restoring %d subjects...", len(backups))
	bar := progressbar.NewOptions(len(backups),
		progressbar.OptionSetDescription("Restoring"),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	var restored, failed int

	for _, backup := range backups {
		// Set subject config
		if backup.Compatibility != "" {
			c.SetSubjectConfig(backup.Subject, backup.Compatibility)
		}
		if backup.Mode != "" {
			c.SetSubjectMode(backup.Subject, backup.Mode)
		}

		// Register schemas (in order by version)
		sort.Slice(backup.Versions, func(i, j int) bool {
			return backup.Versions[i].Version < backup.Versions[j].Version
		})

		allSucceeded := true
		for _, ver := range backup.Versions {
			schema := &client.Schema{
				Schema:     ver.Schema,
				SchemaType: ver.SchemaType,
				References: ver.References,
			}
			// Include schema ID if preserving IDs
			if restorePreserveID {
				schema.ID = ver.SchemaID
			}

			_, err := c.RegisterSchema(backup.Subject, schema)
			if err != nil {
				output.Warning("Failed to restore %s v%d: %v", backup.Subject, ver.Version, err)
				allSucceeded = false
			}
		}

		if allSucceeded {
			restored++
		} else {
			failed++
		}
		bar.Add(1)
	}
	bar.Finish()

	// Restore tags if available and enabled
	var tagDefsRestored, tagAssignsRestored int
	if restoreTags && manifest.IncludesTags {
		output.Step("Restoring tags...")
		tagDefsRestored, tagAssignsRestored = restoreTagsData(c, backupPath)
	}

	output.Header("Restore Complete")
	rows := [][]string{
		{"Subjects Restored", strconv.Itoa(restored)},
		{"Subjects Failed", strconv.Itoa(failed)},
	}
	if restoreTags && manifest.IncludesTags {
		rows = append(rows, []string{"Tag Definitions", strconv.Itoa(tagDefsRestored)})
		rows = append(rows, []string{"Tag Assignments", strconv.Itoa(tagAssignsRestored)})
	}
	output.PrintTable([]string{"Status", "Count"}, rows)

	return nil
}

// rewriteBackupContexts rewrites subject names and references to use a new context
func rewriteBackupContexts(backups []SubjectBackup, targetContext string) {
	// Build mapping from old subject names to new subject names
	rewriteMap := make(map[string]string)

	for i := range backups {
		oldSubject := backups[i].Subject
		newSubject := rewriteSubjectContextForBackup(oldSubject, targetContext)
		rewriteMap[oldSubject] = newSubject
		backups[i].Subject = newSubject
	}

	// Rewrite references in all versions
	for i := range backups {
		for j := range backups[i].Versions {
			for k := range backups[i].Versions[j].References {
				oldRefSubject := backups[i].Versions[j].References[k].Subject
				if newRefSubject, ok := rewriteMap[oldRefSubject]; ok {
					backups[i].Versions[j].References[k].Subject = newRefSubject
				} else {
					// Reference to a subject not in our backup set - try to rewrite anyway
					backups[i].Versions[j].References[k].Subject = rewriteSubjectContextForBackup(oldRefSubject, targetContext)
				}
			}
		}
	}
}

// rewriteSubjectContextForBackup rewrites a single subject name to use a new context
func rewriteSubjectContextForBackup(subject string, targetContext string) string {
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

// sortBackupsByDependencies sorts backups so dependencies come first
func sortBackupsByDependencies(backups []SubjectBackup) {
	// Build dependency graph
	deps := make(map[string]map[string]bool)
	subjectIndex := make(map[string]int)

	for i, b := range backups {
		subjectIndex[b.Subject] = i
		deps[b.Subject] = make(map[string]bool)
		for _, ver := range b.Versions {
			for _, ref := range ver.References {
				deps[b.Subject][ref.Subject] = true
			}
		}
	}

	// Topological sort using Kahn's algorithm
	inDegree := make(map[string]int)
	for subj := range subjectIndex {
		inDegree[subj] = 0
	}
	for _, depSet := range deps {
		for dep := range depSet {
			if _, exists := subjectIndex[dep]; exists {
				inDegree[dep]++
			}
		}
	}

	// Start with subjects that have no dependencies
	var queue []string
	for subj := range subjectIndex {
		if len(deps[subj]) == 0 {
			queue = append(queue, subj)
		}
	}
	sort.Strings(queue)

	var sortedSubjects []string
	for len(queue) > 0 {
		subj := queue[0]
		queue = queue[1:]
		sortedSubjects = append(sortedSubjects, subj)

		for otherSubj, depSet := range deps {
			if depSet[subj] {
				delete(deps[otherSubj], subj)
				if len(deps[otherSubj]) == 0 {
					queue = append(queue, otherSubj)
					sort.Strings(queue)
				}
			}
		}
	}

	// If couldn't sort all (cycle), fall back to simple sort
	if len(sortedSubjects) != len(backups) {
		sort.Slice(backups, func(i, j int) bool {
			iRefs := 0
			jRefs := 0
			for _, v := range backups[i].Versions {
				iRefs += len(v.References)
			}
			for _, v := range backups[j].Versions {
				jRefs += len(v.References)
			}
			return iRefs < jRefs
		})
		return
	}

	// Reorder backups slice according to sorted order
	result := make([]SubjectBackup, len(backups))
	for i, subj := range sortedSubjects {
		result[i] = backups[subjectIndex[subj]]
	}
	copy(backups, result)
}

// restoreTagsData restores tag definitions and assignments from backup
func restoreTagsData(c *client.SchemaRegistryClient, backupPath string) (defsRestored, assignsRestored int) {
	tagsFile := filepath.Join(backupPath, "tags.json")
	tagsData, err := os.ReadFile(tagsFile)
	if err != nil {
		output.Warning("No tags file found in backup")
		return 0, 0
	}

	var tagBackup TagBackup
	if err := json.Unmarshal(tagsData, &tagBackup); err != nil {
		output.Warning("Failed to parse tags file: %v", err)
		return 0, 0
	}

	// Restore tag definitions
	for _, tag := range tagBackup.Definitions {
		err := c.CreateTag(&tag)
		if err != nil {
			// Tag might already exist, which is fine
			if !strings.Contains(err.Error(), "already exists") {
				output.Warning("Failed to create tag %s: %v", tag.Name, err)
				continue
			}
		}
		defsRestored++
	}

	// Restore tag assignments (schema-level)
	for _, assign := range tagBackup.Assignments {
		for _, tagName := range assign.TagNames {
			var err error
			if assign.Version == 0 {
				err = c.AssignTagToSubject(assign.Subject, tagName)
			} else {
				err = c.AssignTagToSchema(assign.Subject, assign.Version, tagName)
			}
			if err != nil && !strings.Contains(err.Error(), "already") {
				output.Warning("Failed to assign tag %s to %s: %v", tagName, assign.Subject, err)
				continue
			}
			assignsRestored++
		}
	}

	// Restore topic-level tag assignments
	for _, topicAssign := range tagBackup.TopicAssignments {
		for _, tagName := range topicAssign.TagNames {
			err := c.AssignTagToTopic(topicAssign.Topic, tagName)
			if err != nil && !strings.Contains(err.Error(), "already") {
				output.Warning("Failed to assign tag %s to topic %s: %v", tagName, topicAssign.Topic, err)
				continue
			}
			assignsRestored++
		}
	}

	return defsRestored, assignsRestored
}
