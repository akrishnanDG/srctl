package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var compareCmd = &cobra.Command{
	Use:     "compare",
	Short:   "Compare schemas across registries",
	GroupID: groupCrossReg,
	Long: `Compare schemas between two Schema Registries to find drift.

Comparison modes:
  • Compare by subject names and versions
  • Compare by schema IDs
  • Show schemas that exist only in source or target

Examples:
  # Compare two registries
  srctl compare --source dev --target prod

  # Compare specific subjects
  srctl compare --source dev --target prod --subjects user-events

  # Compare by schema ID
  srctl compare --source dev --target prod --by-id

  # Show only differences
  srctl compare --source dev --target prod --diff-only

  # Compare specific contexts
  srctl compare --source dev --target prod --source-context .staging --target-context .production`,
	RunE: runCompare,
}

var (
	compareSource        string
	compareTarget        string
	compareSubjects      []string
	compareByID          bool
	compareDiffOnly      bool
	compareSourceContext string
	compareTargetContext string
	compareWorkers       int
)

func init() {
	compareCmd.Flags().StringVar(&compareSource, "source", "", "Source registry name (required)")
	compareCmd.Flags().StringVar(&compareTarget, "target", "", "Target registry name (required)")
	compareCmd.Flags().StringSliceVar(&compareSubjects, "subjects", nil, "Compare only specific subjects")
	compareCmd.Flags().BoolVar(&compareByID, "by-id", false, "Compare using schema IDs")
	compareCmd.Flags().BoolVar(&compareDiffOnly, "diff-only", false, "Show only differences")
	compareCmd.Flags().StringVar(&compareSourceContext, "source-context", "", "Source context")
	compareCmd.Flags().StringVar(&compareTargetContext, "target-context", "", "Target context")
	compareCmd.Flags().IntVar(&compareWorkers, "workers", 10, "Number of parallel workers for comparison")

	compareCmd.MarkFlagRequired("source")
	compareCmd.MarkFlagRequired("target")

	rootCmd.AddCommand(compareCmd)
}

type CompareResult struct {
	Subject      string
	SourceOnly   bool
	TargetOnly   bool
	VersionDiff  bool
	SchemaDiff   bool
	ConfigDiff   bool
	SourceVers   int
	TargetVers   int
	SourceLatest int
	TargetLatest int
}

func runCompare(cmd *cobra.Command, args []string) error {
	output.Header("Registry Comparison")
	output.Info("Source: %s", compareSource)
	output.Info("Target: %s", compareTarget)

	// Get clients
	sourceClient, err := GetClientForRegistry(compareSource)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	targetClient, err := GetClientForRegistry(compareTarget)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	// Apply contexts
	if compareSourceContext != "" {
		sourceClient = sourceClient.WithContext(compareSourceContext)
	}
	if compareTargetContext != "" {
		targetClient = targetClient.WithContext(compareTargetContext)
	}

	// Get subjects from both registries
	output.Step("Fetching subjects from source...")
	sourceSubjects, err := sourceClient.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get source subjects: %w", err)
	}

	output.Step("Fetching subjects from target...")
	targetSubjects, err := targetClient.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get target subjects: %w", err)
	}

	// Filter if specific subjects requested
	if len(compareSubjects) > 0 {
		sourceSubjects = filterByList(sourceSubjects, compareSubjects)
		targetSubjects = filterByList(targetSubjects, compareSubjects)
	}

	// Build maps
	sourceMap := make(map[string]bool)
	for _, s := range sourceSubjects {
		sourceMap[s] = true
	}

	targetMap := make(map[string]bool)
	for _, s := range targetSubjects {
		targetMap[s] = true
	}

	// All subjects
	allSubjects := make(map[string]bool)
	for _, s := range sourceSubjects {
		allSubjects[s] = true
	}
	for _, s := range targetSubjects {
		allSubjects[s] = true
	}

	output.Info("Source subjects: %d", len(sourceSubjects))
	output.Info("Target subjects: %d", len(targetSubjects))

	// Compare in parallel
	output.Step("Comparing schemas (%d workers)...", compareWorkers)
	results, identical, sourceOnly, targetOnly, different := compareSubjectsParallel(
		sourceClient, targetClient, allSubjects, sourceMap, targetMap,
	)

	// Display results
	output.Header("Comparison Results")

	// Summary
	output.PrintTable(
		[]string{"Status", "Count"},
		[][]string{
			{"Identical", strconv.Itoa(identical)},
			{"Different", strconv.Itoa(different)},
			{"Source Only", strconv.Itoa(sourceOnly)},
			{"Target Only", strconv.Itoa(targetOnly)},
			{"Total", strconv.Itoa(len(results))},
		},
	)

	// Details
	if sourceOnly > 0 {
		output.SubHeader("Subjects Only in Source (%s)", compareSource)
		for _, r := range results {
			if r.SourceOnly {
				fmt.Printf("  %s %s\n", output.Yellow("→"), r.Subject)
			}
		}
	}

	if targetOnly > 0 {
		output.SubHeader("Subjects Only in Target (%s)", compareTarget)
		for _, r := range results {
			if r.TargetOnly {
				fmt.Printf("  %s %s\n", output.Yellow("←"), r.Subject)
			}
		}
	}

	if different > 0 {
		output.SubHeader("Subjects with Differences")
		rows := [][]string{}
		for _, r := range results {
			if r.VersionDiff || r.SchemaDiff || r.ConfigDiff {
				diffs := []string{}
				if r.VersionDiff {
					diffs = append(diffs, fmt.Sprintf("versions (%d/%d)", r.SourceVers, r.TargetVers))
				}
				if r.SchemaDiff {
					diffs = append(diffs, "schema content")
				}
				if r.ConfigDiff {
					diffs = append(diffs, "config")
				}
				rows = append(rows, []string{r.Subject, strings.Join(diffs, ", ")})
			}
		}
		output.PrintTable([]string{"Subject", "Differences"}, rows)
	}

	if !compareDiffOnly && identical > 0 {
		output.SubHeader("Identical Subjects")
		for _, r := range results {
			if !r.SourceOnly && !r.TargetOnly && !r.VersionDiff && !r.SchemaDiff && !r.ConfigDiff {
				fmt.Printf("  %s %s\n", output.Green("✓"), r.Subject)
			}
		}
	}

	return nil
}

func filterByList(subjects []string, filter []string) []string {
	filterMap := make(map[string]bool)
	for _, f := range filter {
		filterMap[f] = true
	}

	var result []string
	for _, s := range subjects {
		if filterMap[s] {
			result = append(result, s)
		}
	}
	return result
}

// compareSubjectsParallel compares subjects in parallel
func compareSubjectsParallel(
	sourceClient, targetClient *client.SchemaRegistryClient,
	allSubjects map[string]bool,
	sourceMap, targetMap map[string]bool,
) ([]CompareResult, int, int, int, int) {
	// Convert map to slice for job distribution
	var subjectList []string
	for s := range allSubjects {
		subjectList = append(subjectList, s)
	}

	jobs := make(chan string, len(subjectList))
	results := make(chan CompareResult, len(subjectList))

	bar := progressbar.NewOptions(len(subjectList),
		progressbar.OptionSetDescription("Comparing"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < compareWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := CompareResult{Subject: subj}

				inSource := sourceMap[subj]
				inTarget := targetMap[subj]

				if !inTarget {
					result.SourceOnly = true
				} else if !inSource {
					result.TargetOnly = true
				} else {
					// Both exist - compare details
					sourceVersions, _ := sourceClient.GetVersions(subj, false)
					targetVersions, _ := targetClient.GetVersions(subj, false)

					result.SourceVers = len(sourceVersions)
					result.TargetVers = len(targetVersions)

					if len(sourceVersions) != len(targetVersions) {
						result.VersionDiff = true
					}

					// Get latest schemas
					if len(sourceVersions) > 0 && len(targetVersions) > 0 {
						result.SourceLatest = sourceVersions[len(sourceVersions)-1]
						result.TargetLatest = targetVersions[len(targetVersions)-1]

						if compareByID {
							// Compare by schema ID
							sourceSchema, _ := sourceClient.GetSchema(subj, "latest")
							targetSchema, _ := targetClient.GetSchema(subj, "latest")

							if sourceSchema != nil && targetSchema != nil {
								if sourceSchema.ID != targetSchema.ID {
									result.SchemaDiff = true
								}
							}
						} else {
							// Compare by content
							sourceSchema, _ := sourceClient.GetSchema(subj, "latest")
							targetSchema, _ := targetClient.GetSchema(subj, "latest")

							if sourceSchema != nil && targetSchema != nil {
								if sourceSchema.Schema != targetSchema.Schema {
									result.SchemaDiff = true
								}
							}
						}

						// Compare config
						sourceConfig, _ := sourceClient.GetSubjectConfig(subj, true)
						targetConfig, _ := targetClient.GetSubjectConfig(subj, true)

						if sourceConfig != nil && targetConfig != nil {
							sc := sourceConfig.CompatibilityLevel
							if sc == "" {
								sc = sourceConfig.Compatibility
							}
							tc := targetConfig.CompatibilityLevel
							if tc == "" {
								tc = targetConfig.Compatibility
							}
							if sc != tc {
								result.ConfigDiff = true
							}
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
		for _, subj := range subjectList {
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
	var allResults []CompareResult
	var identical, sourceOnly, targetOnly, different int

	for r := range results {
		allResults = append(allResults, r)

		if r.SourceOnly {
			sourceOnly++
		} else if r.TargetOnly {
			targetOnly++
		} else if r.VersionDiff || r.SchemaDiff || r.ConfigDiff {
			different++
		} else {
			identical++
		}
	}
	bar.Finish()

	return allResults, identical, sourceOnly, targetOnly, different
}

// Clone command
var cloneCmd = &cobra.Command{
	Use:     "clone",
	Short:   "Clone schemas between registries",
	GroupID: groupCrossReg,
	Long: `Clone schemas from one Schema Registry to another.

By default, schema IDs are preserved (requires IMPORT mode on target registry).
This ensures referential integrity and consistent IDs across environments.

Clone options:
  • Clone all subjects or specific subjects
  • Clone to different context  
  • Preserve schema IDs (default, requires IMPORT mode on target)
  • Clone subject-level configurations
  • Multi-threaded cloning
  • Dry run to preview changes

Examples:
  # Clone all schemas from dev to prod (preserves schema IDs)
  srctl clone --source dev --target prod

  # Clone with multi-threading
  srctl clone --source dev --target prod --workers 50

  # Clone specific subjects
  srctl clone --source dev --target prod --subjects user-events,order-events

  # Clone to different context
  srctl clone --source dev --target prod --target-context .staging

  # Clone WITHOUT preserving schema IDs (new IDs will be assigned)
  srctl clone --source dev --target prod --no-preserve-ids

  # Clone with all configs
  srctl clone --source dev --target prod --configs

  # Dry run
  srctl clone --source dev --target prod --dry-run

  # Clone with filter
  srctl clone --source dev --target prod --filter "user-*"`,
	RunE: runClone,
}

var (
	cloneSource        string
	cloneTarget        string
	cloneSubjects      []string
	cloneFilter        string
	cloneDryRun        bool
	cloneSourceContext string
	cloneTargetContext string
	cloneSkipExisting  bool
	cloneWorkers       int
	cloneNoPreserveIDs bool
	cloneConfigs       bool
	cloneTags          bool
)

func init() {
	cloneCmd.Flags().StringVar(&cloneSource, "source", "", "Source registry name (required)")
	cloneCmd.Flags().StringVar(&cloneTarget, "target", "", "Target registry name (required)")
	cloneCmd.Flags().StringSliceVar(&cloneSubjects, "subjects", nil, "Clone only specific subjects")
	cloneCmd.Flags().StringVarP(&cloneFilter, "filter", "f", "", "Filter subjects by pattern")
	cloneCmd.Flags().BoolVar(&cloneDryRun, "dry-run", false, "Preview clone without making changes")
	cloneCmd.Flags().StringVar(&cloneSourceContext, "source-context", "", "Source context")
	cloneCmd.Flags().StringVar(&cloneTargetContext, "target-context", "", "Target context")
	cloneCmd.Flags().BoolVar(&cloneSkipExisting, "skip-existing", false, "Skip subjects that already exist in target")
	cloneCmd.Flags().IntVar(&cloneWorkers, "workers", 10, "Number of parallel workers for cloning")
	cloneCmd.Flags().BoolVar(&cloneNoPreserveIDs, "no-preserve-ids", false, "Do NOT preserve schema IDs (new IDs will be assigned)")
	cloneCmd.Flags().BoolVar(&cloneConfigs, "configs", true, "Clone subject-level configurations")
	cloneCmd.Flags().BoolVar(&cloneTags, "tags", false, "Clone tag definitions and associations")

	cloneCmd.MarkFlagRequired("source")
	cloneCmd.MarkFlagRequired("target")

	rootCmd.AddCommand(cloneCmd)
}

func runClone(cmd *cobra.Command, args []string) error {
	output.Header("Clone Schemas")
	output.Info("Source: %s", cloneSource)
	output.Info("Target: %s", cloneTarget)
	output.Info("Workers: %d", cloneWorkers)

	// Get clients
	sourceClient, err := GetClientForRegistry(cloneSource)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	targetClient, err := GetClientForRegistry(cloneTarget)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	// Apply contexts
	if cloneSourceContext != "" {
		sourceClient = sourceClient.WithContext(cloneSourceContext)
		output.Info("Source context: %s", cloneSourceContext)
	}
	if cloneTargetContext != "" {
		targetClient = targetClient.WithContext(cloneTargetContext)
		output.Info("Target context: %s", cloneTargetContext)
	}

	// Set IMPORT mode if preserving IDs
	if !cloneNoPreserveIDs {
		output.Step("Setting target registry to IMPORT mode...")
		if err := targetClient.SetMode("IMPORT"); err != nil {
			return fmt.Errorf("failed to set IMPORT mode (required for --preserve-ids): %w", err)
		}
		defer func() {
			output.Step("Restoring READWRITE mode...")
			targetClient.SetMode("READWRITE")
		}()
	}

	// Get subjects to clone
	output.Step("Fetching subjects from source...")
	subjects, err := sourceClient.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get source subjects: %w", err)
	}

	// Filter subjects
	if len(cloneSubjects) > 0 {
		subjects = filterByList(subjects, cloneSubjects)
	}
	if cloneFilter != "" {
		subjects = filterSubjects(subjects, cloneFilter)
	}

	if len(subjects) == 0 {
		output.Warning("No subjects to clone")
		return nil
	}

	output.Info("Found %d subjects to clone", len(subjects))

	// Get existing subjects in target if skip-existing
	var existingTarget map[string]bool
	if cloneSkipExisting {
		targetSubjects, _ := targetClient.GetSubjects(false)
		existingTarget = make(map[string]bool)
		for _, s := range targetSubjects {
			existingTarget[s] = true
		}
	}

	// Collect schemas with dependencies using parallel fetching
	output.Step("Collecting schemas and dependencies (%d workers)...", cloneWorkers)
	toClone, refsNeeded := collectSchemasParallel(sourceClient, subjects, existingTarget)

	// Add referenced schemas that aren't already included
	for key := range refsNeeded {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			continue
		}
		refSubj := parts[0]
		refVer := parts[1]

		// Check if already have it
		found := false
		for _, s := range toClone {
			if s.Subject == refSubj && strconv.Itoa(s.Version) == refVer {
				found = true
				break
			}
		}

		if !found {
			schema, err := sourceClient.GetSchema(refSubj, refVer)
			if err != nil {
				output.Warning("Could not fetch reference %s: %v", key, err)
				continue
			}

			schemaType := schema.SchemaType
			if schemaType == "" {
				schemaType = "AVRO"
			}

			version, _ := strconv.Atoi(refVer)
			toClone = append(toClone, schemaToClone{
				Subject:    refSubj,
				Version:    version,
				SchemaID:   schema.ID,
				SchemaType: schemaType,
				Schema:     schema.Schema,
				References: schema.References,
			})
		}
	}

	output.Info("Total schemas to clone: %d", len(toClone))

	// Dry run
	if cloneDryRun {
		output.Header("Dry Run - Would Clone")

		// Group by subject
		subjCount := make(map[string]int)
		for _, s := range toClone {
			subjCount[s.Subject]++
		}

		rows := [][]string{}
		for subj, count := range subjCount {
			rows = append(rows, []string{subj, strconv.Itoa(count)})
		}
		output.PrintTable([]string{"Subject", "Versions"}, rows)

		if !cloneNoPreserveIDs {
			output.Info("Schema IDs would be preserved (IMPORT mode)")
		}

		return nil
	}

	// Perform clone in parallel
	output.Step("Cloning schemas (%d workers)...", cloneWorkers)
	cloned, skipped, failed := cloneSchemasParallel(targetClient, toClone)

	// Clone tags if enabled
	var tagsCloned int
	if cloneTags && !cloneDryRun {
		output.Step("Cloning tags...")
		tagsCloned = cloneTagsData(sourceClient, targetClient, subjects)
	}

	output.Header("Clone Complete")
	rows := [][]string{
		{"Cloned", strconv.Itoa(cloned)},
		{"Skipped (exists)", strconv.Itoa(skipped)},
		{"Failed", strconv.Itoa(failed)},
	}
	if cloneTags {
		rows = append(rows, []string{"Tags Cloned", strconv.Itoa(tagsCloned)})
	}
	output.PrintTable([]string{"Status", "Count"}, rows)

	if !cloneNoPreserveIDs {
		output.Info("Schema IDs preserved via IMPORT mode")
	}

	return nil
}

// cloneTagsData clones tag definitions and assignments between registries
func cloneTagsData(source, target *client.SchemaRegistryClient, subjects []string) int {
	cloned := 0

	// Clone tag definitions first
	sourceTags, err := source.GetTags()
	if err != nil {
		output.Warning("Failed to get source tags: %v", err)
		return 0
	}

	for _, tag := range sourceTags {
		err := target.CreateTag(&tag)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			output.Warning("Failed to create tag %s: %v", tag.Name, err)
		}
	}

	// Clone tag assignments for each subject
	for _, subj := range subjects {
		// Subject-level tags
		subjectTags, err := source.GetSubjectTags(subj)
		if err == nil {
			for _, t := range subjectTags {
				err := target.AssignTagToSubject(subj, t.TypeName)
				if err == nil {
					cloned++
				}
			}
		}

		// Schema-level tags
		versions, err := source.GetVersions(subj, false)
		if err != nil {
			continue
		}

		for _, v := range versions {
			schemaTags, err := source.GetSchemaTags(subj, v)
			if err == nil {
				for _, t := range schemaTags {
					err := target.AssignTagToSchema(subj, v, t.TypeName)
					if err == nil {
						cloned++
					}
				}
			}
		}
	}

	return cloned
}

// schemaToClone holds schema data for cloning
type schemaToClone struct {
	Subject     string
	Version     int
	SchemaID    int
	SchemaType  string
	Schema      string
	References  []client.SchemaReference
	ConfigLevel string
	Mode        string
}

// collectSchemasParallel collects schemas from source in parallel
func collectSchemasParallel(
	sourceClient *client.SchemaRegistryClient,
	subjects []string,
	existingTarget map[string]bool,
) ([]schemaToClone, map[string]bool) {
	type collectResult struct {
		Schemas []schemaToClone
		Refs    map[string]bool
	}

	jobs := make(chan string, len(subjects))
	results := make(chan collectResult, len(subjects))

	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Collecting"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < cloneWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := collectResult{Refs: make(map[string]bool)}

				if cloneSkipExisting && existingTarget != nil && existingTarget[subj] {
					bar.Add(1)
					results <- result
					continue
				}

				versions, err := sourceClient.GetVersions(subj, false)
				if err != nil {
					bar.Add(1)
					results <- result
					continue
				}

				// Get subject config if enabled
				var configLevel, mode string
				if cloneConfigs {
					config, _ := sourceClient.GetSubjectConfig(subj, true)
					if config != nil {
						configLevel = config.CompatibilityLevel
						if configLevel == "" {
							configLevel = config.Compatibility
						}
					}
					modeResp, _ := sourceClient.GetSubjectMode(subj, true)
					if modeResp != nil {
						mode = modeResp.Mode
					}
				}

				for _, v := range versions {
					schema, err := sourceClient.GetSchema(subj, strconv.Itoa(v))
					if err != nil {
						continue
					}

					schemaType := schema.SchemaType
					if schemaType == "" {
						schemaType = "AVRO"
					}

					result.Schemas = append(result.Schemas, schemaToClone{
						Subject:     subj,
						Version:     v,
						SchemaID:    schema.ID,
						SchemaType:  schemaType,
						Schema:      schema.Schema,
						References:  schema.References,
						ConfigLevel: configLevel,
						Mode:        mode,
					})

					// Track references
					for _, ref := range schema.References {
						key := fmt.Sprintf("%s:%d", ref.Subject, ref.Version)
						result.Refs[key] = true
					}
				}

				bar.Add(1)
				results <- result
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
	var allSchemas []schemaToClone
	allRefs := make(map[string]bool)

	for r := range results {
		allSchemas = append(allSchemas, r.Schemas...)
		for k, v := range r.Refs {
			allRefs[k] = v
		}
	}
	bar.Finish()

	return allSchemas, allRefs
}

// cloneSchemasParallel clones schemas to target in parallel
func cloneSchemasParallel(targetClient *client.SchemaRegistryClient, schemas []schemaToClone) (cloned, skipped, failed int) {
	// We need to clone schemas in order (references first)
	// For simplicity, we'll process in batches by subject

	// Group by subject
	bySubject := make(map[string][]schemaToClone)
	for _, s := range schemas {
		bySubject[s.Subject] = append(bySubject[s.Subject], s)
	}

	// Get subject list
	var subjects []string
	for subj := range bySubject {
		subjects = append(subjects, subj)
	}

	jobs := make(chan string, len(subjects))

	var clonedCount, skippedCount, failedCount int64
	configsSet := sync.Map{}

	bar := progressbar.NewOptions(len(schemas),
		progressbar.OptionSetDescription("Cloning"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < cloneWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				schemasForSubj := bySubject[subj]

				// Set subject config if not already set (only first schema has it)
				if len(schemasForSubj) > 0 && schemasForSubj[0].ConfigLevel != "" {
					if _, loaded := configsSet.LoadOrStore(subj, true); !loaded {
						targetClient.SetSubjectConfig(subj, schemasForSubj[0].ConfigLevel)
					}
				}

				// Set subject mode: when preserving IDs, force IMPORT mode at subject level.
				// Confluent Cloud requires subject-level IMPORT mode in addition to the
				// global IMPORT mode. Without this, GetSubjectMode(defaultToGlobal=true)
				// returns the source's READWRITE mode which overrides the target's global
				// IMPORT mode, causing "Subject X is not in import mode" errors.
				if !cloneNoPreserveIDs {
					targetClient.SetSubjectMode(subj, "IMPORT")
				} else if len(schemasForSubj) > 0 && schemasForSubj[0].Mode != "" {
					targetClient.SetSubjectMode(subj, schemasForSubj[0].Mode)
				}

				// Register schemas in order (by version)
				for _, s := range schemasForSubj {
					schema := &client.Schema{
						Schema:     s.Schema,
						SchemaType: s.SchemaType,
						References: s.References,
					}

					// If preserving IDs, we need to use the register with ID
					// (assuming the target is in IMPORT mode)
					if !cloneNoPreserveIDs {
						schema.ID = s.SchemaID
					}

					_, err := targetClient.RegisterSchema(s.Subject, schema)
					if err != nil {
						if strings.Contains(err.Error(), "already exists") ||
							strings.Contains(err.Error(), "already registered") {
							atomic.AddInt64(&skippedCount, 1)
						} else {
							atomic.AddInt64(&failedCount, 1)
						}
					} else {
						atomic.AddInt64(&clonedCount, 1)
					}
					bar.Add(1)
				}

				// Restore subject mode to READWRITE after registration
				if !cloneNoPreserveIDs {
					targetClient.SetSubjectMode(subj, "READWRITE")
				}
			}
		}()
	}

	// Send jobs
	for _, subj := range subjects {
		jobs <- subj
	}
	close(jobs)
	wg.Wait()
	bar.Finish()

	return int(clonedCount), int(skippedCount), int(failedCount)
}
