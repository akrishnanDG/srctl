package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	danglingWorkers int
	danglingJSON    bool
)

func init() {
	danglingCmd.Flags().IntVar(&danglingWorkers, "workers", 20, "Number of parallel workers")
	danglingCmd.Flags().BoolVar(&danglingJSON, "json", false, "Output in JSON format")

	rootCmd.AddCommand(danglingCmd)
}

var danglingCmd = &cobra.Command{
	Use:     "dangling",
	Short:   "Find schemas with broken references (dangling references)",
	GroupID: groupConfig,
	Long: `Find schemas that reference soft-deleted schemas (dangling references).

This command checks all schema versions for references to other schemas
and reports any references that point to soft-deleted subjects or versions.

A dangling reference occurs when:
  • A parent schema references a child schema that has been soft-deleted
  • A parent schema references a specific version that no longer exists

This is useful for:
  • Identifying referential integrity issues before permanent deletion
  • Finding schemas that may fail to deserialize
  • Cleaning up orphaned references

Examples:
  # Find all dangling references
  srctl dangling

  # Find dangling references with more workers (faster for large registries)
  srctl dangling --workers 50

  # Output as JSON
  srctl dangling --json`,
	RunE: runDangling,
}

// DanglingReference represents a broken reference
type DanglingReference struct {
	ParentSubject  string `json:"parentSubject"`
	ParentVersion  int    `json:"parentVersion"`
	ParentSchemaID int    `json:"parentSchemaId"`
	RefName        string `json:"refName"`
	RefSubject     string `json:"refSubject"`
	RefVersion     int    `json:"refVersion"`
	Reason         string `json:"reason"`
}

// DanglingReport contains the full report
type DanglingReport struct {
	TotalSubjects    int                 `json:"totalSubjects"`
	TotalVersions    int                 `json:"totalVersions"`
	SchemasWithRefs  int                 `json:"schemasWithRefs"`
	DanglingCount    int                 `json:"danglingCount"`
	DanglingRefs     []DanglingReference `json:"danglingReferences"`
	DeletedSubjects  []string            `json:"deletedSubjects"`
	AffectedSubjects int                 `json:"affectedSubjects"`
}

func runDangling(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Dangling Reference Check")

	// Step 1: Get all subjects (including deleted)
	output.Step("Fetching all subjects (including deleted)...")
	allSubjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	// Also get active subjects to identify deleted ones
	activeSubjects, err := c.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get active subjects: %w", err)
	}

	// Build maps
	activeMap := make(map[string]bool)
	for _, s := range activeSubjects {
		activeMap[s] = true
	}

	// Filter out internal subjects
	var userSubjects []string
	var deletedSubjects []string
	for _, s := range allSubjects {
		if isInternalSubject(s) {
			continue
		}
		userSubjects = append(userSubjects, s)
		if !activeMap[s] {
			deletedSubjects = append(deletedSubjects, s)
		}
	}

	output.Info("Total subjects: %d (excluding internal)", len(userSubjects))
	output.Info("Deleted subjects: %d", len(deletedSubjects))

	// Step 2: Analyze schemas for references in parallel
	output.Step("Analyzing schemas for dangling references...")

	report := analyzeDanglingParallel(c, userSubjects, activeMap, danglingWorkers)
	report.DeletedSubjects = deletedSubjects

	// Step 3: Output results
	if danglingJSON {
		printer := output.NewPrinter("json")
		return printer.Print(report)
	}

	// Table output
	fmt.Println()
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Total Subjects Scanned", strconv.Itoa(report.TotalSubjects)},
			{"Total Versions Scanned", strconv.Itoa(report.TotalVersions)},
			{"Schemas with References", strconv.Itoa(report.SchemasWithRefs)},
			{"Dangling References Found", strconv.Itoa(report.DanglingCount)},
			{"Affected Parent Subjects", strconv.Itoa(report.AffectedSubjects)},
		},
	)

	if len(deletedSubjects) > 0 {
		fmt.Println()
		output.Warning("Soft-deleted subjects that may be referenced:")
		for i, s := range deletedSubjects {
			if i >= 10 {
				output.Info("  ... and %d more", len(deletedSubjects)-10)
				break
			}
			output.Info("  • %s", s)
		}
	}

	if len(report.DanglingRefs) > 0 {
		fmt.Println()
		output.Error("Dangling References Found:")
		fmt.Println()

		// Group by parent subject
		byParent := make(map[string][]DanglingReference)
		for _, ref := range report.DanglingRefs {
			key := ref.ParentSubject
			byParent[key] = append(byParent[key], ref)
		}

		// Sort parent subjects
		var parents []string
		for p := range byParent {
			parents = append(parents, p)
		}
		sort.Strings(parents)

		// Print details
		rows := [][]string{}
		for _, parent := range parents {
			refs := byParent[parent]
			for _, ref := range refs {
				rows = append(rows, []string{
					ref.ParentSubject,
					strconv.Itoa(ref.ParentVersion),
					ref.RefSubject,
					strconv.Itoa(ref.RefVersion),
					ref.Reason,
				})
			}
		}

		output.PrintTable(
			[]string{"Parent Subject", "Version", "References", "Ref Version", "Issue"},
			rows,
		)

		fmt.Println()
		output.Warning("These schemas reference soft-deleted schemas and may cause issues.")
		output.Info("Consider updating these schemas or restoring the deleted references.")
	} else {
		fmt.Println()
		output.Success("No dangling references found!")
	}

	return nil
}

type danglingResult struct {
	Subject      string
	VersionCount int
	RefsFound    int
	DanglingRefs []DanglingReference
}

func analyzeDanglingParallel(c *client.SchemaRegistryClient, subjects []string, activeMap map[string]bool, workers int) DanglingReport {
	// Create channels
	jobs := make(chan string, len(subjects))
	resultChan := make(chan danglingResult, len(subjects))

	// Progress bar
	var completed int64
	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Analyzing"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subject := range jobs {
				result := analyzeDanglingSubject(c, subject, activeMap)
				resultChan <- result
				atomic.AddInt64(&completed, 1)
				bar.Add(1)
			}
		}()
	}

	// Send subjects to workers
	go func() {
		for _, subject := range subjects {
			jobs <- subject
		}
		close(jobs)
	}()

	// Wait and collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Aggregate results
	report := DanglingReport{
		DanglingRefs: []DanglingReference{},
	}
	affectedSubjects := make(map[string]bool)

	for result := range resultChan {
		report.TotalSubjects++
		report.TotalVersions += result.VersionCount
		report.SchemasWithRefs += result.RefsFound

		if len(result.DanglingRefs) > 0 {
			report.DanglingRefs = append(report.DanglingRefs, result.DanglingRefs...)
			report.DanglingCount += len(result.DanglingRefs)
			affectedSubjects[result.Subject] = true
		}
	}

	bar.Finish()
	report.AffectedSubjects = len(affectedSubjects)

	// Sort dangling refs by parent subject
	sort.Slice(report.DanglingRefs, func(i, j int) bool {
		if report.DanglingRefs[i].ParentSubject != report.DanglingRefs[j].ParentSubject {
			return report.DanglingRefs[i].ParentSubject < report.DanglingRefs[j].ParentSubject
		}
		return report.DanglingRefs[i].ParentVersion < report.DanglingRefs[j].ParentVersion
	})

	return report
}

func analyzeDanglingSubject(c *client.SchemaRegistryClient, subject string, activeMap map[string]bool) danglingResult {
	result := danglingResult{
		Subject:      subject,
		DanglingRefs: []DanglingReference{},
	}

	// Get all versions (including deleted)
	versions, err := c.GetVersions(subject, true)
	if err != nil {
		return result
	}

	result.VersionCount = len(versions)

	// Check each version for references
	for _, version := range versions {
		schema, err := c.GetSchema(subject, strconv.Itoa(version))
		if err != nil {
			// Try with deleted flag
			schema, err = c.GetSchemaWithDeleted(subject, strconv.Itoa(version), true)
			if err != nil {
				continue
			}
		}

		if len(schema.References) == 0 {
			continue
		}

		result.RefsFound++

		// Check each reference
		for _, ref := range schema.References {
			// Check if referenced subject exists and is active
			if !activeMap[ref.Subject] {
				// Referenced subject is deleted or doesn't exist
				result.DanglingRefs = append(result.DanglingRefs, DanglingReference{
					ParentSubject:  subject,
					ParentVersion:  version,
					ParentSchemaID: schema.ID,
					RefName:        ref.Name,
					RefSubject:     ref.Subject,
					RefVersion:     ref.Version,
					Reason:         "Subject soft-deleted",
				})
				continue
			}

			// Check if referenced version exists
			refVersions, err := c.GetVersions(ref.Subject, false)
			if err != nil {
				continue
			}

			versionExists := false
			for _, v := range refVersions {
				if v == ref.Version {
					versionExists = true
					break
				}
			}

			if !versionExists {
				// Check if version exists but is soft-deleted
				allVersions, _ := c.GetVersions(ref.Subject, true)
				wasDeleted := false
				for _, v := range allVersions {
					if v == ref.Version {
						wasDeleted = true
						break
					}
				}

				reason := "Version not found"
				if wasDeleted {
					reason = "Version soft-deleted"
				}

				result.DanglingRefs = append(result.DanglingRefs, DanglingReference{
					ParentSubject:  subject,
					ParentVersion:  version,
					ParentSchemaID: schema.ID,
					RefName:        ref.Name,
					RefSubject:     ref.Subject,
					RefVersion:     ref.Version,
					Reason:         reason,
				})
			}
		}
	}

	return result
}
