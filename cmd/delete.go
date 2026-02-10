package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	deleteForce        bool
	deletePermanent    bool
	deleteKeepLatest   int
	deletePurgeSoftDel bool
	deleteYes          bool
	deleteWorkers      int
	deleteSubjects     []string
	deleteSkipRefCheck bool
)

var deleteCmd = &cobra.Command{
	Use:     "delete [subject] [version]",
	Short:   "Delete schemas from the registry",
	GroupID: groupSchema,
	Long: `Delete schemas from the Schema Registry with various options.

Basic delete operations:
  • Delete a specific version (soft delete)
  • Delete an entire subject (soft delete)
  • Permanent delete (requires --permanent)

Advanced delete operations with --force:
  • Delete an entire context
  • Delete an entire subject with all versions
  • Empty the entire Schema Registry
  • Delete specific version permanently (--force with version)
  • Delete and keep latest N permanently (--force --keep-latest)

Bulk delete operations:
  • Delete multiple subjects (--subjects)
  • Multi-threaded deletion (--workers)

Keep latest N versions:
  • Delete all versions except the latest N (--keep-latest)

Purge soft-deleted schemas:
  • Remove all soft-deleted schemas permanently (--purge-soft-deleted)

Examples:
  # Soft delete a specific version
  srctl delete user-events 3

  # Soft delete entire subject
  srctl delete user-events

  # Permanent delete
  srctl delete user-events --permanent

  # Force delete specific version (soft + hard delete)
  srctl delete user-events 3 --force

  # Force delete entire subject (soft + hard delete)
  srctl delete user-events --force

  # Delete multiple subjects with multi-threading
  srctl delete --subjects user-events,order-events --workers 10

  # Force delete entire context with multi-threading
  srctl delete --context .mycontext --force --workers 20

  # Empty entire registry with multi-threading (DANGEROUS!)
  srctl delete --force --all --workers 50

  # Keep only latest 3 versions (soft delete)
  srctl delete user-events --keep-latest 3

  # Keep only latest 3 versions (permanent delete)
  srctl delete user-events --keep-latest 3 --force

  # Purge all soft-deleted schemas with multi-threading
  srctl delete --purge-soft-deleted --workers 20

  # Purge soft-deleted in specific context
  srctl delete --context .mycontext --purge-soft-deleted

  # Purge soft-deleted for specific subject
  srctl delete user-events --purge-soft-deleted`,
	RunE: runDelete,
}

var deleteAll bool

func init() {
	deleteCmd.Flags().BoolVar(&deleteForce, "force", false, "Force delete (bypasses soft delete, permanently deletes)")
	deleteCmd.Flags().BoolVar(&deletePermanent, "permanent", false, "Permanent delete (hard delete)")
	deleteCmd.Flags().IntVar(&deleteKeepLatest, "keep-latest", 0, "Keep only the latest N versions, delete the rest")
	deleteCmd.Flags().BoolVar(&deletePurgeSoftDel, "purge-soft-deleted", false, "Purge all soft-deleted schemas")
	deleteCmd.Flags().BoolVarP(&deleteYes, "yes", "y", false, "Skip confirmation prompts")
	deleteCmd.Flags().BoolVar(&deleteAll, "all", false, "Delete all subjects in registry (requires --force)")
	deleteCmd.Flags().IntVar(&deleteWorkers, "workers", 10, "Number of parallel workers for bulk operations")
	deleteCmd.Flags().StringSliceVar(&deleteSubjects, "subjects", nil, "Delete specific subjects (comma-separated)")
	deleteCmd.Flags().BoolVar(&deleteSkipRefCheck, "skip-ref-check", false, "Skip referential integrity check (not recommended)")

	rootCmd.AddCommand(deleteCmd)
}

func runDelete(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	// Handle purge soft-deleted schemas
	if deletePurgeSoftDel {
		return purgeSoftDeleted(c, args)
	}

	// Handle keep latest N versions
	if deleteKeepLatest > 0 {
		if len(args) == 0 && len(deleteSubjects) == 0 {
			return fmt.Errorf("subject name required for --keep-latest")
		}
		if len(deleteSubjects) > 0 {
			return keepLatestVersionsMulti(c, deleteSubjects, deleteKeepLatest)
		}
		return keepLatestVersions(c, args[0], deleteKeepLatest)
	}

	// Handle force delete entire registry
	if deleteAll && deleteForce {
		return forceDeleteAllParallel(c)
	}

	// Handle force delete context
	if deleteForce && context != "" && len(args) == 0 && len(deleteSubjects) == 0 {
		return forceDeleteContextParallel(c, context)
	}

	// Handle bulk delete multiple subjects
	if len(deleteSubjects) > 0 {
		return deleteSubjectsParallel(c, deleteSubjects)
	}

	// Handle force delete subject with specific version
	if deleteForce && len(args) > 1 {
		return forceDeleteVersion(c, args[0], args[1])
	}

	// Handle force delete subject
	if deleteForce && len(args) > 0 {
		return forceDeleteSubject(c, args[0])
	}

	// Regular delete operations
	if len(args) == 0 {
		return fmt.Errorf("subject name required (or use --all --force to delete everything, or --subjects for bulk delete)")
	}

	subject := args[0]

	// Delete specific version
	if len(args) > 1 {
		version := args[1]
		return deleteVersion(c, subject, version)
	}

	// Delete entire subject
	return deleteSubject(c, subject)
}

func deleteVersion(c *client.SchemaRegistryClient, subject, version string) error {
	output.Step("Deleting version %s of subject: %s", version, subject)

	// Check referential integrity
	v, _ := strconv.Atoi(version)
	refs, err := checkReferentialIntegrity(c, subject, v)
	if err == nil && len(refs) > 0 {
		output.Error("Cannot delete: version %s is referenced by %d other schema(s)", version, len(refs))
		output.Info("Referenced by schema IDs: %v", refs)
		output.Info("Use --skip-ref-check to bypass this check (not recommended)")
		return fmt.Errorf("referential integrity violation")
	}

	if !deleteYes && !confirmAction(fmt.Sprintf("Delete version %s of %s?", version, subject)) {
		output.Info("Cancelled")
		return nil
	}

	deletedVersion, err := c.DeleteVersion(subject, version, deletePermanent)
	if err != nil {
		return fmt.Errorf("failed to delete version: %w", err)
	}

	if deletePermanent {
		output.Success("Permanently deleted version %d", deletedVersion)
	} else {
		output.Success("Soft deleted version %d", deletedVersion)
	}

	return nil
}

func deleteSubject(c *client.SchemaRegistryClient, subject string) error {
	output.Step("Deleting subject: %s", subject)

	// Check referential integrity for all versions
	refsByVersion, err := checkSubjectReferentialIntegrity(c, subject)
	if err == nil && len(refsByVersion) > 0 {
		output.Error("Cannot delete: subject has versions referenced by other schemas")
		for v, refs := range refsByVersion {
			output.Info("  Version %d is referenced by schema IDs: %v", v, refs)
		}
		output.Info("Use --skip-ref-check to bypass this check (not recommended)")
		return fmt.Errorf("referential integrity violation")
	}

	if !deleteYes && !confirmAction(fmt.Sprintf("Delete subject %s?", subject)) {
		output.Info("Cancelled")
		return nil
	}

	versions, err := c.DeleteSubject(subject, deletePermanent)
	if err != nil {
		return fmt.Errorf("failed to delete subject: %w", err)
	}

	if deletePermanent {
		output.Success("Permanently deleted subject with %d versions", len(versions))
	} else {
		output.Success("Soft deleted subject with %d versions", len(versions))
	}

	return nil
}

func forceDeleteSubject(c interface {
	GetVersions(string, bool) ([]int, error)
	DeleteVersion(string, string, bool) (int, error)
	DeleteSubject(string, bool) ([]int, error)
}, subject string) error {
	output.Header("Force Delete Subject: %s", subject)

	if !deleteYes && !confirmAction(fmt.Sprintf("PERMANENTLY delete ALL versions of %s? This cannot be undone!", subject)) {
		output.Info("Cancelled")
		return nil
	}

	// Step 1: Get all versions (including deleted)
	output.Step("Step 1/3: Fetching all versions...")
	versions, err := c.GetVersions(subject, true)
	if err != nil {
		return fmt.Errorf("failed to get versions: %w", err)
	}
	output.Info("Found %d versions", len(versions))

	// Step 2: Soft delete the subject if not already
	output.Step("Step 2/3: Soft deleting subject...")
	_, err = c.DeleteSubject(subject, false)
	if err != nil && !strings.Contains(err.Error(), "already deleted") {
		// May already be soft deleted, continue
		output.Warning("Soft delete warning: %v", err)
	}
	output.Success("Subject soft deleted")

	// Step 3: Hard delete the subject
	output.Step("Step 3/3: Permanently deleting subject...")
	deletedVersions, err := c.DeleteSubject(subject, true)
	if err != nil {
		return fmt.Errorf("failed to permanently delete: %w", err)
	}

	output.Success("Permanently deleted subject %s with %d versions", subject, len(deletedVersions))
	return nil
}

func forceDeleteContext(c interface {
	GetSubjects(bool) ([]string, error)
	GetVersions(string, bool) ([]int, error)
	DeleteSubject(string, bool) ([]int, error)
}, ctx string) error {
	output.Header("Force Delete Context: %s", ctx)
	output.Warning("This will PERMANENTLY delete ALL subjects and schemas in context '%s'!", ctx)

	if !deleteYes && !confirmAction("Are you absolutely sure? This cannot be undone!") {
		output.Info("Cancelled")
		return nil
	}

	// Get all subjects in context
	output.Step("Step 1/4: Fetching all subjects...")
	subjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}
	output.Info("Found %d subjects", len(subjects))

	if len(subjects) == 0 {
		output.Success("Context is already empty")
		return nil
	}

	// Create progress bar
	bar := progressbar.NewOptions(len(subjects)*2, // x2 for soft + hard delete
		progressbar.OptionSetDescription("Deleting subjects"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	var totalVersions int

	// Step 2: Soft delete all subjects
	output.Step("Step 2/4: Soft deleting all subjects...")
	for _, subj := range subjects {
		_, _ = c.DeleteSubject(subj, false)
		bar.Add(1)
	}

	// Step 3: Hard delete all subjects
	output.Step("Step 3/4: Permanently deleting all subjects...")
	for _, subj := range subjects {
		versions, err := c.DeleteSubject(subj, true)
		if err != nil {
			output.Warning("Failed to delete %s: %v", subj, err)
		} else {
			totalVersions += len(versions)
		}
		bar.Add(1)
	}

	bar.Finish()

	// Step 4: Summary
	output.Step("Step 4/4: Cleanup complete")
	output.Success("Deleted %d subjects with %d total versions from context '%s'", len(subjects), totalVersions, ctx)

	return nil
}

func forceDeleteAll(c interface {
	GetContexts() ([]string, error)
	GetSubjects(bool) ([]string, error)
	DeleteSubject(string, bool) ([]int, error)
}) error {
	output.Header("⚠️  DANGER: Empty Entire Schema Registry")
	output.Error("This will PERMANENTLY delete ALL schemas across ALL contexts!")

	if !deleteYes {
		fmt.Print("\nType 'DELETE EVERYTHING' to confirm: ")
		reader := bufio.NewReader(os.Stdin)
		confirmation, _ := reader.ReadString('\n')
		if strings.TrimSpace(confirmation) != "DELETE EVERYTHING" {
			output.Info("Cancelled - confirmation text did not match")
			return nil
		}
	}

	// Get all contexts
	output.Step("Step 1/5: Fetching all contexts...")
	contexts, err := c.GetContexts()
	if err != nil {
		// If contexts API not available, use default context
		contexts = []string{"."}
	}
	output.Info("Found %d contexts", len(contexts))

	// Get all subjects
	output.Step("Step 2/5: Fetching all subjects...")
	subjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}
	output.Info("Found %d subjects", len(subjects))

	if len(subjects) == 0 {
		output.Success("Schema Registry is already empty")
		return nil
	}

	bar := progressbar.NewOptions(len(subjects)*2,
		progressbar.OptionSetDescription("Deleting all schemas"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	var totalVersions int

	// Soft delete all
	output.Step("Step 3/5: Soft deleting all subjects...")
	for _, subj := range subjects {
		_, _ = c.DeleteSubject(subj, false)
		bar.Add(1)
	}

	// Hard delete all
	output.Step("Step 4/5: Permanently deleting all subjects...")
	for _, subj := range subjects {
		versions, err := c.DeleteSubject(subj, true)
		if err == nil {
			totalVersions += len(versions)
		}
		bar.Add(1)
	}

	bar.Finish()

	// Summary
	output.Step("Step 5/5: Complete")
	output.Success("Deleted %d subjects with %d total versions", len(subjects), totalVersions)

	return nil
}

func keepLatestVersions(c interface {
	GetVersions(string, bool) ([]int, error)
	DeleteVersion(string, string, bool) (int, error)
}, subject string, keepN int) error {
	output.Header("Keep Latest %d Versions: %s", keepN, subject)

	// Get all versions
	versions, err := c.GetVersions(subject, false)
	if err != nil {
		return fmt.Errorf("failed to get versions: %w", err)
	}

	if len(versions) <= keepN {
		output.Info("Subject has %d versions, nothing to delete (keeping %d)", len(versions), keepN)
		return nil
	}

	// Sort versions and identify ones to delete
	toDelete := versions[:len(versions)-keepN]
	toKeep := versions[len(versions)-keepN:]

	permanentDelete := deletePermanent || deleteForce
	deleteType := "soft"
	if permanentDelete {
		deleteType = "permanently"
	}

	output.Info("Current versions: %v", versions)
	output.Info("Will %s delete: %v", deleteType, toDelete)
	output.Info("Will keep: %v", toKeep)

	if !deleteYes && !confirmAction(fmt.Sprintf("%s delete %d versions?", strings.Title(deleteType), len(toDelete))) {
		output.Info("Cancelled")
		return nil
	}

	bar := progressbar.NewOptions(len(toDelete),
		progressbar.OptionSetDescription("Deleting old versions"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	var deleted, failed int
	for _, v := range toDelete {
		// Soft delete first
		_, err := c.DeleteVersion(subject, strconv.Itoa(v), false)
		if err != nil && !strings.Contains(err.Error(), "already deleted") {
			failed++
			bar.Add(1)
			continue
		}

		// Then hard delete if permanent or force flag is set
		if permanentDelete {
			_, err = c.DeleteVersion(subject, strconv.Itoa(v), true)
			if err != nil {
				failed++
				bar.Add(1)
				continue
			}
		}

		deleted++
		bar.Add(1)
	}

	bar.Finish()

	if permanentDelete {
		output.Success("Permanently deleted %d versions (failed: %d)", deleted, failed)
	} else {
		output.Success("Soft deleted %d versions (failed: %d)", deleted, failed)
	}
	output.Info("Remaining versions: %v", toKeep)

	return nil
}

func purgeSoftDeleted(c interface {
	GetSubjects(bool) ([]string, error)
	GetVersions(string, bool) ([]int, error)
	DeleteSubject(string, bool) ([]int, error)
	DeleteVersion(string, string, bool) (int, error)
}, args []string) error {
	var subject string
	if len(args) > 0 {
		subject = args[0]
	}

	if subject != "" {
		// Purge soft-deleted for specific subject
		return purgeSoftDeletedSubject(c, subject)
	}

	// Purge all soft-deleted schemas
	return purgeSoftDeletedAll(c)
}

func purgeSoftDeletedSubject(c interface {
	GetVersions(string, bool) ([]int, error)
	DeleteVersion(string, string, bool) (int, error)
	DeleteSubject(string, bool) ([]int, error)
}, subject string) error {
	output.Header("Purge Soft-Deleted: %s", subject)

	// Get all versions including deleted
	allVersions, err := c.GetVersions(subject, true)
	if err != nil {
		return fmt.Errorf("failed to get versions: %w", err)
	}

	// Get active versions
	activeVersions, err := c.GetVersions(subject, false)
	if err != nil {
		// Subject might be entirely soft-deleted
		activeVersions = []int{}
	}

	// Find soft-deleted versions
	activeSet := make(map[int]bool)
	for _, v := range activeVersions {
		activeSet[v] = true
	}

	var softDeleted []int
	for _, v := range allVersions {
		if !activeSet[v] {
			softDeleted = append(softDeleted, v)
		}
	}

	if len(softDeleted) == 0 {
		output.Info("No soft-deleted versions found")
		return nil
	}

	output.Info("Found %d soft-deleted versions: %v", len(softDeleted), softDeleted)

	if !deleteYes && !confirmAction("Permanently delete these versions?") {
		output.Info("Cancelled")
		return nil
	}

	bar := progressbar.NewOptions(len(softDeleted),
		progressbar.OptionSetDescription("Purging soft-deleted"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	var purged, failed int
	for _, v := range softDeleted {
		_, err := c.DeleteVersion(subject, strconv.Itoa(v), true)
		if err != nil {
			failed++
		} else {
			purged++
		}
		bar.Add(1)
	}

	bar.Finish()

	output.Success("Purged %d soft-deleted versions (failed: %d)", purged, failed)
	return nil
}

func purgeSoftDeletedAll(c interface {
	GetSubjects(bool) ([]string, error)
	GetVersions(string, bool) ([]int, error)
	DeleteSubject(string, bool) ([]int, error)
	DeleteVersion(string, string, bool) (int, error)
}) error {
	output.Header("Purge All Soft-Deleted Schemas")
	if context != "" {
		output.Info("Context: %s", context)
	}

	// Get all subjects including deleted
	output.Step("Step 1/3: Scanning for soft-deleted schemas...")
	allSubjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	activeSubjects, err := c.GetSubjects(false)
	if err != nil {
		activeSubjects = []string{}
	}

	activeSet := make(map[string]bool)
	for _, s := range activeSubjects {
		activeSet[s] = true
	}

	// Find soft-deleted subjects
	var softDeletedSubjects []string
	for _, s := range allSubjects {
		if !activeSet[s] {
			softDeletedSubjects = append(softDeletedSubjects, s)
		}
	}

	// Find soft-deleted versions in active subjects
	type versionToPurge struct {
		subject string
		version int
	}
	var versionsToPurge []versionToPurge

	for _, subj := range activeSubjects {
		allVersions, err := c.GetVersions(subj, true)
		if err != nil {
			continue
		}
		activeVersions, err := c.GetVersions(subj, false)
		if err != nil {
			activeVersions = []int{}
		}

		activeVerSet := make(map[int]bool)
		for _, v := range activeVersions {
			activeVerSet[v] = true
		}

		for _, v := range allVersions {
			if !activeVerSet[v] {
				versionsToPurge = append(versionsToPurge, versionToPurge{subj, v})
			}
		}
	}

	totalToPurge := len(softDeletedSubjects) + len(versionsToPurge)
	if totalToPurge == 0 {
		output.Success("No soft-deleted schemas found")
		return nil
	}

	output.Info("Found %d soft-deleted subjects", len(softDeletedSubjects))
	output.Info("Found %d soft-deleted versions in active subjects", len(versionsToPurge))

	if !deleteYes && !confirmAction(fmt.Sprintf("Permanently purge %d items?", totalToPurge)) {
		output.Info("Cancelled")
		return nil
	}

	// Step 2: Purge soft-deleted subjects
	output.Step("Step 2/3: Purging soft-deleted subjects...")
	var purgedSubjects, purgedVersions, failedSubjects, failedVersions int

	if len(softDeletedSubjects) > 0 {
		bar := progressbar.NewOptions(len(softDeletedSubjects),
			progressbar.OptionSetDescription("Purging subjects"),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(40),
			progressbar.OptionClearOnFinish(),
		)

		for _, subj := range softDeletedSubjects {
			_, err := c.DeleteSubject(subj, true)
			if err != nil {
				failedSubjects++
			} else {
				purgedSubjects++
			}
			bar.Add(1)
		}
		bar.Finish()
	}

	// Step 3: Purge soft-deleted versions
	output.Step("Step 3/3: Purging soft-deleted versions...")
	if len(versionsToPurge) > 0 {
		bar := progressbar.NewOptions(len(versionsToPurge),
			progressbar.OptionSetDescription("Purging versions"),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(40),
			progressbar.OptionClearOnFinish(),
		)

		for _, vp := range versionsToPurge {
			_, err := c.DeleteVersion(vp.subject, strconv.Itoa(vp.version), true)
			if err != nil {
				failedVersions++
			} else {
				purgedVersions++
			}
			bar.Add(1)
		}
		bar.Finish()
	}

	// Summary
	output.Header("Purge Complete")
	output.PrintTable(
		[]string{"Type", "Purged", "Failed"},
		[][]string{
			{"Subjects", strconv.Itoa(purgedSubjects), strconv.Itoa(failedSubjects)},
			{"Versions", strconv.Itoa(purgedVersions), strconv.Itoa(failedVersions)},
		},
	)

	return nil
}

func confirmAction(prompt string) bool {
	fmt.Printf("\n%s [y/N]: ", prompt)
	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}

// checkReferentialIntegrity checks if any schemas reference the given subject/version
// Returns list of referencing schema IDs and any error
func checkReferentialIntegrity(c *client.SchemaRegistryClient, subject string, version int) ([]int, error) {
	if deleteSkipRefCheck {
		return nil, nil
	}

	refs, err := c.GetSchemaReferencedBy(subject, version)
	if err != nil {
		// If API not available, return no refs (older SR versions may not support this)
		return nil, nil
	}

	return refs, nil
}

// checkSubjectReferentialIntegrity checks all versions of a subject for references
func checkSubjectReferentialIntegrity(c *client.SchemaRegistryClient, subject string) (map[int][]int, error) {
	if deleteSkipRefCheck {
		return nil, nil
	}

	versions, err := c.GetVersions(subject, true)
	if err != nil {
		return nil, err
	}

	refsByVersion := make(map[int][]int)
	for _, v := range versions {
		refs, err := checkReferentialIntegrity(c, subject, v)
		if err != nil {
			continue
		}
		if len(refs) > 0 {
			refsByVersion[v] = refs
		}
	}

	return refsByVersion, nil
}

// forceDeleteVersion force deletes a specific version (soft + hard delete)
func forceDeleteVersion(c *client.SchemaRegistryClient, subject, version string) error {
	output.Header("Force Delete Version: %s v%s", subject, version)

	if !deleteYes && !confirmAction(fmt.Sprintf("PERMANENTLY delete version %s of %s? This cannot be undone!", version, subject)) {
		output.Info("Cancelled")
		return nil
	}

	// Step 1: Soft delete
	output.Step("Step 1/2: Soft deleting version...")
	_, err := c.DeleteVersion(subject, version, false)
	if err != nil && !strings.Contains(err.Error(), "already deleted") {
		output.Warning("Soft delete warning: %v", err)
	}

	// Step 2: Hard delete
	output.Step("Step 2/2: Permanently deleting version...")
	deletedVersion, err := c.DeleteVersion(subject, version, true)
	if err != nil {
		return fmt.Errorf("failed to permanently delete: %w", err)
	}

	output.Success("Permanently deleted version %d of %s", deletedVersion, subject)
	return nil
}

// deleteSubjectsParallel deletes multiple subjects in parallel
func deleteSubjectsParallel(c *client.SchemaRegistryClient, subjects []string) error {
	output.Header("Bulk Delete Subjects")
	output.Info("Subjects to delete: %d", len(subjects))

	if !deleteYes && !confirmAction(fmt.Sprintf("Delete %d subjects?", len(subjects))) {
		output.Info("Cancelled")
		return nil
	}

	type deleteResult struct {
		Subject  string
		Versions int
		Error    error
	}

	jobs := make(chan string, len(subjects))
	results := make(chan deleteResult, len(subjects))

	// Create progress bar
	totalOps := len(subjects)
	if deleteForce || deletePermanent {
		totalOps *= 2 // soft + hard delete
	}
	bar := progressbar.NewOptions(totalOps,
		progressbar.OptionSetDescription("Deleting"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < deleteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := deleteResult{Subject: subj}

				// Soft delete
				versions, err := c.DeleteSubject(subj, false)
				bar.Add(1)

				if err != nil && !strings.Contains(err.Error(), "already deleted") {
					result.Error = err
					results <- result
					if deleteForce || deletePermanent {
						bar.Add(1) // Skip hard delete progress
					}
					continue
				}

				result.Versions = len(versions)

				// Hard delete if force or permanent
				if deleteForce || deletePermanent {
					_, err = c.DeleteSubject(subj, true)
					bar.Add(1)
					if err != nil {
						result.Error = err
					}
				}

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
	var deleted, failed int
	var totalVersions int
	var errors []string

	for r := range results {
		if r.Error != nil {
			failed++
			errors = append(errors, fmt.Sprintf("%s: %v", r.Subject, r.Error))
		} else {
			deleted++
			totalVersions += r.Versions
		}
	}
	bar.Finish()

	// Summary
	output.Header("Delete Complete")
	deleteType := "Soft"
	if deleteForce || deletePermanent {
		deleteType = "Permanent"
	}
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Delete Type", deleteType},
			{"Subjects Deleted", strconv.Itoa(deleted)},
			{"Total Versions", strconv.Itoa(totalVersions)},
			{"Failed", strconv.Itoa(failed)},
		},
	)

	if len(errors) > 0 && len(errors) <= 10 {
		output.SubHeader("Errors")
		for _, e := range errors {
			output.Error("  %s", e)
		}
	} else if len(errors) > 10 {
		output.Error("Too many errors to display (%d total)", len(errors))
	}

	return nil
}

// forceDeleteContextParallel force deletes entire context with parallel workers
func forceDeleteContextParallel(c *client.SchemaRegistryClient, ctx string) error {
	output.Header("Force Delete Context: %s", ctx)
	output.Warning("This will PERMANENTLY delete ALL subjects and schemas in context '%s'!", ctx)

	if !deleteYes && !confirmAction("Are you absolutely sure? This cannot be undone!") {
		output.Info("Cancelled")
		return nil
	}

	// Get all subjects in context
	output.Step("Step 1/4: Fetching all subjects...")
	subjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}
	output.Info("Found %d subjects", len(subjects))

	if len(subjects) == 0 {
		output.Success("Context is already empty")
		return nil
	}

	type deleteResult struct {
		Subject  string
		Versions int
		Phase    string // "soft" or "hard"
		Error    error
	}

	// Phase 1: Soft delete all subjects in parallel
	output.Step("Step 2/4: Soft deleting all subjects...")
	softDeleteParallel(c, subjects)

	// Phase 2: Hard delete all subjects in parallel
	output.Step("Step 3/4: Permanently deleting all subjects...")
	totalVersions, failedCount := hardDeleteParallel(c, subjects)

	// Summary
	output.Step("Step 4/4: Cleanup complete")
	output.Success("Deleted %d subjects with %d total versions from context '%s' (failed: %d)",
		len(subjects)-failedCount, totalVersions, ctx, failedCount)

	return nil
}

// forceDeleteAllParallel force deletes entire registry with parallel workers
func forceDeleteAllParallel(c *client.SchemaRegistryClient) error {
	output.Header("⚠️  DANGER: Empty Entire Schema Registry")
	output.Error("This will PERMANENTLY delete ALL schemas across ALL contexts!")

	if !deleteYes {
		fmt.Print("\nType 'DELETE EVERYTHING' to confirm: ")
		reader := bufio.NewReader(os.Stdin)
		confirmation, _ := reader.ReadString('\n')
		if strings.TrimSpace(confirmation) != "DELETE EVERYTHING" {
			output.Info("Cancelled - confirmation text did not match")
			return nil
		}
	}

	// Get all contexts
	output.Step("Step 1/5: Fetching all contexts...")
	contexts, err := c.GetContexts()
	if err != nil {
		// If contexts API not available, use default context
		contexts = []string{"."}
	}
	output.Info("Found %d contexts", len(contexts))

	// Get all subjects
	output.Step("Step 2/5: Fetching all subjects...")
	subjects, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}
	output.Info("Found %d subjects", len(subjects))

	if len(subjects) == 0 {
		output.Success("Schema Registry is already empty")
		return nil
	}

	// Phase 1: Soft delete all
	output.Step("Step 3/5: Soft deleting all subjects (%d workers)...", deleteWorkers)
	softDeleteParallel(c, subjects)

	// Phase 2: Hard delete all
	output.Step("Step 4/5: Permanently deleting all subjects (%d workers)...", deleteWorkers)
	totalVersions, failedCount := hardDeleteParallel(c, subjects)

	// Summary
	output.Step("Step 5/5: Complete")
	output.Success("Deleted %d subjects with %d total versions (failed: %d)",
		len(subjects)-failedCount, totalVersions, failedCount)

	return nil
}

// softDeleteParallel performs soft deletes in parallel
func softDeleteParallel(c *client.SchemaRegistryClient, subjects []string) {
	var wg sync.WaitGroup
	jobs := make(chan string, len(subjects))

	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Soft deleting"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	for i := 0; i < deleteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				c.DeleteSubject(subj, false)
				bar.Add(1)
			}
		}()
	}

	for _, subj := range subjects {
		jobs <- subj
	}
	close(jobs)
	wg.Wait()
	bar.Finish()
}

// hardDeleteParallel performs hard deletes in parallel and returns total versions and failure count
func hardDeleteParallel(c *client.SchemaRegistryClient, subjects []string) (totalVersions int, failedCount int) {
	var wg sync.WaitGroup
	jobs := make(chan string, len(subjects))

	var versions int64
	var failed int64

	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Hard deleting"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	for i := 0; i < deleteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				vers, err := c.DeleteSubject(subj, true)
				if err != nil {
					atomic.AddInt64(&failed, 1)
				} else {
					atomic.AddInt64(&versions, int64(len(vers)))
				}
				bar.Add(1)
			}
		}()
	}

	for _, subj := range subjects {
		jobs <- subj
	}
	close(jobs)
	wg.Wait()
	bar.Finish()

	return int(versions), int(failed)
}

// keepLatestVersionsMulti handles --keep-latest for multiple subjects
func keepLatestVersionsMulti(c *client.SchemaRegistryClient, subjects []string, keepN int) error {
	output.Header("Keep Latest %d Versions for %d Subjects", keepN, len(subjects))

	if !deleteYes && !confirmAction(fmt.Sprintf("Process %d subjects?", len(subjects))) {
		output.Info("Cancelled")
		return nil
	}

	type keepResult struct {
		Subject string
		Deleted int
		Kept    int
		Error   error
	}

	jobs := make(chan string, len(subjects))
	results := make(chan keepResult, len(subjects))

	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Processing"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < deleteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subj := range jobs {
				result := keepResult{Subject: subj}

				versions, err := c.GetVersions(subj, false)
				if err != nil {
					result.Error = err
					results <- result
					bar.Add(1)
					continue
				}

				if len(versions) <= keepN {
					result.Kept = len(versions)
					results <- result
					bar.Add(1)
					continue
				}

				toDelete := versions[:len(versions)-keepN]
				result.Kept = keepN

				for _, v := range toDelete {
					// Soft delete
					_, err := c.DeleteVersion(subj, strconv.Itoa(v), false)
					if err != nil && !strings.Contains(err.Error(), "already deleted") {
						continue
					}

					// Hard delete if force
					if deleteForce {
						_, err = c.DeleteVersion(subj, strconv.Itoa(v), true)
						if err != nil {
							continue
						}
					}
					result.Deleted++
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
	var totalDeleted, totalKept, errCount int
	for r := range results {
		if r.Error != nil {
			errCount++
		} else {
			totalDeleted += r.Deleted
			totalKept += r.Kept
		}
	}
	bar.Finish()

	// Summary
	deleteType := "Soft"
	if deleteForce {
		deleteType = "Permanent"
	}
	output.Header("Keep Latest Complete")
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Delete Type", deleteType},
			{"Subjects Processed", strconv.Itoa(len(subjects))},
			{"Versions Deleted", strconv.Itoa(totalDeleted)},
			{"Versions Kept", strconv.Itoa(totalKept)},
			{"Errors", strconv.Itoa(errCount)},
		},
	)

	return nil
}
