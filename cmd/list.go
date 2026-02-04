package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	listFilter         string
	listIncludeDeleted bool
	listShowVersions   bool
	listSortBy         string
	listLimit          int
)

// subjectInfo holds information about a subject
type subjectInfo struct {
	Subject      string `json:"subject"`
	VersionCount int    `json:"versionCount,omitempty"`
	LatestID     int    `json:"latestId,omitempty"`
}

var listCmd = &cobra.Command{
	Use:     "list",
	Short:   "List subjects in the Schema Registry",
	GroupID: groupSchema,
	Long: `List all subjects in the Schema Registry with optional filtering and sorting.

Examples:
  # List all subjects
  srctl list

  # Filter subjects by prefix
  srctl list --filter "user-*"

  # Include soft-deleted subjects
  srctl list --deleted

  # Show version counts
  srctl list --versions

  # Sort by name or version count
  srctl list --versions --sort versions

  # List from specific context
  srctl list --context .mycontext

  # Limit results
  srctl list --limit 10`,
	RunE: runList,
}

func init() {
	listCmd.Flags().StringVarP(&listFilter, "filter", "f", "", "Filter subjects by pattern (supports * wildcard)")
	listCmd.Flags().BoolVarP(&listIncludeDeleted, "deleted", "d", false, "Include soft-deleted subjects")
	listCmd.Flags().BoolVarP(&listShowVersions, "versions", "V", false, "Show version count for each subject")
	listCmd.Flags().StringVar(&listSortBy, "sort", "name", "Sort by: name, versions (requires --versions)")
	listCmd.Flags().IntVar(&listLimit, "limit", 0, "Limit number of results (0 = no limit)")

	rootCmd.AddCommand(listCmd)
}

func runList(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	subjects, err := c.GetSubjects(listIncludeDeleted)
	if err != nil {
		return fmt.Errorf("failed to list subjects: %w", err)
	}

	// Apply filter
	if listFilter != "" {
		subjects = filterSubjects(subjects, listFilter)
	}

	var results []subjectInfo
	for _, subj := range subjects {
		info := subjectInfo{Subject: subj}

		if listShowVersions {
			versions, err := c.GetVersions(subj, listIncludeDeleted)
			if err == nil {
				info.VersionCount = len(versions)
				// Get latest schema ID
				if len(versions) > 0 {
					schema, err := c.GetSchema(subj, "latest")
					if err == nil {
						info.LatestID = schema.ID
					}
				}
			}
		}

		results = append(results, info)
	}

	// Sort results
	sortSubjectResults(results, listSortBy)

	// Apply limit
	if listLimit > 0 && len(results) > listLimit {
		results = results[:listLimit]
	}

	printer := output.NewPrinter(outputFormat)

	if outputFormat == "table" {
		printSubjectTable(results, listShowVersions, len(subjects))
		return nil
	}

	// For non-table formats
	if listShowVersions {
		return printer.Print(results)
	}

	// Just return subject names for simple list
	names := make([]string, len(results))
	for i, r := range results {
		names[i] = r.Subject
	}
	return printer.Print(names)
}

func filterSubjects(subjects []string, pattern string) []string {
	// Convert glob pattern to simple matching
	var filtered []string
	pattern = strings.ToLower(pattern)

	for _, subj := range subjects {
		if matchGlob(strings.ToLower(subj), pattern) {
			filtered = append(filtered, subj)
		}
	}

	return filtered
}

func matchGlob(s, pattern string) bool {
	// Simple glob matching supporting * wildcard
	if pattern == "*" {
		return true
	}

	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		// No wildcard, exact match
		return s == pattern
	}

	// Check prefix
	if parts[0] != "" && !strings.HasPrefix(s, parts[0]) {
		return false
	}

	// Check suffix
	if parts[len(parts)-1] != "" && !strings.HasSuffix(s, parts[len(parts)-1]) {
		return false
	}

	// Check middle parts
	idx := len(parts[0])
	for i := 1; i < len(parts)-1; i++ {
		if parts[i] == "" {
			continue
		}
		newIdx := strings.Index(s[idx:], parts[i])
		if newIdx < 0 {
			return false
		}
		idx += newIdx + len(parts[i])
	}

	return true
}

func sortSubjectResults(results []subjectInfo, sortBy string) {
	switch sortBy {
	case "versions":
		sort.Slice(results, func(i, j int) bool {
			return results[i].VersionCount > results[j].VersionCount
		})
	default: // "name"
		sort.Slice(results, func(i, j int) bool {
			return results[i].Subject < results[j].Subject
		})
	}
}

func printSubjectTable(results []subjectInfo, showVersions bool, total int) {
	output.Header("Subjects")

	if len(results) == 0 {
		output.Info("No subjects found")
		return
	}

	var headers []string
	var rows [][]string

	if showVersions {
		headers = []string{"#", "Subject", "Versions", "Latest ID"}
		for i, r := range results {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				r.Subject,
				strconv.Itoa(r.VersionCount),
				strconv.Itoa(r.LatestID),
			})
		}
	} else {
		headers = []string{"#", "Subject"}
		for i, r := range results {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				r.Subject,
			})
		}
	}

	output.PrintTable(headers, rows)
	fmt.Printf("\nShowing %d of %d subject(s)\n", len(results), total)
}

// ListVersions command to list versions of a subject
var listVersionsCmd = &cobra.Command{
	Use:   "versions <subject>",
	Short: "List all versions of a subject",
	Long: `List all schema versions for a specific subject.

Examples:
  # List versions
  srctl versions user-events

  # Include deleted versions
  srctl versions user-events --deleted

  # Output as JSON
  srctl versions user-events -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runListVersions,
}

var versionsIncludeDeleted bool

func init() {
	listVersionsCmd.Flags().BoolVarP(&versionsIncludeDeleted, "deleted", "d", false, "Include soft-deleted versions")
	rootCmd.AddCommand(listVersionsCmd)
}

func runListVersions(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]
	versions, err := c.GetVersions(subject, versionsIncludeDeleted)
	if err != nil {
		return fmt.Errorf("failed to get versions: %w", err)
	}

	printer := output.NewPrinter(outputFormat)

	if outputFormat == "table" {
		output.Header("Versions for: %s", subject)

		type versionInfo struct {
			Version int
			ID      int
			Type    string
		}

		var infos []versionInfo
		for _, v := range versions {
			schema, err := c.GetSchema(subject, strconv.Itoa(v))
			if err != nil {
				infos = append(infos, versionInfo{Version: v})
				continue
			}
			schemaType := schema.SchemaType
			if schemaType == "" {
				schemaType = "AVRO"
			}
			infos = append(infos, versionInfo{
				Version: v,
				ID:      schema.ID,
				Type:    schemaType,
			})
		}

		rows := make([][]string, len(infos))
		for i, info := range infos {
			rows[i] = []string{
				strconv.Itoa(info.Version),
				strconv.Itoa(info.ID),
				info.Type,
			}
		}
		output.PrintTable([]string{"Version", "Schema ID", "Type"}, rows)
		fmt.Printf("\nTotal: %d version(s)\n", len(versions))
		return nil
	}

	// Build detailed response for JSON/YAML
	type versionDetail struct {
		Version    int                      `json:"version"`
		ID         int                      `json:"id"`
		SchemaType string                   `json:"schemaType"`
		References []client.SchemaReference `json:"references,omitempty"`
	}

	var details []versionDetail
	for _, v := range versions {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			details = append(details, versionDetail{Version: v})
			continue
		}
		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}
		details = append(details, versionDetail{
			Version:    v,
			ID:         schema.ID,
			SchemaType: schemaType,
			References: schema.References,
		})
	}

	return printer.Print(map[string]interface{}{
		"subject":  subject,
		"versions": details,
	})
}
