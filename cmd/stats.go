package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var statsCmd = &cobra.Command{
	Use:     "stats",
	Short:   "Show Schema Registry statistics",
	GroupID: groupConfig,
	Long: `Display comprehensive statistics about the Schema Registry.

Statistics include:
  • Total subjects (active and soft-deleted)
  • Total schema versions
  • Schema counts by type (Avro, Protobuf, JSON)
  • Unique schema IDs
  • Size metrics (schema sizes)
  • Subjects with most versions
  • Reference statistics

Examples:
  # Show all statistics
  srctl stats

  # Show stats for specific context
  srctl stats --context .production

  # Output as JSON
  srctl stats -o json

  # Show detailed breakdown
  srctl stats --detailed
  
  # Control parallelism
  srctl stats --workers 50`,
	RunE: runStats,
}

var (
	statsDetailed bool
	statsWorkers  int
)

func init() {
	statsCmd.Flags().BoolVar(&statsDetailed, "detailed", false, "Show detailed breakdown")
	statsCmd.Flags().IntVar(&statsWorkers, "workers", 20, "Number of parallel workers for fetching schemas")
	rootCmd.AddCommand(statsCmd)
}

type RegistryStats struct {
	// Subject counts (excludes internal subjects like _confluent-ksql-*)
	ActiveSubjects   int `json:"activeSubjects"`
	DeletedSubjects  int `json:"deletedSubjects"`
	TotalSubjects    int `json:"totalSubjects"`
	InternalSubjects int `json:"internalSubjects"`

	// Version counts (excludes internal)
	ActiveVersions   int `json:"activeVersions"`
	DeletedVersions  int `json:"deletedVersions"`
	TotalVersions    int `json:"totalVersions"`
	InternalVersions int `json:"internalVersions"`

	// Schema ID stats
	UniqueSchemaIDs int `json:"uniqueSchemaIds"`
	MinSchemaID     int `json:"minSchemaId"`
	MaxSchemaID     int `json:"maxSchemaId"`

	// Type breakdown (user schemas only)
	AvroSchemas     int `json:"avroSchemas"`
	ProtobufSchemas int `json:"protobufSchemas"`
	JSONSchemas     int `json:"jsonSchemas"`

	// Size metrics
	TotalSchemaSize int64   `json:"totalSchemaSize"`
	AvgSchemaSize   float64 `json:"avgSchemaSize"`
	MinSchemaSize   int64   `json:"minSchemaSize"`
	MaxSchemaSize   int64   `json:"maxSchemaSize"`
	LargestSchema   string  `json:"largestSchema"`

	// Reference stats
	SchemasWithRefs int `json:"schemasWithRefs"`
	TotalReferences int `json:"totalReferences"`

	// Top subjects
	TopByVersions []SubjectVersionCount `json:"topByVersions,omitempty"`
	TopBySize     []SubjectSizeInfo     `json:"topBySize,omitempty"`
}

type SubjectVersionCount struct {
	Subject  string `json:"subject"`
	Versions int    `json:"versions"`
}

type SubjectSizeInfo struct {
	Subject      string `json:"subject"`
	TotalSize    int64  `json:"totalSize"`
	AvgSize      int64  `json:"avgSize"`
	VersionCount int    `json:"versionCount"`
}

// subjectResult holds the analysis result for a single subject
type subjectResult struct {
	Subject          string
	VersionCount     int
	TotalSize        int64
	SchemaIDs        []int
	TypeCounts       map[string]int
	TotalRefCount    int // Total number of references across all versions
	VersionsWithRefs int // Number of schema versions that have references
	MinID            int
	MaxID            int
	MinSize          int64
	MaxSize          int64
	MaxSizeInfo      string
	Errors           []string
	IsInternal       bool
}

// isInternalSubject checks if a subject is an internal/system subject
func isInternalSubject(subject string) bool {
	return strings.HasPrefix(subject, "_confluent-ksql-")
}

func runStats(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Schema Registry Statistics")

	// Get all subjects (active)
	output.Step("Fetching active subjects...")
	allActiveSubjects, err := c.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	// Get all subjects including deleted
	output.Step("Fetching all subjects (including deleted)...")
	allSubjectsIncludingDeleted, err := c.GetSubjects(true)
	if err != nil {
		return fmt.Errorf("failed to get all subjects: %w", err)
	}

	// Filter out internal subjects (_confluent-ksql-*)
	var activeSubjects, allSubjects []string
	var internalActiveCount, internalAllCount int
	for _, s := range allActiveSubjects {
		if isInternalSubject(s) {
			internalActiveCount++
		} else {
			activeSubjects = append(activeSubjects, s)
		}
	}
	for _, s := range allSubjectsIncludingDeleted {
		if isInternalSubject(s) {
			internalAllCount++
		} else {
			allSubjects = append(allSubjects, s)
		}
	}

	stats := RegistryStats{
		ActiveSubjects:   len(activeSubjects),
		TotalSubjects:    len(allSubjects),
		DeletedSubjects:  len(allSubjects) - len(activeSubjects),
		InternalSubjects: internalAllCount,
		MinSchemaID:      int(^uint(0) >> 1),
		MinSchemaSize:    int64(^uint64(0) >> 1),
	}

	if stats.TotalSubjects == 0 {
		output.Info("Registry is empty")
		return nil
	}

	output.Info("Found %d subjects (%d active, %d deleted) - excluding %d internal subjects", stats.TotalSubjects, stats.ActiveSubjects, stats.DeletedSubjects, stats.InternalSubjects)

	// Analyze schemas using worker pool
	output.Step("Analyzing schemas with %d workers...", statsWorkers)

	results := analyzeSubjectsParallel(c, allSubjects, statsWorkers)

	// Aggregate results
	schemaIDs := make(map[int]bool)
	subjectVersionCounts := make(map[string]int)
	subjectSizes := make(map[string]int64)

	var allErrors []string
	var subjectsWithErrors int

	for _, r := range results {
		// Skip internal schemas from ALL statistics
		if r.IsInternal {
			stats.InternalSubjects++
			stats.InternalVersions += r.VersionCount
			continue
		}

		// Track errors (user schemas only)
		if len(r.Errors) > 0 {
			subjectsWithErrors++
			for _, e := range r.Errors {
				allErrors = append(allErrors, fmt.Sprintf("%s: %s", r.Subject, e))
			}
		}

		stats.TotalVersions += r.VersionCount
		stats.TotalSubjects++
		subjectVersionCounts[r.Subject] = r.VersionCount
		subjectSizes[r.Subject] = r.TotalSize

		for _, id := range r.SchemaIDs {
			schemaIDs[id] = true
			if id < stats.MinSchemaID {
				stats.MinSchemaID = id
			}
			if id > stats.MaxSchemaID {
				stats.MaxSchemaID = id
			}
		}

		stats.AvroSchemas += r.TypeCounts["AVRO"]
		stats.ProtobufSchemas += r.TypeCounts["PROTOBUF"]
		stats.JSONSchemas += r.TypeCounts["JSON"]

		stats.TotalSchemaSize += r.TotalSize
		stats.TotalReferences += r.TotalRefCount
		stats.SchemasWithRefs += r.VersionsWithRefs

		if r.MinSize < stats.MinSchemaSize && r.MinSize > 0 {
			stats.MinSchemaSize = r.MinSize
		}
		if r.MaxSize > stats.MaxSchemaSize {
			stats.MaxSchemaSize = r.MaxSize
			stats.LargestSchema = r.MaxSizeInfo
		}
	}

	// Report errors if any
	if len(allErrors) > 0 {
		output.Warning("Encountered %d errors across %d subjects", len(allErrors), subjectsWithErrors)
		if len(allErrors) <= 20 {
			for _, e := range allErrors {
				output.Error("  %s", e)
			}
		} else {
			for _, e := range allErrors[:10] {
				output.Error("  %s", e)
			}
			output.Info("  ... and %d more errors", len(allErrors)-10)
		}
	}

	// Calculate active versions (approximate from active subjects)
	for _, subj := range activeSubjects {
		stats.ActiveVersions += subjectVersionCounts[subj]
	}
	stats.DeletedVersions = stats.TotalVersions - stats.ActiveVersions

	stats.UniqueSchemaIDs = len(schemaIDs)

	if stats.TotalVersions > 0 {
		stats.AvgSchemaSize = float64(stats.TotalSchemaSize) / float64(stats.TotalVersions)
	}

	if stats.MinSchemaID == int(^uint(0)>>1) {
		stats.MinSchemaID = 0
	}
	if stats.MinSchemaSize == int64(^uint64(0)>>1) {
		stats.MinSchemaSize = 0
	}

	// Top subjects by versions
	type kv struct {
		Key   string
		Value int
	}
	var versionsSorted []kv
	for k, v := range subjectVersionCounts {
		versionsSorted = append(versionsSorted, kv{k, v})
	}
	sort.Slice(versionsSorted, func(i, j int) bool {
		return versionsSorted[i].Value > versionsSorted[j].Value
	})

	limit := 10
	if len(versionsSorted) < limit {
		limit = len(versionsSorted)
	}
	for i := 0; i < limit; i++ {
		stats.TopByVersions = append(stats.TopByVersions, SubjectVersionCount{
			Subject:  versionsSorted[i].Key,
			Versions: versionsSorted[i].Value,
		})
	}

	// Top subjects by size
	type kvSize struct {
		Key   string
		Value int64
	}
	var sizesSorted []kvSize
	for k, v := range subjectSizes {
		sizesSorted = append(sizesSorted, kvSize{k, v})
	}
	sort.Slice(sizesSorted, func(i, j int) bool {
		return sizesSorted[i].Value > sizesSorted[j].Value
	})

	limit = 10
	if len(sizesSorted) < limit {
		limit = len(sizesSorted)
	}
	for i := 0; i < limit; i++ {
		count := subjectVersionCounts[sizesSorted[i].Key]
		avgSize := int64(0)
		if count > 0 {
			avgSize = sizesSorted[i].Value / int64(count)
		}
		stats.TopBySize = append(stats.TopBySize, SubjectSizeInfo{
			Subject:      sizesSorted[i].Key,
			TotalSize:    sizesSorted[i].Value,
			AvgSize:      avgSize,
			VersionCount: count,
		})
	}

	// Output
	printer := output.NewPrinter(outputFormat)

	if outputFormat != "table" {
		return printer.Print(stats)
	}

	// Table output
	output.SubHeader("Subject Statistics")
	output.PrintTable(
		[]string{"Metric", "Active", "Deleted", "Total"},
		[][]string{
			{"Subjects", strconv.Itoa(stats.ActiveSubjects), strconv.Itoa(stats.DeletedSubjects), strconv.Itoa(stats.TotalSubjects)},
			{"Schema Versions", strconv.Itoa(stats.ActiveVersions), strconv.Itoa(stats.DeletedVersions), strconv.Itoa(stats.TotalVersions)},
		},
	)
	output.Info("(Excluding %d internal subjects with %d versions)", stats.InternalSubjects, stats.InternalVersions)

	output.SubHeader("Schema ID Statistics")
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Unique Schema IDs", strconv.Itoa(stats.UniqueSchemaIDs)},
			{"Min Schema ID", strconv.Itoa(stats.MinSchemaID)},
			{"Max Schema ID", strconv.Itoa(stats.MaxSchemaID)},
			{"ID Range", strconv.Itoa(stats.MaxSchemaID - stats.MinSchemaID + 1)},
		},
	)

	output.SubHeader("Schema Type Distribution")
	total := stats.AvroSchemas + stats.ProtobufSchemas + stats.JSONSchemas
	if total == 0 {
		total = 1 // Avoid division by zero
	}
	output.PrintTable(
		[]string{"Type", "Count", "Percentage"},
		[][]string{
			{"AVRO", strconv.Itoa(stats.AvroSchemas), fmt.Sprintf("%.1f%%", float64(stats.AvroSchemas)/float64(total)*100)},
			{"PROTOBUF", strconv.Itoa(stats.ProtobufSchemas), fmt.Sprintf("%.1f%%", float64(stats.ProtobufSchemas)/float64(total)*100)},
			{"JSON", strconv.Itoa(stats.JSONSchemas), fmt.Sprintf("%.1f%%", float64(stats.JSONSchemas)/float64(total)*100)},
		},
	)

	output.SubHeader("Size Metrics")
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Total Schema Size", output.FormatBytes(stats.TotalSchemaSize)},
			{"Average Schema Size", output.FormatBytes(int64(stats.AvgSchemaSize))},
			{"Min Schema Size", output.FormatBytes(stats.MinSchemaSize)},
			{"Max Schema Size", output.FormatBytes(stats.MaxSchemaSize)},
			{"Largest Schema", stats.LargestSchema},
		},
	)

	output.SubHeader("Reference Statistics")
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Schema Versions with References", strconv.Itoa(stats.SchemasWithRefs)},
			{"Total References", strconv.Itoa(stats.TotalReferences)},
		},
	)

	if statsDetailed {
		output.SubHeader("Top 10 Subjects by Version Count")
		var versionRows [][]string
		for _, s := range stats.TopByVersions {
			versionRows = append(versionRows, []string{s.Subject, strconv.Itoa(s.Versions)})
		}
		output.PrintTable([]string{"Subject", "Versions"}, versionRows)

		output.SubHeader("Top 10 Subjects by Total Size")
		var sizeRows [][]string
		for _, s := range stats.TopBySize {
			sizeRows = append(sizeRows, []string{
				s.Subject,
				output.FormatBytes(s.TotalSize),
				output.FormatBytes(s.AvgSize),
				strconv.Itoa(s.VersionCount),
			})
		}
		output.PrintTable([]string{"Subject", "Total Size", "Avg Size", "Versions"}, sizeRows)
	}

	return nil
}

// analyzeSubjectsParallel analyzes subjects using a worker pool
func analyzeSubjectsParallel(c *client.SchemaRegistryClient, subjects []string, numWorkers int) []subjectResult {
	// Create channels
	jobs := make(chan string, len(subjects))
	results := make(chan subjectResult, len(subjects))

	// Progress tracking
	var completed int64
	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Analyzing"),
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
			for subject := range jobs {
				result := analyzeSubject(c, subject)
				results <- result
				atomic.AddInt64(&completed, 1)
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

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allResults []subjectResult
	for r := range results {
		allResults = append(allResults, r)
	}

	bar.Finish()
	return allResults
}

// analyzeSubject analyzes a single subject by fetching ALL versions
func analyzeSubject(c *client.SchemaRegistryClient, subject string) subjectResult {
	result := subjectResult{
		Subject:    subject,
		TypeCounts: make(map[string]int),
		MinID:      int(^uint(0) >> 1),
		MinSize:    int64(^uint64(0) >> 1),
		Errors:     []string{},
		IsInternal: isInternalSubject(subject),
	}

	// Get all versions
	versions, err := c.GetVersions(subject, true)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("GetVersions: %v", err))
		return result
	}

	result.VersionCount = len(versions)

	// Fetch EVERY version for accurate counts
	for _, v := range versions {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			// Retry with deleted=true for soft-deleted schemas
			schema, err = c.GetSchemaWithDeleted(subject, strconv.Itoa(v), true)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("GetSchema v%d: %v", v, err))
				continue
			}
		}

		// Track schema type
		schemaType := strings.ToUpper(schema.SchemaType)
		if schemaType == "" {
			schemaType = "AVRO"
		}
		result.TypeCounts[schemaType]++

		// Track schema ID
		result.SchemaIDs = append(result.SchemaIDs, schema.ID)

		// Track size
		schemaSize := int64(len(schema.Schema))
		result.TotalSize += schemaSize

		if schemaSize < result.MinSize {
			result.MinSize = schemaSize
		}
		if schemaSize > result.MaxSize {
			result.MaxSize = schemaSize
			result.MaxSizeInfo = fmt.Sprintf("%s (v%d)", subject, v)
		}

		// Track references
		if len(schema.References) > 0 {
			result.TotalRefCount += len(schema.References)
			result.VersionsWithRefs++
		}
	}

	return result
}

// Health command
var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check Schema Registry health",
	Long: `Check the health and connectivity of the Schema Registry.

Performs various checks:
  • API connectivity
  • Authentication
  • Basic operations (list subjects)
  • Response times

Examples:
  # Check default registry
  srctl health

  # Check specific registry
  srctl health --registry prod

  # Check all configured registries
  srctl health --all`,
	RunE: runHealth,
}

var healthAll bool

func init() {
	healthCmd.Flags().BoolVar(&healthAll, "all", false, "Check all configured registries")
	rootCmd.AddCommand(healthCmd)
}

func runHealth(cmd *cobra.Command, args []string) error {
	output.Header("Schema Registry Health Check")

	c, err := GetClient()
	if err != nil {
		output.Error("Failed to create client: %v", err)
		return err
	}

	// Test connectivity
	output.Step("Checking connectivity...")

	// Try to list subjects
	subjects, err := c.GetSubjects(false)
	if err != nil {
		output.Error("API Error: %v", err)
		return err
	}

	output.Success("Connection successful")
	output.Info("Registry URL: %s", registryURL)
	output.Info("Subjects found: %d", len(subjects))

	// Check mode
	mode, err := c.GetMode()
	if err == nil && mode != nil {
		output.Info("Mode: %s", mode.Mode)
	}

	// Check config
	config, err := c.GetConfig()
	if err == nil && config != nil {
		level := config.CompatibilityLevel
		if level == "" {
			level = config.Compatibility
		}
		output.Info("Compatibility: %s", level)
	}

	// Try to get contexts
	contexts, err := c.GetContexts()
	if err == nil {
		output.Info("Contexts: %d", len(contexts))
	}

	output.Success("All health checks passed")
	return nil
}
