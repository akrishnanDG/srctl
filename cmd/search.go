package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var searchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search schemas by field, type, tag, or content",
	GroupID: groupSchema,
	Long: `Search across all schemas in the registry by field name, type, tag,
or content. Useful for governance, discovery, and impact analysis.

Search modes:
  --field     Search for field/property names (supports glob: *.address)
  --text      Full-text search in schema content
  --tag       Search for schemas with specific tags

Examples:
  # Find all schemas with an 'email' field
  srctl search --field email

  # Find schemas with fields matching a pattern
  srctl search --field "*.address"

  # Find fields of a specific type
  srctl search --field email --field-type string

  # Full-text search
  srctl search --text customerId

  # Search for tagged schemas
  srctl search --tag PII

  # Only search latest versions
  srctl search --field email --version latest

  # Filter by subject name pattern
  srctl search --field email --filter "order-*"

  # Output as JSON for scripting
  srctl search --field email -o json`,
	RunE: runSearch,
}

var (
	searchField     string
	searchFieldType string
	searchText      string
	searchTag       string
	searchVersion   string
	searchFilter    string
	searchWorkers   int
)

func init() {
	searchCmd.Flags().StringVar(&searchField, "field", "", "Search for field names (supports glob patterns)")
	searchCmd.Flags().StringVar(&searchFieldType, "field-type", "", "Filter by field type (used with --field)")
	searchCmd.Flags().StringVar(&searchText, "text", "", "Full-text search in schema content")
	searchCmd.Flags().StringVar(&searchTag, "tag", "", "Search for schemas with specific tags")
	searchCmd.Flags().StringVar(&searchVersion, "version", "latest", "Which versions to search: 'all' or 'latest'")
	searchCmd.Flags().StringVar(&searchFilter, "filter", "", "Subject name filter (glob pattern)")
	searchCmd.Flags().IntVar(&searchWorkers, "workers", 20, "Number of parallel workers")

	rootCmd.AddCommand(searchCmd)
}

// SearchResult represents a matching schema
type SearchResult struct {
	Subject    string        `json:"subject"`
	Version    int           `json:"version"`
	SchemaType string        `json:"schemaType"`
	SchemaID   int           `json:"schemaId"`
	Matches    []SearchMatch `json:"matches"`
}

// SearchMatch represents a single match within a schema
type SearchMatch struct {
	FieldPath string `json:"fieldPath,omitempty"`
	FieldType string `json:"fieldType,omitempty"`
	Context   string `json:"context,omitempty"`
	MatchType string `json:"matchType"`
}

func runSearch(cmd *cobra.Command, args []string) error {
	if searchField == "" && searchText == "" && searchTag == "" {
		return fmt.Errorf("at least one search criteria required: --field, --text, or --tag")
	}

	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Schema Search")
	if searchField != "" {
		output.Info("Field pattern: %s", searchField)
		if searchFieldType != "" {
			output.Info("Field type filter: %s", searchFieldType)
		}
	}
	if searchText != "" {
		output.Info("Text search: %s", searchText)
	}
	if searchTag != "" {
		output.Info("Tag search: %s", searchTag)
	}
	fmt.Println()

	output.Step("Fetching subjects...")
	subjects, err := c.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	if searchFilter != "" {
		var filtered []string
		for _, subj := range subjects {
			matched, _ := filepath.Match(searchFilter, subj)
			if matched {
				filtered = append(filtered, subj)
			}
		}
		subjects = filtered
		output.Info("Filtered to %d subjects matching '%s'", len(subjects), searchFilter)
	}

	if len(subjects) == 0 {
		output.Info("No subjects to search")
		return nil
	}

	output.Step("Searching %d subjects with %d workers...", len(subjects), searchWorkers)
	results := searchSubjectsParallel(c, subjects, searchWorkers)

	var matchingResults []SearchResult
	for _, r := range results {
		if len(r.Matches) > 0 {
			matchingResults = append(matchingResults, r)
		}
	}

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(matchingResults)
	}

	if len(matchingResults) == 0 {
		fmt.Println()
		output.Info("No matches found")
		return nil
	}

	fmt.Println()
	output.SubHeader("Results (%d matches across %d schema versions)", countTotalMatches(matchingResults), len(matchingResults))

	headers := []string{"Subject", "Version", "Type", "Match", "Field Path", "Field Type"}
	var rows [][]string

	for _, r := range matchingResults {
		for _, m := range r.Matches {
			rows = append(rows, []string{
				r.Subject,
				strconv.Itoa(r.Version),
				r.SchemaType,
				m.MatchType,
				truncate(m.FieldPath, 40),
				m.FieldType,
			})
		}
	}

	output.PrintTable(headers, rows)
	return nil
}

func searchSubjectsParallel(c *client.SchemaRegistryClient, subjects []string, numWorkers int) []SearchResult {
	jobs := make(chan string, len(subjects))
	results := make(chan []SearchResult, len(subjects))

	var completed int64
	bar := progressbar.NewOptions(len(subjects),
		progressbar.OptionSetDescription("Searching"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
	)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subject := range jobs {
				subjectResults := searchSubject(c, subject)
				results <- subjectResults
				atomic.AddInt64(&completed, 1)
				bar.Add(1)
			}
		}()
	}

	go func() {
		for _, subj := range subjects {
			jobs <- subj
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []SearchResult
	for batch := range results {
		allResults = append(allResults, batch...)
	}

	bar.Finish()
	return allResults
}

func searchSubject(c *client.SchemaRegistryClient, subject string) []SearchResult {
	var results []SearchResult

	versions, err := c.GetVersions(subject, false)
	if err != nil || len(versions) == 0 {
		return results
	}

	var versionsToSearch []int
	if searchVersion == "latest" {
		versionsToSearch = []int{versions[len(versions)-1]}
	} else {
		versionsToSearch = versions
	}

	for _, v := range versionsToSearch {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			continue
		}

		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}

		var matches []SearchMatch

		if searchField != "" {
			fieldMatches := searchSchemaFields(schema.Schema, schemaType, searchField, searchFieldType)
			matches = append(matches, fieldMatches...)
		}

		if searchText != "" {
			textMatches := searchSchemaText(schema.Schema, searchText)
			matches = append(matches, textMatches...)
		}

		if searchTag != "" {
			tagMatches := searchSchemaTags(c, subject, v, searchTag)
			matches = append(matches, tagMatches...)
		}

		if len(matches) > 0 {
			results = append(results, SearchResult{
				Subject:    subject,
				Version:    v,
				SchemaType: schemaType,
				SchemaID:   schema.ID,
				Matches:    matches,
			})
		}
	}

	return results
}

func searchSchemaFields(schemaContent, schemaType, pattern, typeFilter string) []SearchMatch {
	switch strings.ToUpper(schemaType) {
	case "AVRO":
		return searchAvroFields(schemaContent, pattern, typeFilter)
	case "PROTOBUF":
		return searchProtobufFields(schemaContent, pattern, typeFilter)
	case "JSON":
		return searchJSONSchemaFields(schemaContent, pattern, typeFilter)
	default:
		return nil
	}
}

func searchAvroFields(content, pattern, typeFilter string) []SearchMatch {
	var matches []SearchMatch

	var schema interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return nil
	}

	fields := extractAllAvroFieldPaths(schema, "")
	for _, f := range fields {
		if matchFieldPattern(f.Path, pattern) {
			if typeFilter != "" && !strings.EqualFold(f.Type, typeFilter) {
				continue
			}
			matches = append(matches, SearchMatch{
				FieldPath: f.Path,
				FieldType: f.Type,
				MatchType: "field",
			})
		}
	}

	return matches
}

type fieldPathInfo struct {
	Path string
	Type string
}

func extractAllAvroFieldPaths(schema interface{}, prefix string) []fieldPathInfo {
	var fields []fieldPathInfo

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return fields
	}

	fieldList, ok := schemaMap["fields"].([]interface{})
	if !ok {
		return fields
	}

	for _, f := range fieldList {
		field, ok := f.(map[string]interface{})
		if !ok {
			continue
		}

		name, _ := field["name"].(string)
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		typeStr := formatAvroType(field["type"])
		fields = append(fields, fieldPathInfo{Path: path, Type: typeStr})

		if fieldType, ok := field["type"].(map[string]interface{}); ok {
			if ft, ok := fieldType["type"].(string); ok && ft == "record" {
				nested := extractAllAvroFieldPaths(fieldType, path)
				fields = append(fields, nested...)
			}
		}

		if unionTypes, ok := field["type"].([]interface{}); ok {
			for _, ut := range unionTypes {
				if utMap, ok := ut.(map[string]interface{}); ok {
					if ft, ok := utMap["type"].(string); ok && ft == "record" {
						nested := extractAllAvroFieldPaths(utMap, path)
						fields = append(fields, nested...)
					}
				}
			}
		}

		if fieldType, ok := field["type"].(map[string]interface{}); ok {
			if ft, ok := fieldType["type"].(string); ok && ft == "array" {
				if items, ok := fieldType["items"].(map[string]interface{}); ok {
					if it, ok := items["type"].(string); ok && it == "record" {
						nested := extractAllAvroFieldPaths(items, path+"[]")
						fields = append(fields, nested...)
					}
				}
			}
		}
	}

	return fields
}

func searchProtobufFields(content, pattern, typeFilter string) []SearchMatch {
	var matches []SearchMatch

	fieldRe := regexp.MustCompile(`(?m)^\s*(repeated\s+|optional\s+|required\s+)?(\w[\w.]*)\s+(\w+)\s*=\s*\d+`)
	allMatches := fieldRe.FindAllStringSubmatch(content, -1)

	for _, m := range allMatches {
		fieldType := m[2]
		fieldName := m[3]

		if matchFieldPattern(fieldName, pattern) {
			if typeFilter != "" && !strings.EqualFold(fieldType, typeFilter) {
				continue
			}
			matches = append(matches, SearchMatch{
				FieldPath: fieldName,
				FieldType: fieldType,
				MatchType: "field",
			})
		}
	}

	return matches
}

func searchJSONSchemaFields(content, pattern, typeFilter string) []SearchMatch {
	var matches []SearchMatch

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return nil
	}

	fields := extractAllJSONSchemaFieldPaths(schema, "")
	for _, f := range fields {
		if matchFieldPattern(f.Path, pattern) {
			if typeFilter != "" && !strings.EqualFold(f.Type, typeFilter) {
				continue
			}
			matches = append(matches, SearchMatch{
				FieldPath: f.Path,
				FieldType: f.Type,
				MatchType: "field",
			})
		}
	}

	return matches
}

func extractAllJSONSchemaFieldPaths(schema map[string]interface{}, prefix string) []fieldPathInfo {
	var fields []fieldPathInfo

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return fields
	}

	for name, val := range properties {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		propMap, ok := val.(map[string]interface{})
		if !ok {
			continue
		}

		typeStr, _ := propMap["type"].(string)
		if typeStr == "" {
			if _, hasRef := propMap["$ref"]; hasRef {
				typeStr = "$ref"
			}
		}

		fields = append(fields, fieldPathInfo{Path: path, Type: typeStr})

		if typeStr == "object" {
			nested := extractAllJSONSchemaFieldPaths(propMap, path)
			fields = append(fields, nested...)
		}

		if typeStr == "array" {
			if items, ok := propMap["items"].(map[string]interface{}); ok {
				if itemType, _ := items["type"].(string); itemType == "object" {
					nested := extractAllJSONSchemaFieldPaths(items, path+"[]")
					fields = append(fields, nested...)
				}
			}
		}
	}

	return fields
}

func searchSchemaText(content, searchTerm string) []SearchMatch {
	var matches []SearchMatch

	contentLower := strings.ToLower(content)
	termLower := strings.ToLower(searchTerm)

	if !strings.Contains(contentLower, termLower) {
		return nil
	}

	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if strings.Contains(strings.ToLower(line), termLower) {
			context := strings.TrimSpace(line)
			if len(context) > 80 {
				context = context[:80] + "..."
			}
			matches = append(matches, SearchMatch{
				FieldPath: fmt.Sprintf("line %d", i+1),
				Context:   context,
				MatchType: "text",
			})
		}
	}

	return matches
}

func searchSchemaTags(c *client.SchemaRegistryClient, subject string, version int, tagName string) []SearchMatch {
	var matches []SearchMatch

	tags, err := c.GetSchemaTags(subject, version)
	if err != nil {
		return nil
	}

	for _, tag := range tags {
		if strings.EqualFold(tag.TypeName, tagName) {
			matches = append(matches, SearchMatch{
				FieldPath: tag.TypeName,
				MatchType: "tag",
			})
		}
	}

	subjectTags, err := c.GetSubjectTags(subject)
	if err != nil {
		return matches
	}

	for _, tag := range subjectTags {
		if strings.EqualFold(tag.TypeName, tagName) {
			matches = append(matches, SearchMatch{
				FieldPath: tag.TypeName,
				MatchType: "tag",
			})
		}
	}

	return matches
}

func matchFieldPattern(fieldName, pattern string) bool {
	if strings.Contains(pattern, "*") || strings.Contains(pattern, "?") {
		matched, _ := filepath.Match(pattern, fieldName)
		if matched {
			return true
		}
		parts := strings.Split(fieldName, ".")
		for _, part := range parts {
			matched, _ = filepath.Match(pattern, part)
			if matched {
				return true
			}
		}
		return false
	}

	if strings.EqualFold(fieldName, pattern) {
		return true
	}

	parts := strings.Split(fieldName, ".")
	for _, part := range parts {
		if strings.EqualFold(part, pattern) {
			return true
		}
	}

	return false
}

func countTotalMatches(results []SearchResult) int {
	total := 0
	for _, r := range results {
		total += len(r.Matches)
	}
	return total
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
