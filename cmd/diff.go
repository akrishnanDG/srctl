package cmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var diffCmd = &cobra.Command{
	Use:     "diff <subject1>[@version1] [subject2[@version2]]",
	Short:   "Compare schemas between versions, subjects, or registries",
	GroupID: groupSchema,
	Long: `Compare schemas and show differences.

Comparison modes:
  • Compare two versions of the same subject
  • Compare schemas from different subjects
  • Compare schemas across different registries

Examples:
  # Compare latest with previous version
  srctl diff user-events

  # Compare specific versions
  srctl diff user-events@3 user-events@5

  # Compare different subjects
  srctl diff user-events order-events

  # Compare across registries
  srctl diff user-events --registry dev --with-registry prod

  # Compare by schema IDs
  srctl diff --id 100 --with-id 105`,
	RunE: runDiff,
}

var (
	diffSchemaID1    int
	diffSchemaID2    int
	diffWithRegistry string
	diffShowFull     bool
)

func init() {
	diffCmd.Flags().IntVar(&diffSchemaID1, "id", 0, "First schema ID to compare")
	diffCmd.Flags().IntVar(&diffSchemaID2, "with-id", 0, "Second schema ID to compare")
	diffCmd.Flags().StringVar(&diffWithRegistry, "with-registry", "", "Compare with schema from another registry")
	diffCmd.Flags().BoolVar(&diffShowFull, "full", false, "Show full schema content in diff")

	rootCmd.AddCommand(diffCmd)
}

func runDiff(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	// Compare by schema IDs
	if diffSchemaID1 > 0 && diffSchemaID2 > 0 {
		return compareSchemasByID(c, diffSchemaID1, diffSchemaID2)
	}

	// Parse arguments
	if len(args) == 0 && diffSchemaID1 == 0 {
		return fmt.Errorf("please provide subject name(s) or use --id flag")
	}

	subject1, version1 := parseSubjectVersion(args[0])

	var subject2, version2 string
	if len(args) > 1 {
		subject2, version2 = parseSubjectVersion(args[1])
	} else {
		// Compare with previous version
		subject2 = subject1
		if version1 == "latest" {
			// Get actual versions
			versions, err := c.GetVersions(subject1, false)
			if err != nil {
				return fmt.Errorf("failed to get versions: %w", err)
			}
			if len(versions) < 2 {
				return fmt.Errorf("subject has only one version, nothing to compare")
			}
			version1 = strconv.Itoa(versions[len(versions)-1])
			version2 = strconv.Itoa(versions[len(versions)-2])
		} else {
			v, _ := strconv.Atoi(version1)
			version2 = strconv.Itoa(v - 1)
		}
	}

	// Get second client if comparing across registries
	c2 := c
	if diffWithRegistry != "" {
		c2, err = GetClientForRegistry(diffWithRegistry)
		if err != nil {
			return fmt.Errorf("failed to get client for registry '%s': %w", diffWithRegistry, err)
		}
	}

	// Fetch schemas
	schema1, err := c.GetSchema(subject1, version1)
	if err != nil {
		return fmt.Errorf("failed to get schema %s@%s: %w", subject1, version1, err)
	}

	schema2, err := c2.GetSchema(subject2, version2)
	if err != nil {
		return fmt.Errorf("failed to get schema %s@%s: %w", subject2, version2, err)
	}

	return showDiff(schema1, schema2, subject1, version1, subject2, version2)
}

func parseSubjectVersion(arg string) (string, string) {
	parts := strings.Split(arg, "@")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return arg, "latest"
}

func compareSchemasByID(c *client.SchemaRegistryClient, id1, id2 int) error {
	schema1, err := c.GetSchemaByID(id1)
	if err != nil {
		return fmt.Errorf("failed to get schema ID %d: %w", id1, err)
	}

	schema2, err := c.GetSchemaByID(id2)
	if err != nil {
		return fmt.Errorf("failed to get schema ID %d: %w", id2, err)
	}

	return showDiff(schema1, schema2, fmt.Sprintf("ID:%d", id1), "", fmt.Sprintf("ID:%d", id2), "")
}

func showDiff(schema1, schema2 *client.Schema, name1, ver1, name2, ver2 string) error {
	output.Header("Schema Diff")

	// Header info
	leftLabel := name1
	if ver1 != "" {
		leftLabel = fmt.Sprintf("%s@%s", name1, ver1)
	}
	rightLabel := name2
	if ver2 != "" {
		rightLabel = fmt.Sprintf("%s@%s", name2, ver2)
	}

	fmt.Printf("  %s  ←→  %s\n", output.Cyan(leftLabel), output.Cyan(rightLabel))
	fmt.Println()

	// Parse schemas for structured diff
	var parsed1, parsed2 interface{}
	json.Unmarshal([]byte(schema1.Schema), &parsed1)
	json.Unmarshal([]byte(schema2.Schema), &parsed2)

	// Compare basic properties
	type1 := schema1.SchemaType
	if type1 == "" {
		type1 = "AVRO"
	}
	type2 := schema2.SchemaType
	if type2 == "" {
		type2 = "AVRO"
	}

	if type1 != type2 {
		output.Warning("Schema types differ: %s vs %s", type1, type2)
	}

	// For Avro schemas, do structured diff
	if type1 == "AVRO" && type2 == "AVRO" {
		return diffAvroSchemas(parsed1, parsed2)
	}

	// For other types, do line-by-line diff
	return diffText(schema1.Schema, schema2.Schema)
}

func diffAvroSchemas(schema1, schema2 interface{}) error {
	// Extract fields from Avro schemas
	fields1 := extractAvroFields(schema1)
	fields2 := extractAvroFields(schema2)

	// Find additions, deletions, modifications
	allFields := make(map[string]bool)
	for f := range fields1 {
		allFields[f] = true
	}
	for f := range fields2 {
		allFields[f] = true
	}

	var added, removed, modified, unchanged []string

	for field := range allFields {
		f1, exists1 := fields1[field]
		f2, exists2 := fields2[field]

		if !exists1 {
			added = append(added, field)
		} else if !exists2 {
			removed = append(removed, field)
		} else if f1 != f2 {
			modified = append(modified, field)
		} else {
			unchanged = append(unchanged, field)
		}
	}

	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(modified)
	sort.Strings(unchanged)

	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	// Summary
	output.SubHeader("Summary")
	fmt.Printf("  Added:     %d field(s)\n", len(added))
	fmt.Printf("  Removed:   %d field(s)\n", len(removed))
	fmt.Printf("  Modified:  %d field(s)\n", len(modified))
	fmt.Printf("  Unchanged: %d field(s)\n", len(unchanged))
	fmt.Println()

	// Details
	if len(added) > 0 {
		output.SubHeader("Added Fields")
		for _, f := range added {
			fmt.Printf("  %s %s: %s\n", green("+"), f, fields2[f])
		}
		fmt.Println()
	}

	if len(removed) > 0 {
		output.SubHeader("Removed Fields")
		for _, f := range removed {
			fmt.Printf("  %s %s: %s\n", red("-"), f, fields1[f])
		}
		fmt.Println()
	}

	if len(modified) > 0 {
		output.SubHeader("Modified Fields")
		for _, f := range modified {
			fmt.Printf("  %s %s:\n", yellow("~"), f)
			fmt.Printf("    %s %s\n", red("-"), fields1[f])
			fmt.Printf("    %s %s\n", green("+"), fields2[f])
		}
		fmt.Println()
	}

	// Compatibility analysis
	output.SubHeader("Compatibility Analysis")
	if len(removed) > 0 {
		output.Warning("Removing fields may break backward compatibility")
	}
	if len(added) > 0 {
		hasDefaults := true // Simplified check
		if hasDefaults {
			output.Info("Added fields - check if they have defaults for backward compatibility")
		}
	}
	if len(removed) == 0 && len(modified) == 0 {
		output.Success("Changes appear to be backward compatible")
	}

	return nil
}

func extractAvroFields(schema interface{}) map[string]string {
	fields := make(map[string]string)

	switch s := schema.(type) {
	case map[string]interface{}:
		if fieldList, ok := s["fields"].([]interface{}); ok {
			for _, f := range fieldList {
				if field, ok := f.(map[string]interface{}); ok {
					name, _ := field["name"].(string)
					typeStr := formatAvroType(field["type"])
					fields[name] = typeStr
				}
			}
		}
	}

	return fields
}

func formatAvroType(t interface{}) string {
	switch v := t.(type) {
	case string:
		return v
	case map[string]interface{}:
		if typ, ok := v["type"].(string); ok {
			if typ == "array" {
				items := formatAvroType(v["items"])
				return fmt.Sprintf("array<%s>", items)
			}
			if typ == "map" {
				values := formatAvroType(v["values"])
				return fmt.Sprintf("map<%s>", values)
			}
			return typ
		}
		b, _ := json.Marshal(v)
		return string(b)
	case []interface{}:
		// Union type
		var types []string
		for _, ut := range v {
			types = append(types, formatAvroType(ut))
		}
		return fmt.Sprintf("union[%s]", strings.Join(types, ","))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func diffText(text1, text2 string) error {
	lines1 := strings.Split(text1, "\n")
	lines2 := strings.Split(text2, "\n")

	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// Simple line-by-line comparison
	maxLines := len(lines1)
	if len(lines2) > maxLines {
		maxLines = len(lines2)
	}

	output.SubHeader("Line Diff")
	for i := 0; i < maxLines; i++ {
		var l1, l2 string
		if i < len(lines1) {
			l1 = lines1[i]
		}
		if i < len(lines2) {
			l2 = lines2[i]
		}

		if l1 != l2 {
			if l1 != "" {
				fmt.Printf("%s %s\n", red("-"), l1)
			}
			if l2 != "" {
				fmt.Printf("%s %s\n", green("+"), l2)
			}
		}
	}

	return nil
}

// Evolve command
var evolveCmd = &cobra.Command{
	Use:   "evolve <subject>",
	Short: "Analyze schema evolution history",
	Long: `Analyze the evolution history of a schema subject.

Shows:
  • All versions with changes between each
  • Breaking changes detection
  • Field additions/removals timeline
  • Compatibility status at each version

Examples:
  # Show evolution history
  srctl evolve user-events

  # Show detailed changes
  srctl evolve user-events --detailed

  # Output as JSON
  srctl evolve user-events -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runEvolve,
}

var evolveDetailed bool

func init() {
	evolveCmd.Flags().BoolVar(&evolveDetailed, "detailed", false, "Show detailed changes between versions")
	rootCmd.AddCommand(evolveCmd)
}

func runEvolve(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]

	// Get all versions
	versions, err := c.GetVersions(subject, false)
	if err != nil {
		return fmt.Errorf("failed to get versions: %w", err)
	}

	if len(versions) == 0 {
		return fmt.Errorf("no versions found for subject %s", subject)
	}

	output.Header("Schema Evolution: %s", subject)
	fmt.Printf("Total versions: %d\n\n", len(versions))

	printer := output.NewPrinter(outputFormat)

	// Collect evolution data
	type versionEvolution struct {
		Version       int      `json:"version"`
		SchemaID      int      `json:"schemaId"`
		SchemaType    string   `json:"schemaType"`
		FieldsAdded   []string `json:"fieldsAdded,omitempty"`
		FieldsRemoved []string `json:"fieldsRemoved,omitempty"`
		FieldsChanged []string `json:"fieldsChanged,omitempty"`
		TotalFields   int      `json:"totalFields"`
		Breaking      bool     `json:"breaking"`
	}

	var evolution []versionEvolution
	var prevFields map[string]string

	for i, v := range versions {
		schema, err := c.GetSchema(subject, strconv.Itoa(v))
		if err != nil {
			continue
		}

		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}

		var parsed interface{}
		json.Unmarshal([]byte(schema.Schema), &parsed)
		currentFields := extractAvroFields(parsed)

		ev := versionEvolution{
			Version:     v,
			SchemaID:    schema.ID,
			SchemaType:  schemaType,
			TotalFields: len(currentFields),
		}

		// Compare with previous version
		if i > 0 && prevFields != nil {
			for field := range currentFields {
				if _, exists := prevFields[field]; !exists {
					ev.FieldsAdded = append(ev.FieldsAdded, field)
				} else if currentFields[field] != prevFields[field] {
					ev.FieldsChanged = append(ev.FieldsChanged, field)
				}
			}
			for field := range prevFields {
				if _, exists := currentFields[field]; !exists {
					ev.FieldsRemoved = append(ev.FieldsRemoved, field)
					ev.Breaking = true // Removing fields is breaking
				}
			}
		}

		evolution = append(evolution, ev)
		prevFields = currentFields
	}

	if outputFormat != "table" {
		return printer.Print(map[string]interface{}{
			"subject":   subject,
			"versions":  len(versions),
			"evolution": evolution,
		})
	}

	// Table output
	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	for i, ev := range evolution {
		verLabel := fmt.Sprintf("Version %d (ID: %d)", ev.Version, ev.SchemaID)
		if ev.Breaking {
			verLabel += " " + red("[BREAKING]")
		}

		fmt.Printf("%s %s\n", output.Cyan("●"), verLabel)
		fmt.Printf("  Fields: %d", ev.TotalFields)

		if i > 0 {
			changes := []string{}
			if len(ev.FieldsAdded) > 0 {
				changes = append(changes, green(fmt.Sprintf("+%d added", len(ev.FieldsAdded))))
			}
			if len(ev.FieldsRemoved) > 0 {
				changes = append(changes, red(fmt.Sprintf("-%d removed", len(ev.FieldsRemoved))))
			}
			if len(ev.FieldsChanged) > 0 {
				changes = append(changes, yellow(fmt.Sprintf("~%d modified", len(ev.FieldsChanged))))
			}
			if len(changes) > 0 {
				fmt.Printf(" (%s)", strings.Join(changes, ", "))
			}
		}
		fmt.Println()

		if evolveDetailed && i > 0 {
			if len(ev.FieldsAdded) > 0 {
				fmt.Printf("    %s Added: %s\n", green("+"), strings.Join(ev.FieldsAdded, ", "))
			}
			if len(ev.FieldsRemoved) > 0 {
				fmt.Printf("    %s Removed: %s\n", red("-"), strings.Join(ev.FieldsRemoved, ", "))
			}
			if len(ev.FieldsChanged) > 0 {
				fmt.Printf("    %s Changed: %s\n", yellow("~"), strings.Join(ev.FieldsChanged, ", "))
			}
		}

		if i < len(evolution)-1 {
			fmt.Println("  │")
		}
	}

	return nil
}
