package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/output"
)

var suggestCmd = &cobra.Command{
	Use:     "suggest [subject] <description>",
	Short:   "Propose compatible schema changes from a description",
	GroupID: groupSchema,
	Long: `Given a change description (e.g., "add discount code"), analyzes the
current schema and proposes a compatible modification. Warns about
breaking changes and suggests safe alternatives.

Works with schemas from the registry or local files.

Examples:
  # Suggest adding a field (against registry)
  srctl suggest orders-value "add discount code"

  # Suggest against a local file
  srctl suggest --file order.avsc "add shipping address"

  # Suggest removing a field (warns about breaking change)
  srctl suggest orders-value "remove the notes field"

  # Suggest renaming a field
  srctl suggest --file order.avsc "rename email to emailAddress"`,
	RunE: runSuggest,
}

var (
	suggestFile          string
	suggestVersion       string
	suggestType          string
	suggestCompatibility string
)

func init() {
	suggestCmd.Flags().StringVarP(&suggestFile, "file", "f", "", "Local schema file")
	suggestCmd.Flags().StringVarP(&suggestVersion, "version", "v", "latest", "Schema version")
	suggestCmd.Flags().StringVarP(&suggestType, "type", "t", "", "Schema type override")
	suggestCmd.Flags().StringVar(&suggestCompatibility, "compatibility", "BACKWARD", "Compatibility mode")

	rootCmd.AddCommand(suggestCmd)
}

// Suggestion is the structured output
type Suggestion struct {
	Description   string   `json:"description"`
	Action        string   `json:"action"` // add, remove, rename, changeType
	Compatible    bool     `json:"compatible"`
	Compatibility string   `json:"compatibility"`
	Proposal      string   `json:"proposal,omitempty"`
	Explanation   string   `json:"explanation"`
	Warning       string   `json:"warning,omitempty"`
	Alternatives  []string `json:"alternatives,omitempty"`
	FieldName     string   `json:"fieldName,omitempty"`
	FieldDef      string   `json:"fieldDef,omitempty"`
}

func runSuggest(cmd *cobra.Command, args []string) error {
	var schemaContent string
	var schemaType string
	var description string
	compat := suggestCompatibility

	if suggestFile != "" {
		// Local file mode
		content, err := os.ReadFile(suggestFile)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		schemaContent = string(content)
		schemaType = suggestType
		if schemaType == "" {
			schemaType = detectSchemaType(schemaContent, suggestFile)
		}
		if len(args) < 1 {
			return fmt.Errorf("provide a change description, e.g.: srctl suggest --file schema.avsc \"add discount code\"")
		}
		description = strings.Join(args, " ")
	} else if len(args) >= 2 {
		// Registry mode: subject + description
		c, err := GetClient()
		if err != nil {
			return err
		}
		schema, err := c.GetSchema(args[0], suggestVersion)
		if err != nil {
			return fmt.Errorf("failed to get schema: %w", err)
		}
		schemaContent = schema.Schema
		schemaType = schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}
		description = strings.Join(args[1:], " ")

		// Try to get subject compatibility
		config, err := c.GetSubjectConfig(args[0], true)
		if err == nil && config != nil {
			level := config.CompatibilityLevel
			if level == "" {
				level = config.Compatibility
			}
			if level != "" {
				compat = level
			}
		}
	} else {
		return fmt.Errorf("usage: srctl suggest [subject] <description> or srctl suggest --file <file> <description>")
	}

	// Parse the action from description
	action, fieldName, targetName := parseChangeDescription(description)

	suggestion := generateSuggestion(schemaContent, schemaType, compat, description, action, fieldName, targetName)

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(suggestion)
	}

	displaySuggestion(suggestion)
	return nil
}

func parseChangeDescription(desc string) (action, fieldName, targetName string) {
	desc = strings.ToLower(strings.TrimSpace(desc))

	// "remove the notes field" / "remove notes" / "delete notes"
	removeRe := regexp.MustCompile(`(?:remove|delete|drop)\s+(?:the\s+)?(?:field\s+)?['"]?(\w+)['"]?`)
	if m := removeRe.FindStringSubmatch(desc); len(m) >= 2 {
		return "remove", m[1], ""
	}

	// "rename email to emailAddress" / "rename email as emailAddress"
	renameRe := regexp.MustCompile(`rename\s+(?:the\s+)?(?:field\s+)?['"]?(\w+)['"]?\s+(?:to|as)\s+['"]?(\w+)['"]?`)
	if m := renameRe.FindStringSubmatch(desc); len(m) >= 3 {
		return "rename", m[1], m[2]
	}

	// "change type of amount to string" / "change amount type to string"
	changeTypeRe := regexp.MustCompile(`change\s+(?:the\s+)?(?:type\s+of\s+)?['"]?(\w+)['"]?\s+(?:type\s+)?to\s+['"]?(\w+)['"]?`)
	if m := changeTypeRe.FindStringSubmatch(desc); len(m) >= 3 {
		return "changeType", m[1], m[2]
	}

	// "add discount code" / "add a discount code field" / "add field discountCode"
	addRe := regexp.MustCompile(`add\s+(?:a\s+)?(?:new\s+)?(?:field\s+)?['"]?([\w\s]+?)['"]?(?:\s+field)?$`)
	if m := addRe.FindStringSubmatch(desc); len(m) >= 2 {
		fieldName = toCamelCase(strings.TrimSpace(m[1]))
		return "add", fieldName, ""
	}

	// Fallback: try to extract a reasonable field name from the description
	// If the description doesn't match any known pattern, default to add
	// but flag it as a best-guess interpretation
	return "add", toCamelCase(desc), ""
}

// SupportedActions returns usage help for the suggest command's parser
func SupportedActions() string {
	return `Supported change descriptions:
  "add <field name>"                    - Add a new field
  "add a <descriptive name> field"      - Add with multi-word name (auto camelCase)
  "remove <field name>"                 - Remove a field
  "delete <field name>"                 - Remove a field
  "rename <old> to <new>"              - Rename a field
  "change <field> type to <type>"       - Change field type`
}

func toCamelCase(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return s
	}
	if len(words) == 1 {
		return words[0]
	}
	result := strings.ToLower(words[0])
	for _, w := range words[1:] {
		if len(w) > 0 {
			result += strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
		}
	}
	return result
}

func generateSuggestion(schemaContent, schemaType, compat, description, action, fieldName, targetName string) Suggestion {
	s := Suggestion{
		Description:   description,
		Action:        action,
		Compatibility: compat,
		FieldName:     fieldName,
	}

	// Only do detailed analysis for Avro (most common case)
	if strings.ToUpper(schemaType) != "AVRO" {
		return generateGenericSuggestion(s, schemaType, action, fieldName, targetName)
	}

	// Parse the schema
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaContent), &schema); err != nil {
		s.Warning = "Could not parse schema"
		return s
	}

	// Get existing fields
	existingFields := extractAvroFields(schema)

	switch action {
	case "add":
		s = suggestAddField(s, schema, existingFields, fieldName, compat)
	case "remove":
		s = suggestRemoveField(s, schema, existingFields, fieldName, compat)
	case "rename":
		s = suggestRenameField(s, schema, existingFields, fieldName, targetName, compat)
	case "changeType":
		s = suggestChangeType(s, schema, existingFields, fieldName, targetName, compat)
	default:
		s = suggestAddField(s, schema, existingFields, fieldName, compat)
	}

	return s
}

func suggestAddField(s Suggestion, schema map[string]interface{}, fields map[string]string, fieldName, compat string) Suggestion {
	// Check if field already exists
	if _, exists := fields[fieldName]; exists {
		s.Compatible = false
		s.Warning = fmt.Sprintf("Field '%s' already exists with type '%s'", fieldName, fields[fieldName])
		s.Explanation = "Cannot add a field that already exists. Use a different name."
		return s
	}

	compat = strings.ToUpper(compat)

	// For BACKWARD/FULL: must have default
	needsDefault := compat == "BACKWARD" || compat == "BACKWARD_TRANSITIVE" ||
		compat == "FULL" || compat == "FULL_TRANSITIVE"

	if needsDefault {
		fieldDef := fmt.Sprintf(`{"name": "%s", "type": ["null", "string"], "default": null}`, fieldName)
		s.FieldDef = fieldDef
		s.Compatible = true
		s.Proposal = fmt.Sprintf("Add nullable field '%s' with default null", fieldName)
		s.Explanation = fmt.Sprintf(
			"This is safe under %s compatibility because:\n"+
				"  - The field is nullable (union with null)\n"+
				"  - It has a default value (null)\n"+
				"  - Existing consumers will ignore the new field\n"+
				"  - Existing messages without this field will deserialize with null",
			compat)
	} else {
		fieldDef := fmt.Sprintf(`{"name": "%s", "type": "string"}`, fieldName)
		s.FieldDef = fieldDef
		s.Compatible = true
		s.Proposal = fmt.Sprintf("Add field '%s' of type string", fieldName)
		s.Explanation = fmt.Sprintf(
			"Under %s compatibility, new fields don't need defaults.\n"+
				"  However, consider adding a default for future flexibility.",
			compat)
	}

	return s
}

func suggestRemoveField(s Suggestion, schema map[string]interface{}, fields map[string]string, fieldName, compat string) Suggestion {
	// Check if field exists
	fieldType, exists := fields[fieldName]
	if !exists {
		s.Warning = fmt.Sprintf("Field '%s' does not exist in the schema", fieldName)
		s.Explanation = "Cannot remove a field that doesn't exist."
		return s
	}

	compat = strings.ToUpper(compat)

	isBackwardCompat := compat == "BACKWARD" || compat == "BACKWARD_TRANSITIVE" ||
		compat == "FULL" || compat == "FULL_TRANSITIVE"

	if isBackwardCompat {
		s.Compatible = false
		s.Warning = fmt.Sprintf("Removing field '%s' (%s) is NOT compatible under %s", fieldName, fieldType, compat)
		s.Explanation = fmt.Sprintf(
			"Removing field '%s' means existing data with this field cannot\n"+
				"  be read by the new schema (no matching field to deserialize into).",
			fieldName)
		s.Alternatives = []string{
			fmt.Sprintf("Mark as deprecated: change doc to \"DEPRECATED: %s\"", fieldName),
			fmt.Sprintf("Keep but ignore: leave '%s' in the schema, stop populating it in producers", fieldName),
			"Change compatibility to NONE (not recommended for production)",
		}
	} else {
		s.Compatible = true
		s.Proposal = fmt.Sprintf("Remove field '%s' from the schema", fieldName)
		s.Explanation = fmt.Sprintf(
			"Under %s compatibility, field removal is allowed.\n"+
				"  However, ensure no active consumers depend on this field.",
			compat)
	}

	return s
}

func suggestRenameField(s Suggestion, schema map[string]interface{}, fields map[string]string, oldName, newName, compat string) Suggestion {
	_, exists := fields[oldName]
	if !exists {
		s.Warning = fmt.Sprintf("Field '%s' does not exist in the schema", oldName)
		return s
	}

	if _, exists := fields[newName]; exists {
		s.Warning = fmt.Sprintf("Target field '%s' already exists", newName)
		return s
	}

	compat = strings.ToUpper(compat)

	s.Compatible = false
	s.Warning = fmt.Sprintf("Renaming '%s' to '%s' is NOT compatible under %s", oldName, newName, compat)
	s.Explanation = fmt.Sprintf(
		"A rename is equivalent to removing '%s' and adding '%s'.\n"+
			"  The removal breaks backward compatibility since existing messages\n"+
			"  contain the old field name.",
		oldName, newName)

	fieldType := fields[oldName]
	newFieldDef := fmt.Sprintf(`{"name": "%s", "type": ["null", "%s"], "default": null}`, newName, fieldType)
	s.FieldDef = newFieldDef
	s.Alternatives = []string{
		fmt.Sprintf("Add '%s' as a new field alongside '%s' (recommended):\n    %s", newName, oldName, newFieldDef),
		fmt.Sprintf("Add aliases: set \"aliases\": [\"%s\"] on the new field", oldName),
		fmt.Sprintf("Deprecate '%s' (add doc: \"DEPRECATED, use %s\") and add '%s' as new field", oldName, newName, newName),
	}

	return s
}

func suggestChangeType(s Suggestion, schema map[string]interface{}, fields map[string]string, fieldName, newType, compat string) Suggestion {
	oldType, exists := fields[fieldName]
	if !exists {
		s.Warning = fmt.Sprintf("Field '%s' does not exist in the schema", fieldName)
		return s
	}

	compat = strings.ToUpper(compat)

	// Check Avro type promotion rules
	promoted := isAvroTypePromotion(oldType, newType)

	if promoted {
		s.Compatible = true
		s.Proposal = fmt.Sprintf("Change '%s' type from '%s' to '%s' (type promotion)", fieldName, oldType, newType)
		s.Explanation = fmt.Sprintf(
			"Avro supports promoting %s to %s.\n"+
				"  This is safe because the old data can be read with the new type.",
			oldType, newType)
		s.FieldDef = fmt.Sprintf(`{"name": "%s", "type": "%s"}`, fieldName, newType)
	} else {
		s.Compatible = false
		s.Warning = fmt.Sprintf("Changing '%s' from '%s' to '%s' is NOT compatible", fieldName, oldType, newType)
		s.Explanation = fmt.Sprintf(
			"Type change from %s to %s is not an Avro type promotion.\n"+
				"  Existing data written as %s cannot be read as %s.",
			oldType, newType, oldType, newType)

		newFieldName := fieldName + strings.ToUpper(newType[:1]) + newType[1:]
		newFieldDef := fmt.Sprintf(`{"name": "%s", "type": ["null", "%s"], "default": null}`, newFieldName, newType)
		s.Alternatives = []string{
			fmt.Sprintf("Add a new field '%s' with the desired type:\n    %s", newFieldName, newFieldDef),
			fmt.Sprintf("Keep '%s' as %s and add a computed field in the consumer", fieldName, oldType),
		}
	}

	return s
}

// isAvroTypePromotion checks if oldType can be promoted to newType per Avro spec
func isAvroTypePromotion(oldType, newType string) bool {
	promotions := map[string][]string{
		"int":    {"long", "float", "double"},
		"long":   {"float", "double"},
		"float":  {"double"},
		"string": {"bytes"},
		"bytes":  {"string"},
	}

	if allowed, ok := promotions[oldType]; ok {
		for _, a := range allowed {
			if a == newType {
				return true
			}
		}
	}
	return false
}

func generateGenericSuggestion(s Suggestion, schemaType, action, fieldName, targetName string) Suggestion {
	switch action {
	case "add":
		s.Compatible = true
		switch strings.ToUpper(schemaType) {
		case "PROTOBUF":
			s.Proposal = fmt.Sprintf("Add field to the message:\n  optional string %s = <next_number>;", fieldName)
			s.Explanation = "In Protobuf, adding optional fields is always backward compatible."
		case "JSON":
			s.Proposal = fmt.Sprintf("Add property to the schema:\n  \"%s\": {\"type\": \"string\"}", fieldName)
			s.Explanation = "In JSON Schema, adding properties is generally safe if additionalProperties is not false."
		}
	case "remove":
		s.Compatible = false
		s.Warning = fmt.Sprintf("Removing '%s' may break compatibility", fieldName)
		s.Explanation = "Field removal is generally a breaking change. Consider deprecating instead."
	case "rename":
		s.Compatible = false
		s.Warning = fmt.Sprintf("Renaming '%s' to '%s' is a breaking change", fieldName, targetName)
		s.Explanation = "Add the new field alongside the old one instead."
	}
	return s
}

func displaySuggestion(s Suggestion) {
	output.Header("Suggestion: %s", s.Description)

	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	output.Info("Action: %s", s.Action)
	output.Info("Compatibility mode: %s", s.Compatibility)
	if s.FieldName != "" {
		output.Info("Field: %s", s.FieldName)
	}
	fmt.Println()

	if s.Warning != "" {
		fmt.Printf("  %s %s\n\n", red("WARNING:"), s.Warning)
	}

	if s.Compatible {
		fmt.Printf("  %s %s\n\n", green("COMPATIBLE:"), s.Proposal)
	}

	if s.Explanation != "" {
		output.SubHeader("Explanation")
		for _, line := range strings.Split(s.Explanation, "\n") {
			fmt.Printf("  %s\n", line)
		}
		fmt.Println()
	}

	if s.FieldDef != "" {
		output.SubHeader("Field Definition")
		// Pretty print the JSON
		var parsed interface{}
		if err := json.Unmarshal([]byte(s.FieldDef), &parsed); err == nil {
			pretty, _ := json.MarshalIndent(parsed, "  ", "  ")
			fmt.Printf("  %s\n\n", string(pretty))
		} else {
			fmt.Printf("  %s\n\n", s.FieldDef)
		}
	}

	if len(s.Alternatives) > 0 {
		output.SubHeader("Alternatives")
		for i, alt := range s.Alternatives {
			fmt.Printf("  %s %s\n", yellow(fmt.Sprintf("%d.", i+1)), alt)
		}
		fmt.Println()
	}
}
