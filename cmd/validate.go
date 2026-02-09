package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/output"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate schema syntax and compatibility offline",
	GroupID: groupSchema,
	Long: `Validate schemas without requiring a running Schema Registry.

Supports three modes:
  1. Syntax validation - check if a schema file is well-formed
  2. Compatibility check - compare two local files for compatibility
  3. Directory validation - validate all schemas in a directory

When --subject is used, the schema is checked against the latest version
in the registry (requires connectivity).

Examples:
  # Validate syntax of a schema file
  srctl validate --file order.avsc

  # Validate with explicit type
  srctl validate --file order.proto --type PROTOBUF

  # Check compatibility between two local files
  srctl validate --file order-v2.avsc --against order-v1.avsc

  # Check with specific compatibility mode
  srctl validate --file order-v2.avsc --against order-v1.avsc --compatibility FULL

  # Validate all schemas in a directory
  srctl validate --dir ./schemas/

  # Check compatibility against latest version in registry
  srctl validate --file order-v2.avsc --subject orders-value`,
	RunE: runValidate,
}

var (
	validateFile          string
	validateType          string
	validateAgainst       string
	validateCompatibility string
	validateDir           string
	validateSubject       string
)

func init() {
	validateCmd.Flags().StringVarP(&validateFile, "file", "f", "", "Path to schema file to validate")
	validateCmd.Flags().StringVarP(&validateType, "type", "t", "", "Schema type: AVRO, PROTOBUF, JSON (auto-detected)")
	validateCmd.Flags().StringVar(&validateAgainst, "against", "", "Previous schema file for compatibility check")
	validateCmd.Flags().StringVar(&validateCompatibility, "compatibility", "BACKWARD", "Compatibility mode: BACKWARD, FORWARD, FULL, NONE")
	validateCmd.Flags().StringVar(&validateDir, "dir", "", "Directory of schemas to validate")
	validateCmd.Flags().StringVar(&validateSubject, "subject", "", "Subject to check compatibility against (requires registry)")

	rootCmd.AddCommand(validateCmd)
}

// ValidationIssue represents a single validation problem
type ValidationIssue struct {
	Severity string `json:"severity"` // ERROR, WARNING
	Message  string `json:"message"`
	Fix      string `json:"fix,omitempty"`
	Field    string `json:"field,omitempty"`
}

// ValidationResult is the outcome of validating a schema
type ValidationResult struct {
	File       string            `json:"file"`
	SchemaType string            `json:"schemaType"`
	Valid      bool              `json:"valid"`
	Issues     []ValidationIssue `json:"issues,omitempty"`
}

func runValidate(cmd *cobra.Command, args []string) error {
	// Directory validation mode
	if validateDir != "" {
		return runValidateDir(validateDir)
	}

	if validateFile == "" {
		return fmt.Errorf("either --file or --dir is required")
	}

	content, err := os.ReadFile(validateFile)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	schemaType := validateType
	if schemaType == "" {
		schemaType = detectSchemaType(string(content), validateFile)
	}

	// Compatibility check against another local file
	if validateAgainst != "" {
		return runValidateCompatibility(string(content), schemaType)
	}

	// Compatibility check against registry
	if validateSubject != "" {
		return runValidateAgainstRegistry(string(content), schemaType)
	}

	// Syntax-only validation
	return runValidateSyntax(string(content), schemaType, validateFile)
}

// ========================
// Syntax validation
// ========================

func runValidateSyntax(content, schemaType, filename string) error {
	output.Header("Schema Validation: %s", filename)
	output.Info("Schema type: %s", schemaType)
	fmt.Println()

	result := validateSchemaSyntax(content, schemaType, filename)

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(result)
	}

	displayValidationResult(result)
	return nil
}

func validateSchemaSyntax(content, schemaType, filename string) ValidationResult {
	result := ValidationResult{
		File:       filename,
		SchemaType: schemaType,
		Valid:      true,
	}

	switch strings.ToUpper(schemaType) {
	case "AVRO":
		result.Issues = validateAvroSyntax(content)
	case "PROTOBUF":
		result.Issues = validateProtobufSyntax(content)
	case "JSON":
		result.Issues = validateJSONSchemaSyntax(content)
	default:
		result.Issues = append(result.Issues, ValidationIssue{
			Severity: "ERROR",
			Message:  fmt.Sprintf("Unknown schema type: %s", schemaType),
			Fix:      "Use --type to specify AVRO, PROTOBUF, or JSON",
		})
	}

	for _, issue := range result.Issues {
		if issue.Severity == "ERROR" {
			result.Valid = false
			break
		}
	}

	return result
}

func validateAvroSyntax(content string) []ValidationIssue {
	var issues []ValidationIssue

	// Must be valid JSON
	var schema interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return []ValidationIssue{{
			Severity: "ERROR",
			Message:  fmt.Sprintf("Invalid JSON: %v", err),
			Fix:      "Ensure the schema is valid JSON",
		}}
	}

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return []ValidationIssue{{
			Severity: "ERROR",
			Message:  "Schema must be a JSON object",
			Fix:      "Wrap in a record type: {\"type\": \"record\", \"name\": \"...\", \"fields\": [...]}",
		}}
	}

	// Check required 'type' field
	typeVal, hasType := schemaMap["type"]
	if !hasType {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Missing required field: 'type'",
			Fix:      "Add \"type\": \"record\" (or enum, fixed, array, map)",
		})
		return issues
	}

	typeName, _ := typeVal.(string)

	switch typeName {
	case "record":
		issues = append(issues, validateAvroRecord(schemaMap)...)
	case "enum":
		issues = append(issues, validateAvroEnum(schemaMap)...)
	case "fixed":
		issues = append(issues, validateAvroFixed(schemaMap)...)
	case "array":
		if _, ok := schemaMap["items"]; !ok {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  "Array type missing 'items' field",
				Fix:      "Add \"items\": \"string\" (or another type)",
			})
		}
	case "map":
		if _, ok := schemaMap["values"]; !ok {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  "Map type missing 'values' field",
				Fix:      "Add \"values\": \"string\" (or another type)",
			})
		}
	default:
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  fmt.Sprintf("Unusual top-level type: '%s'", typeName),
			Fix:      "Top-level Avro schemas are typically 'record' types",
		})
	}

	return issues
}

func validateAvroRecord(schema map[string]interface{}) []ValidationIssue {
	var issues []ValidationIssue

	// Check name
	name, hasName := schema["name"]
	if !hasName {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Record missing required field: 'name'",
			Fix:      "Add \"name\": \"MyRecord\"",
		})
	} else if _, ok := name.(string); !ok {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "'name' must be a string",
		})
	}

	// Check namespace (warning if missing)
	if _, hasNS := schema["namespace"]; !hasNS {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Record missing 'namespace'",
			Fix:      "Add \"namespace\": \"com.example\" for proper namespacing",
		})
	}

	// Check fields
	fields, hasFields := schema["fields"]
	if !hasFields {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Record missing required field: 'fields'",
			Fix:      "Add \"fields\": [{\"name\": \"id\", \"type\": \"string\"}]",
		})
		return issues
	}

	fieldList, ok := fields.([]interface{})
	if !ok {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "'fields' must be an array",
			Fix:      "Change 'fields' to an array of field objects",
		})
		return issues
	}

	if len(fieldList) == 0 {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Record has no fields",
			Fix:      "Add at least one field to the record",
		})
	}

	fieldNames := make(map[string]bool)
	for i, f := range fieldList {
		field, ok := f.(map[string]interface{})
		if !ok {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Field %d is not a JSON object", i),
			})
			continue
		}

		fname, hasName := field["name"].(string)
		if !hasName {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Field %d missing 'name'", i),
				Field:    fmt.Sprintf("fields[%d]", i),
			})
		} else {
			if fieldNames[fname] {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Message:  fmt.Sprintf("Duplicate field name: '%s'", fname),
					Field:    fname,
					Fix:      "Each field name must be unique within a record",
				})
			}
			fieldNames[fname] = true
		}

		if _, hasType := field["type"]; !hasType {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Field '%s' missing 'type'", fname),
				Field:    fname,
				Fix:      fmt.Sprintf("Add \"type\": \"string\" to field '%s'", fname),
			})
		}

		// Recursively validate nested records
		if fieldType, ok := field["type"].(map[string]interface{}); ok {
			if ft, ok := fieldType["type"].(string); ok && ft == "record" {
				nested := validateAvroRecord(fieldType)
				for j := range nested {
					nested[j].Field = fname + "." + nested[j].Field
				}
				issues = append(issues, nested...)
			}
		}
	}

	return issues
}

func validateAvroEnum(schema map[string]interface{}) []ValidationIssue {
	var issues []ValidationIssue

	if _, hasName := schema["name"]; !hasName {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Enum missing required field: 'name'",
		})
	}

	symbols, hasSymbols := schema["symbols"]
	if !hasSymbols {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Enum missing required field: 'symbols'",
			Fix:      "Add \"symbols\": [\"A\", \"B\", \"C\"]",
		})
		return issues
	}

	symList, ok := symbols.([]interface{})
	if !ok {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "'symbols' must be an array of strings",
		})
		return issues
	}

	if len(symList) == 0 {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Enum must have at least one symbol",
		})
	}

	symNames := make(map[string]bool)
	for i, s := range symList {
		str, ok := s.(string)
		if !ok {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Symbol %d is not a string", i),
			})
			continue
		}
		if symNames[str] {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Duplicate symbol: '%s'", str),
			})
		}
		symNames[str] = true
	}

	return issues
}

func validateAvroFixed(schema map[string]interface{}) []ValidationIssue {
	var issues []ValidationIssue

	if _, hasName := schema["name"]; !hasName {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Fixed type missing required field: 'name'",
		})
	}

	size, hasSize := schema["size"]
	if !hasSize {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Fixed type missing required field: 'size'",
			Fix:      "Add \"size\": 16 (number of bytes)",
		})
	} else {
		sizeNum, ok := size.(float64)
		if !ok || sizeNum <= 0 {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  "'size' must be a positive integer",
			})
		}
	}

	return issues
}

func validateProtobufSyntax(content string) []ValidationIssue {
	var issues []ValidationIssue

	// Check syntax declaration
	syntaxRe := regexp.MustCompile(`syntax\s*=\s*"(proto[23])"\s*;`)
	if !syntaxRe.MatchString(content) {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Missing or invalid syntax declaration",
			Fix:      "Add: syntax = \"proto3\";",
		})
	}

	// Check for at least one message
	messageRe := regexp.MustCompile(`(?m)^message\s+(\w+)\s*\{`)
	messages := messageRe.FindAllStringSubmatch(content, -1)
	if len(messages) == 0 {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "No message definitions found",
			Fix:      "Add at least one message definition",
		})
	}

	// Check brace matching
	openBraces := strings.Count(content, "{")
	closeBraces := strings.Count(content, "}")
	if openBraces != closeBraces {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Message:  fmt.Sprintf("Unmatched braces: %d open, %d close", openBraces, closeBraces),
			Fix:      "Check for missing or extra braces",
		})
	}

	// Check field number duplicates within each message
	for _, msg := range messages {
		msgName := msg[1]
		msgContent := extractProtobufMessageBody(content, msgName)
		if msgContent != "" {
			fieldNumRe := regexp.MustCompile(`=\s*(\d+)\s*;`)
			fieldNums := fieldNumRe.FindAllStringSubmatch(msgContent, -1)
			seen := make(map[string]bool)
			for _, fn := range fieldNums {
				num := fn[1]
				if seen[num] {
					issues = append(issues, ValidationIssue{
						Severity: "ERROR",
						Message:  fmt.Sprintf("Duplicate field number %s in message '%s'", num, msgName),
						Field:    msgName,
						Fix:      "Each field must have a unique number within its message",
					})
				}
				seen[num] = true
			}
		}
	}

	// Check for semicolons on fields
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "/*") {
			continue
		}
		// Field lines should end with semicolon
		if regexp.MustCompile(`^\w+\s+\w+\s*=\s*\d+`).MatchString(trimmed) {
			if !strings.HasSuffix(trimmed, ";") && !strings.Contains(trimmed, "[") {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Message:  fmt.Sprintf("Line %d: field definition missing semicolon", i+1),
					Fix:      "Add a semicolon at the end of the field definition",
				})
			}
		}
	}

	return issues
}

func extractProtobufMessageBody(content, msgName string) string {
	re := regexp.MustCompile(fmt.Sprintf(`message\s+%s\s*\{`, regexp.QuoteMeta(msgName)))
	loc := re.FindStringIndex(content)
	if loc == nil {
		return ""
	}
	depth := 0
	start := -1
	for i := loc[0]; i < len(content); i++ {
		if content[i] == '{' {
			if depth == 0 {
				start = i + 1
			}
			depth++
		} else if content[i] == '}' {
			depth--
			if depth == 0 {
				return content[start:i]
			}
		}
	}
	return ""
}

func validateJSONSchemaSyntax(content string) []ValidationIssue {
	var issues []ValidationIssue

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return []ValidationIssue{{
			Severity: "ERROR",
			Message:  fmt.Sprintf("Invalid JSON: %v", err),
			Fix:      "Ensure the schema is valid JSON",
		}}
	}

	// Check $schema
	if _, has := schema["$schema"]; !has {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Missing '$schema' declaration",
			Fix:      "Add \"$schema\": \"http://json-schema.org/draft-07/schema#\"",
		})
	}

	// Check type
	typeVal, hasType := schema["type"]
	if !hasType {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Missing 'type' field",
			Fix:      "Add \"type\": \"object\"",
		})
	} else {
		validTypes := map[string]bool{
			"object": true, "array": true, "string": true,
			"number": true, "integer": true, "boolean": true, "null": true,
		}
		if typeStr, ok := typeVal.(string); ok && !validTypes[typeStr] {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Message:  fmt.Sprintf("Invalid type: '%s'", typeStr),
				Fix:      "Use one of: object, array, string, number, integer, boolean, null",
			})
		}
	}

	// Validate properties if object type
	if typeVal == "object" {
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			for propName, propVal := range props {
				if propMap, ok := propVal.(map[string]interface{}); ok {
					// Recursively check nested objects
					if propMap["type"] == "object" {
						nested := validateJSONSchemaObject(propMap, propName)
						issues = append(issues, nested...)
					}
				}
			}
		}

		// Check $ref values look valid
		issues = append(issues, validateJSONSchemaRefs(schema, "")...)
	}

	return issues
}

func validateJSONSchemaObject(schema map[string]interface{}, path string) []ValidationIssue {
	var issues []ValidationIssue

	if props, ok := schema["properties"].(map[string]interface{}); ok {
		for propName, propVal := range props {
			propPath := path + "." + propName
			if propMap, ok := propVal.(map[string]interface{}); ok {
				if propMap["type"] == "object" {
					issues = append(issues, validateJSONSchemaObject(propMap, propPath)...)
				}
			}
		}
	}

	return issues
}

func validateJSONSchemaRefs(schema map[string]interface{}, path string) []ValidationIssue {
	var issues []ValidationIssue

	for key, val := range schema {
		if key == "$ref" {
			if refStr, ok := val.(string); ok {
				if refStr == "" {
					issues = append(issues, ValidationIssue{
						Severity: "ERROR",
						Message:  fmt.Sprintf("Empty $ref at '%s'", path),
						Fix:      "Provide a valid reference URI",
					})
				}
			}
		}
		if subMap, ok := val.(map[string]interface{}); ok {
			childPath := path + "." + key
			issues = append(issues, validateJSONSchemaRefs(subMap, childPath)...)
		}
	}

	return issues
}

// ========================
// Compatibility checking
// ========================

func runValidateCompatibility(newContent, schemaType string) error {
	oldContent, err := os.ReadFile(validateAgainst)
	if err != nil {
		return fmt.Errorf("failed to read --against file: %w", err)
	}

	output.Header("Compatibility Check")
	output.Info("New schema: %s", validateFile)
	output.Info("Old schema: %s", validateAgainst)
	output.Info("Mode: %s", validateCompatibility)
	fmt.Println()

	// First validate syntax of both schemas
	newResult := validateSchemaSyntax(newContent, schemaType, validateFile)
	if !newResult.Valid {
		output.Error("New schema has syntax errors:")
		displayValidationResult(newResult)
		return fmt.Errorf("new schema is not valid")
	}

	oldResult := validateSchemaSyntax(string(oldContent), schemaType, validateAgainst)
	if !oldResult.Valid {
		output.Error("Old schema has syntax errors:")
		displayValidationResult(oldResult)
		return fmt.Errorf("old schema is not valid")
	}

	output.Success("Both schemas are syntactically valid")
	fmt.Println()

	// Run compatibility check
	issues := checkCompatibility(newContent, string(oldContent), schemaType, validateCompatibility)

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(map[string]interface{}{
			"compatible": len(issues) == 0,
			"mode":       validateCompatibility,
			"issues":     issues,
		})
	}

	if len(issues) == 0 {
		output.Success("Schema is compatible (%s)", validateCompatibility)
		return nil
	}

	displayCompatibilityIssues(issues)
	return fmt.Errorf("schema is not compatible")
}

func runValidateAgainstRegistry(content, schemaType string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	output.Header("Registry Compatibility Check")
	output.Info("New schema: %s", validateFile)
	output.Info("Subject: %s", validateSubject)
	fmt.Println()

	// Get latest schema from registry
	schema, err := c.GetSchema(validateSubject, "latest")
	if err != nil {
		return fmt.Errorf("failed to get latest schema for '%s': %w", validateSubject, err)
	}

	output.Info("Comparing against version %d (schema ID %d)", schema.Version, schema.ID)
	fmt.Println()

	// Get subject compatibility config
	compat := validateCompatibility
	config, err := c.GetSubjectConfig(validateSubject, true)
	if err == nil && config != nil {
		level := config.CompatibilityLevel
		if level == "" {
			level = config.Compatibility
		}
		if level != "" {
			compat = level
			output.Info("Using subject compatibility: %s", compat)
		}
	}

	issues := checkCompatibility(content, schema.Schema, schemaType, compat)

	if len(issues) == 0 {
		output.Success("Schema is compatible with %s@%d (%s)", validateSubject, schema.Version, compat)
		return nil
	}

	displayCompatibilityIssues(issues)
	return fmt.Errorf("schema is not compatible")
}

// checkCompatibility performs a local compatibility check between two schemas
func checkCompatibility(newContent, oldContent, schemaType, mode string) []ValidationIssue {
	switch strings.ToUpper(schemaType) {
	case "AVRO":
		return checkAvroCompatibility(newContent, oldContent, mode)
	case "JSON":
		return checkJSONSchemaCompatibility(newContent, oldContent, mode)
	default:
		// For Protobuf and unknown types, do basic field-level check
		return checkGenericCompatibility(newContent, oldContent, mode)
	}
}

func checkAvroCompatibility(newContent, oldContent, mode string) []ValidationIssue {
	var issues []ValidationIssue

	var newSchema, oldSchema interface{}
	json.Unmarshal([]byte(newContent), &newSchema)
	json.Unmarshal([]byte(oldContent), &oldSchema)

	newFields := extractAvroFieldsDeep(newSchema, "")
	oldFields := extractAvroFieldsDeep(oldSchema, "")

	mode = strings.ToUpper(mode)

	// BACKWARD: new schema can read data written by old schema
	// All fields in old must exist in new OR new field must have a default
	if mode == "BACKWARD" || mode == "BACKWARD_TRANSITIVE" || mode == "FULL" || mode == "FULL_TRANSITIVE" {
		for fieldPath, oldField := range oldFields {
			newField, exists := newFields[fieldPath]
			if !exists {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    fieldPath,
					Message:  fmt.Sprintf("Field '%s' was removed", fieldPath),
					Fix:      fmt.Sprintf("Keep the field '%s', or change compatibility to NONE", fieldPath),
				})
			} else if oldField.Type != newField.Type {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    fieldPath,
					Message:  fmt.Sprintf("Field '%s' type changed from '%s' to '%s'", fieldPath, oldField.Type, newField.Type),
					Fix:      fmt.Sprintf("Add a new field instead of changing the type of '%s'", fieldPath),
				})
			}
		}
		// New fields should have defaults for backward compat
		for fieldPath, newField := range newFields {
			if _, exists := oldFields[fieldPath]; !exists {
				if !newField.HasDefault && !newField.IsNullable {
					issues = append(issues, ValidationIssue{
						Severity: "WARNING",
						Field:    fieldPath,
						Message:  fmt.Sprintf("New field '%s' has no default value", fieldPath),
						Fix:      fmt.Sprintf("Add a default value or make '%s' nullable: [\"null\", \"%s\"]", fieldPath, newField.Type),
					})
				}
			}
		}
	}

	// FORWARD: old schema can read data written by new schema
	// All fields in new must exist in old OR old field must have a default
	if mode == "FORWARD" || mode == "FORWARD_TRANSITIVE" || mode == "FULL" || mode == "FULL_TRANSITIVE" {
		for fieldPath, newField := range newFields {
			oldField, exists := oldFields[fieldPath]
			if !exists {
				// Adding a field without the old schema having a default for it
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    fieldPath,
					Message:  fmt.Sprintf("New field '%s' added (old consumers cannot read it)", fieldPath),
					Fix:      fmt.Sprintf("Ensure old consumers can ignore unknown field '%s', or use BACKWARD compatibility", fieldPath),
				})
			} else if newField.Type != oldField.Type {
				// Already reported in BACKWARD section if FULL
				if mode == "FORWARD" || mode == "FORWARD_TRANSITIVE" {
					issues = append(issues, ValidationIssue{
						Severity: "ERROR",
						Field:    fieldPath,
						Message:  fmt.Sprintf("Field '%s' type changed from '%s' to '%s'", fieldPath, oldField.Type, newField.Type),
						Fix:      fmt.Sprintf("Add a new field instead of changing the type of '%s'", fieldPath),
					})
				}
			}
		}
	}

	return issues
}

type fieldInfo struct {
	Type       string
	HasDefault bool
	IsNullable bool
}

func extractAvroFieldsDeep(schema interface{}, prefix string) map[string]fieldInfo {
	fields := make(map[string]fieldInfo)

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

		_, hasDefault := field["default"]
		typeStr := formatAvroType(field["type"])
		isNullable := false

		// Check if the type is a union containing null
		if unionTypes, ok := field["type"].([]interface{}); ok {
			for _, ut := range unionTypes {
				if utStr, ok := ut.(string); ok && utStr == "null" {
					isNullable = true
					break
				}
			}
		}

		fields[path] = fieldInfo{
			Type:       typeStr,
			HasDefault: hasDefault,
			IsNullable: isNullable,
		}

		// Recurse into nested records
		if fieldType, ok := field["type"].(map[string]interface{}); ok {
			if ft, ok := fieldType["type"].(string); ok && ft == "record" {
				nested := extractAvroFieldsDeep(fieldType, path)
				for k, v := range nested {
					fields[k] = v
				}
			}
		}
	}

	return fields
}

func checkJSONSchemaCompatibility(newContent, oldContent, mode string) []ValidationIssue {
	var issues []ValidationIssue

	var newSchema, oldSchema map[string]interface{}
	json.Unmarshal([]byte(newContent), &newSchema)
	json.Unmarshal([]byte(oldContent), &oldSchema)

	newProps := extractJSONSchemaProperties(newSchema, "")
	oldProps := extractJSONSchemaProperties(oldSchema, "")

	mode = strings.ToUpper(mode)

	if mode == "BACKWARD" || mode == "BACKWARD_TRANSITIVE" || mode == "FULL" || mode == "FULL_TRANSITIVE" {
		for path, oldType := range oldProps {
			newType, exists := newProps[path]
			if !exists {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    path,
					Message:  fmt.Sprintf("Property '%s' was removed", path),
					Fix:      fmt.Sprintf("Keep the property '%s', or change compatibility to NONE", path),
				})
			} else if oldType != newType {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    path,
					Message:  fmt.Sprintf("Property '%s' type changed from '%s' to '%s'", path, oldType, newType),
					Fix:      fmt.Sprintf("Add a new property instead of changing '%s'", path),
				})
			}
		}
	}

	if mode == "FORWARD" || mode == "FORWARD_TRANSITIVE" || mode == "FULL" || mode == "FULL_TRANSITIVE" {
		for path := range newProps {
			if _, exists := oldProps[path]; !exists {
				issues = append(issues, ValidationIssue{
					Severity: "ERROR",
					Field:    path,
					Message:  fmt.Sprintf("New property '%s' added (old consumers cannot read it)", path),
					Fix:      "Ensure old consumers can handle unknown properties",
				})
			}
		}
	}

	return issues
}

func extractJSONSchemaProperties(schema map[string]interface{}, prefix string) map[string]string {
	props := make(map[string]string)

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return props
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
		props[path] = typeStr

		if typeStr == "object" {
			nested := extractJSONSchemaProperties(propMap, path)
			for k, v := range nested {
				props[k] = v
			}
		}
	}

	return props
}

func checkGenericCompatibility(newContent, oldContent, mode string) []ValidationIssue {
	// For unsupported types, just report that offline check is limited
	return []ValidationIssue{{
		Severity: "WARNING",
		Message:  "Offline compatibility check is limited for this schema type",
		Fix:      "Use --subject to check compatibility against the registry",
	}}
}

// ========================
// Directory validation
// ========================

func runValidateDir(dir string) error {
	output.Header("Directory Validation: %s", dir)

	// Find schema files
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".avsc" || ext == ".avro" || ext == ".proto" || ext == ".json" {
			// Skip manifest.json
			if filepath.Base(path) == "manifest.json" {
				return nil
			}
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(files) == 0 {
		output.Warning("No schema files found in %s", dir)
		return nil
	}

	output.Info("Found %d schema files", len(files))
	fmt.Println()

	var results []ValidationResult
	var errorCount int

	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			results = append(results, ValidationResult{
				File:  file,
				Valid: false,
				Issues: []ValidationIssue{{
					Severity: "ERROR",
					Message:  fmt.Sprintf("Cannot read file: %v", err),
				}},
			})
			errorCount++
			continue
		}

		relPath, _ := filepath.Rel(dir, file)
		schemaType := detectSchemaType(string(content), file)
		result := validateSchemaSyntax(string(content), schemaType, relPath)
		results = append(results, result)

		if !result.Valid {
			errorCount++
		}
	}

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(results)
	}

	// Display results
	for _, r := range results {
		displayValidationResult(r)
	}

	fmt.Println()
	if errorCount == 0 {
		output.Success("All %d schemas are valid", len(files))
	} else {
		output.Error("%d of %d schemas have errors", errorCount, len(files))
	}

	if errorCount > 0 {
		return fmt.Errorf("%d schemas have validation errors", errorCount)
	}
	return nil
}

// ========================
// Display helpers
// ========================

func displayValidationResult(result ValidationResult) {
	if result.Valid {
		output.Success("%s (%s) - valid", result.File, result.SchemaType)
	} else {
		output.Error("%s (%s) - invalid", result.File, result.SchemaType)
	}

	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	for _, issue := range result.Issues {
		marker := yellow("WARNING")
		if issue.Severity == "ERROR" {
			marker = red("ERROR")
		}

		fieldStr := ""
		if issue.Field != "" {
			fieldStr = fmt.Sprintf(" [%s]", issue.Field)
		}

		fmt.Printf("  %s%s: %s\n", marker, fieldStr, issue.Message)
		if issue.Fix != "" {
			fmt.Printf("    Fix: %s\n", issue.Fix)
		}
	}
}

func displayCompatibilityIssues(issues []ValidationIssue) {
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	var errors, warnings int
	for _, issue := range issues {
		if issue.Severity == "ERROR" {
			errors++
		} else {
			warnings++
		}
	}

	if errors > 0 {
		output.Error("Schema is NOT compatible (%d error(s), %d warning(s))", errors, warnings)
	} else {
		output.Warning("Schema has compatibility warnings (%d warning(s))", warnings)
	}
	fmt.Println()

	output.SubHeader("Issues")
	for _, issue := range issues {
		marker := yellow("WARN")
		if issue.Severity == "ERROR" {
			marker = red("ERROR")
		}

		fieldStr := ""
		if issue.Field != "" {
			fieldStr = fmt.Sprintf(" [%s]", issue.Field)
		}

		fmt.Printf("  %s%s: %s\n", marker, fieldStr, issue.Message)
		if issue.Fix != "" {
			fmt.Printf("    Fix: %s\n", issue.Fix)
		}
	}
}
