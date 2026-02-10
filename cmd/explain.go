package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/output"
)

var explainCmd = &cobra.Command{
	Use:   "explain [subject]",
	Short: "Describe a schema in human-readable terms",
	GroupID: groupSchema,
	Long: `Produce a human-readable description of a schema, including field names,
types, documentation, references, and structure. Useful for understanding
schemas quickly or for AI coding agents that need schema context.

Works with schemas from the registry or local files.

Examples:
  # Explain a schema from the registry
  srctl explain orders-value

  # Explain a specific version
  srctl explain orders-value --version 2

  # Explain a local file (no registry needed)
  srctl explain --file order.avsc

  # Structured JSON output
  srctl explain orders-value -o json`,
	RunE: runExplain,
}

var (
	explainFile    string
	explainVersion string
	explainType    string
)

func init() {
	explainCmd.Flags().StringVarP(&explainFile, "file", "f", "", "Local schema file (no registry needed)")
	explainCmd.Flags().StringVarP(&explainVersion, "version", "v", "latest", "Schema version")
	explainCmd.Flags().StringVarP(&explainType, "type", "t", "", "Schema type override (auto-detected)")

	rootCmd.AddCommand(explainCmd)
}

// SchemaExplanation is the structured output for explain
type SchemaExplanation struct {
	Name       string               `json:"name"`
	Namespace  string               `json:"namespace,omitempty"`
	SchemaType string               `json:"schemaType"`
	RecordType string               `json:"recordType"` // record, enum, fixed, message, object
	Doc        string               `json:"doc,omitempty"`
	Fields     []FieldExplanation   `json:"fields,omitempty"`
	Symbols    []string             `json:"symbols,omitempty"` // for enums
	References []RefExplanation     `json:"references,omitempty"`
	Size       int                  `json:"size"`
	Subject    string               `json:"subject,omitempty"`
	Version    int                  `json:"version,omitempty"`
	SchemaID   int                  `json:"schemaId,omitempty"`
}

// FieldExplanation describes a single field
type FieldExplanation struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Doc         string `json:"doc,omitempty"`
	HasDefault  bool   `json:"hasDefault"`
	Default     string `json:"default,omitempty"`
	IsNullable  bool   `json:"isNullable"`
	IsReference bool   `json:"isReference"`
}

// RefExplanation describes a schema reference
type RefExplanation struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

func runExplain(cmd *cobra.Command, args []string) error {
	var schemaContent string
	var schemaType string
	var explanation SchemaExplanation

	if explainFile != "" {
		// Local file mode
		content, err := os.ReadFile(explainFile)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		schemaContent = string(content)
		schemaType = explainType
		if schemaType == "" {
			schemaType = detectSchemaType(schemaContent, explainFile)
		}
		explanation.Size = len(content)
	} else if len(args) > 0 {
		// Registry mode
		c, err := GetClient()
		if err != nil {
			return err
		}
		schema, err := c.GetSchema(args[0], explainVersion)
		if err != nil {
			return fmt.Errorf("failed to get schema: %w", err)
		}
		schemaContent = schema.Schema
		schemaType = schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}
		explanation.Subject = args[0]
		explanation.Version = schema.Version
		explanation.SchemaID = schema.ID
		explanation.Size = len(schema.Schema)

		for _, ref := range schema.References {
			explanation.References = append(explanation.References, RefExplanation{
				Name:    ref.Name,
				Subject: ref.Subject,
				Version: ref.Version,
			})
		}
	} else {
		return fmt.Errorf("provide a subject name or use --file")
	}

	explanation.SchemaType = schemaType

	switch strings.ToUpper(schemaType) {
	case "AVRO":
		explainAvro(schemaContent, &explanation)
	case "PROTOBUF":
		explainProtobuf(schemaContent, &explanation)
	case "JSON":
		explainJSONSchema(schemaContent, &explanation)
	}

	printer := output.NewPrinter(outputFormat)
	if outputFormat != "table" {
		return printer.Print(explanation)
	}

	displayExplanation(explanation)
	return nil
}

func explainAvro(content string, exp *SchemaExplanation) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return
	}

	exp.Name, _ = schema["name"].(string)
	exp.Namespace, _ = schema["namespace"].(string)
	exp.Doc, _ = schema["doc"].(string)

	typeName, _ := schema["type"].(string)
	exp.RecordType = typeName

	switch typeName {
	case "record":
		if fields, ok := schema["fields"].([]interface{}); ok {
			for _, f := range fields {
				field, ok := f.(map[string]interface{})
				if !ok {
					continue
				}

				fe := FieldExplanation{}
				fe.Name, _ = field["name"].(string)
				fe.Doc, _ = field["doc"].(string)
				fe.Type = describeAvroType(field["type"])

				if defVal, has := field["default"]; has {
					fe.HasDefault = true
					b, _ := json.Marshal(defVal)
					fe.Default = string(b)
				}

				// Check nullable
				if unionTypes, ok := field["type"].([]interface{}); ok {
					for _, ut := range unionTypes {
						if utStr, ok := ut.(string); ok && utStr == "null" {
							fe.IsNullable = true
							break
						}
					}
				}

				// Check if it's a reference (string type that looks like a fully qualified name)
				if typeStr, ok := field["type"].(string); ok && strings.Contains(typeStr, ".") {
					fe.IsReference = true
				}

				exp.Fields = append(exp.Fields, fe)
			}
		}

	case "enum":
		if symbols, ok := schema["symbols"].([]interface{}); ok {
			for _, s := range symbols {
				if str, ok := s.(string); ok {
					exp.Symbols = append(exp.Symbols, str)
				}
			}
		}
	}
}

func describeAvroType(t interface{}) string {
	switch v := t.(type) {
	case string:
		return v
	case map[string]interface{}:
		typeName, _ := v["type"].(string)
		switch typeName {
		case "record":
			name, _ := v["name"].(string)
			return fmt.Sprintf("record(%s)", name)
		case "enum":
			name, _ := v["name"].(string)
			if symbols, ok := v["symbols"].([]interface{}); ok {
				syms := make([]string, 0, len(symbols))
				for _, s := range symbols {
					if str, ok := s.(string); ok {
						syms = append(syms, str)
					}
				}
				return fmt.Sprintf("enum(%s: %s)", name, strings.Join(syms, ", "))
			}
			return fmt.Sprintf("enum(%s)", name)
		case "array":
			items := describeAvroType(v["items"])
			return fmt.Sprintf("array<%s>", items)
		case "map":
			values := describeAvroType(v["values"])
			return fmt.Sprintf("map<%s>", values)
		case "fixed":
			name, _ := v["name"].(string)
			size, _ := v["size"].(float64)
			return fmt.Sprintf("fixed(%s, %d bytes)", name, int(size))
		default:
			return typeName
		}
	case []interface{}:
		types := make([]string, 0, len(v))
		for _, ut := range v {
			types = append(types, describeAvroType(ut))
		}
		// Simplify nullable display
		if len(types) == 2 && types[0] == "null" {
			return fmt.Sprintf("nullable %s", types[1])
		}
		if len(types) == 2 && types[1] == "null" {
			return fmt.Sprintf("nullable %s", types[0])
		}
		return fmt.Sprintf("union[%s]", strings.Join(types, ", "))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func explainProtobuf(content string, exp *SchemaExplanation) {
	exp.RecordType = "message"

	// Extract package
	pkgRe := regexp.MustCompile(`(?m)^package\s+([\w.]+)\s*;`)
	if m := pkgRe.FindStringSubmatch(content); len(m) >= 2 {
		exp.Namespace = m[1]
	}

	// Extract messages
	messages := parseProtobufMessages(content)
	if len(messages) > 0 {
		exp.Name = messages[0].Name
	}

	// Extract fields from first (main) message
	fieldRe := regexp.MustCompile(`(?m)^\s*(repeated\s+|optional\s+|required\s+)?(\w[\w.]*)\s+(\w+)\s*=\s*(\d+)\s*;`)

	for _, msg := range messages {
		fields := fieldRe.FindAllStringSubmatch(msg.Body, -1)
		for _, f := range fields {
			modifier := strings.TrimSpace(f[1])
			fieldType := f[2]
			fieldName := f[3]

			typeDesc := fieldType
			if modifier == "repeated" {
				typeDesc = fmt.Sprintf("repeated %s", fieldType)
			}

			exp.Fields = append(exp.Fields, FieldExplanation{
				Name: fieldName,
				Type: typeDesc,
			})
		}
		break // Only explain the first/main message
	}
}

func explainJSONSchema(content string, exp *SchemaExplanation) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return
	}

	exp.RecordType = "object"
	if id, ok := schema["$id"].(string); ok {
		exp.Name = id
	}
	if title, ok := schema["title"].(string); ok {
		exp.Name = title
	}
	exp.Doc, _ = schema["description"].(string)

	if props, ok := schema["properties"].(map[string]interface{}); ok {
		// Get required fields
		requiredSet := make(map[string]bool)
		if required, ok := schema["required"].([]interface{}); ok {
			for _, r := range required {
				if str, ok := r.(string); ok {
					requiredSet[str] = true
				}
			}
		}

		// Sort property names for consistent output
		names := make([]string, 0, len(props))
		for name := range props {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			propVal := props[name]
			propMap, ok := propVal.(map[string]interface{})
			if !ok {
				continue
			}

			fe := FieldExplanation{Name: name}

			// Build type description
			if ref, ok := propMap["$ref"].(string); ok {
				fe.Type = fmt.Sprintf("$ref(%s)", ref)
				fe.IsReference = true
			} else {
				fe.Type = describeJSONSchemaType(propMap)
			}

			fe.Doc, _ = propMap["description"].(string)

			if !requiredSet[name] {
				fe.IsNullable = true // Not required = optional
			}

			if defVal, has := propMap["default"]; has {
				fe.HasDefault = true
				b, _ := json.Marshal(defVal)
				fe.Default = string(b)
			}

			exp.Fields = append(exp.Fields, fe)
		}
	}
}

func describeJSONSchemaType(schema map[string]interface{}) string {
	typeStr, _ := schema["type"].(string)

	switch typeStr {
	case "object":
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			return fmt.Sprintf("object(%d properties)", len(props))
		}
		return "object"
	case "array":
		if items, ok := schema["items"].(map[string]interface{}); ok {
			if ref, ok := items["$ref"].(string); ok {
				return fmt.Sprintf("array<$ref(%s)>", ref)
			}
			itemType, _ := items["type"].(string)
			return fmt.Sprintf("array<%s>", itemType)
		}
		return "array"
	case "string":
		if format, ok := schema["format"].(string); ok {
			return fmt.Sprintf("string(%s)", format)
		}
		if enum, ok := schema["enum"].([]interface{}); ok {
			vals := make([]string, 0, len(enum))
			for _, e := range enum {
				if str, ok := e.(string); ok {
					vals = append(vals, str)
				}
			}
			return fmt.Sprintf("enum(%s)", strings.Join(vals, ", "))
		}
		return "string"
	default:
		return typeStr
	}
}

func displayExplanation(exp SchemaExplanation) {
	// Header
	nameDisplay := exp.Name
	if exp.Namespace != "" {
		nameDisplay = fmt.Sprintf("%s (%s)", exp.Name, exp.Namespace)
	}

	output.Header("Schema: %s", nameDisplay)

	// Metadata
	output.Info("Type: %s %s", exp.SchemaType, exp.RecordType)
	if exp.Subject != "" {
		output.Info("Subject: %s (v%d, ID %d)", exp.Subject, exp.Version, exp.SchemaID)
	}
	if exp.Doc != "" {
		output.Info("Description: %s", exp.Doc)
	}
	output.Info("Size: %s", output.FormatBytes(int64(exp.Size)))

	// Enum symbols
	if len(exp.Symbols) > 0 {
		fmt.Println()
		output.SubHeader("Symbols (%d)", len(exp.Symbols))
		fmt.Printf("  %s\n", strings.Join(exp.Symbols, ", "))
	}

	// Fields
	if len(exp.Fields) > 0 {
		fmt.Println()
		output.SubHeader("Fields (%d)", len(exp.Fields))

		headers := []string{"Name", "Type", "Description", "Default"}
		var rows [][]string

		for _, f := range exp.Fields {
			typeStr := f.Type
			if f.IsReference {
				typeStr = typeStr + " (ref)"
			}

			desc := f.Doc
			if f.IsNullable && !strings.Contains(f.Type, "nullable") {
				if desc != "" {
					desc += " | optional"
				} else {
					desc = "optional"
				}
			}

			defStr := ""
			if f.HasDefault {
				defStr = f.Default
			}

			rows = append(rows, []string{f.Name, typeStr, desc, defStr})
		}

		output.PrintTable(headers, rows)
	}

	// References
	if len(exp.References) > 0 {
		fmt.Println()
		output.SubHeader("References (%d)", len(exp.References))

		headers := []string{"Name", "Subject", "Version"}
		var rows [][]string
		for _, ref := range exp.References {
			rows = append(rows, []string{ref.Name, ref.Subject, fmt.Sprintf("%d", ref.Version)})
		}
		output.PrintTable(headers, rows)
	}
}
