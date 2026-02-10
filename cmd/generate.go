package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/output"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Infer a schema from sample JSON data",
	GroupID: groupSchema,
	Long: `Generate an Avro, Protobuf, or JSON Schema from sample JSON data.
Reads from stdin or a file, infers field types, and outputs a complete schema.

Supports single JSON objects or JSONL (one object per line) for better
type inference across multiple samples.

Examples:
  # Generate Avro schema from JSON
  echo '{"orderId": "123", "amount": 49.99, "active": true}' | srctl generate

  # Generate with custom name and namespace
  echo '{"id": "1", "name": "test"}' | srctl generate --name Order --namespace com.example

  # Generate JSON Schema
  echo '{"id": "1"}' | srctl generate --type JSON

  # Generate Protobuf
  echo '{"id": "1", "count": 5}' | srctl generate --type PROTOBUF

  # From a file
  srctl generate --from sample.json --name Event

  # From JSONL (multiple samples for better inference)
  cat samples.jsonl | srctl generate --name Event`,
	RunE: runGenerate,
}

var (
	generateFrom      string
	generateType      string
	generateName      string
	generateNamespace string
)

func init() {
	generateCmd.Flags().StringVar(&generateFrom, "from", "", "Sample data file (alternative to stdin)")
	generateCmd.Flags().StringVarP(&generateType, "type", "t", "AVRO", "Output schema type: AVRO, PROTOBUF, JSON")
	generateCmd.Flags().StringVar(&generateName, "name", "Record", "Record/message name")
	generateCmd.Flags().StringVar(&generateNamespace, "namespace", "com.example", "Namespace (Avro/Protobuf)")

	rootCmd.AddCommand(generateCmd)
}

func runGenerate(cmd *cobra.Command, args []string) error {
	var reader io.Reader

	if generateFrom != "" {
		f, err := os.Open(generateFrom)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()
		reader = f
	} else {
		// Check if stdin has data
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			return fmt.Errorf("provide JSON data via stdin or use --from flag\nExample: echo '{\"id\": \"123\"}' | srctl generate")
		}
		reader = os.Stdin
	}

	// Read all samples
	samples, err := readJSONSamples(reader)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(samples) == 0 {
		return fmt.Errorf("no valid JSON objects found in input")
	}

	// Merge field types from all samples
	fields := inferFieldTypes(samples)

	// Generate schema
	var schema string
	switch strings.ToUpper(generateType) {
	case "AVRO":
		schema = generateAvroSchema(fields, generateName, generateNamespace)
	case "PROTOBUF":
		schema = generateProtobufSchema(fields, generateName, generateNamespace)
	case "JSON":
		schema = generateJSONSchemaFromFields(fields)
	default:
		return fmt.Errorf("unsupported type: %s (use AVRO, PROTOBUF, or JSON)", generateType)
	}

	fmt.Println(schema)
	return nil
}

func readJSONSamples(reader io.Reader) ([]map[string]interface{}, error) {
	var samples []map[string]interface{}

	scanner := bufio.NewScanner(reader)
	// Increase buffer for large JSON objects
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	var accumulated strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Try to parse each line as JSON
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err == nil {
			samples = append(samples, obj)
			continue
		}

		// Accumulate lines for multi-line JSON
		accumulated.WriteString(line)
	}

	// Try accumulated content as single JSON object
	if accumulated.Len() > 0 {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(accumulated.String()), &obj); err == nil {
			samples = append(samples, obj)
		}
	}

	// If no samples yet, try reading everything as one blob
	if len(samples) == 0 && accumulated.Len() > 0 {
		// Try as JSON array
		var arr []map[string]interface{}
		if err := json.Unmarshal([]byte(accumulated.String()), &arr); err == nil {
			samples = arr
		}
	}

	return samples, scanner.Err()
}

// inferredField holds the inferred type information for a field
type inferredField struct {
	Name       string
	Type       string // avro type: string, int, long, double, boolean, null
	Format     string // detected format: date-time, date, uuid, email (for doc/hints)
	IsNullable bool
	IsArray    bool
	ItemType   string // for arrays
	IsObject   bool
	Children   map[string]*inferredField // for nested objects
}

func inferFieldTypes(samples []map[string]interface{}) map[string]*inferredField {
	fields := make(map[string]*inferredField)

	for _, sample := range samples {
		for key, value := range sample {
			existing := fields[key]
			inferred := inferType(key, value)

			if existing == nil {
				fields[key] = inferred
			} else {
				// Merge: if types conflict, make nullable or use union
				mergeInferredField(existing, inferred)
			}
		}

		// Fields not present in this sample should be nullable
		for key, field := range fields {
			if _, present := sample[key]; !present {
				field.IsNullable = true
			}
		}
	}

	return fields
}

// Common format patterns for string type inference
var (
	isoDateTimeRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	isoDateRe     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	uuidRe        = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	emailRe       = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
)

// inferStringFormat detects common string formats for documentation
func inferStringFormat(value string) string {
	switch {
	case isoDateTimeRe.MatchString(value):
		return "date-time"
	case isoDateRe.MatchString(value):
		return "date"
	case uuidRe.MatchString(value):
		return "uuid"
	case emailRe.MatchString(value):
		return "email"
	default:
		return ""
	}
}

func inferType(name string, value interface{}) *inferredField {
	field := &inferredField{Name: name}

	switch v := value.(type) {
	case string:
		field.Type = "string"
		field.Format = inferStringFormat(v)
	case float64:
		// Check if it's an integer
		if v == float64(int64(v)) {
			field.Type = "long"
		} else {
			field.Type = "double"
		}
	case bool:
		field.Type = "boolean"
	case nil:
		field.Type = "null"
		field.IsNullable = true
	case map[string]interface{}:
		field.IsObject = true
		field.Type = "record"
		field.Children = make(map[string]*inferredField)
		for k, val := range v {
			field.Children[k] = inferType(k, val)
		}
	case []interface{}:
		field.IsArray = true
		field.Type = "array"
		if len(v) > 0 {
			itemField := inferType("item", v[0])
			field.ItemType = itemField.Type
			if itemField.IsObject {
				field.Children = itemField.Children
			}
		} else {
			field.ItemType = "string" // default for empty arrays
		}
	default:
		field.Type = "string"
	}

	return field
}

func mergeInferredField(existing, new *inferredField) {
	if existing.Type != new.Type {
		// Types differ across samples - make nullable
		existing.IsNullable = true
		// Prefer the more specific type
		if existing.Type == "null" {
			existing.Type = new.Type
			existing.IsObject = new.IsObject
			existing.IsArray = new.IsArray
			existing.ItemType = new.ItemType
			existing.Children = new.Children
		}
	}
	if new.IsNullable {
		existing.IsNullable = true
	}
}

// ========================
// Avro generation
// ========================

func generateAvroSchema(fields map[string]*inferredField, name, namespace string) string {
	schema := map[string]interface{}{
		"type":      "record",
		"name":      name,
		"namespace": namespace,
		"fields":    buildAvroFields(fields),
	}

	pretty, _ := json.MarshalIndent(schema, "", "  ")
	return string(pretty)
}

func buildAvroFields(fields map[string]*inferredField) []interface{} {
	// Sort field names for deterministic output
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	var avroFields []interface{}
	for _, name := range names {
		f := fields[name]
		field := map[string]interface{}{
			"name": name,
		}

		// Add format hint as doc if detected
		if f.Format != "" {
			field["doc"] = fmt.Sprintf("format: %s", f.Format)
		}

		if f.IsObject && f.Children != nil {
			// Nested record
			nested := map[string]interface{}{
				"type":   "record",
				"name":   strings.ToUpper(name[:1]) + name[1:],
				"fields": buildAvroFields(f.Children),
			}
			if f.IsNullable {
				field["type"] = []interface{}{"null", nested}
				field["default"] = nil
			} else {
				field["type"] = nested
			}
		} else if f.IsArray {
			arrayType := map[string]interface{}{
				"type":  "array",
				"items": f.ItemType,
			}
			if f.Children != nil {
				// Array of records
				arrayType["items"] = map[string]interface{}{
					"type":   "record",
					"name":   strings.ToUpper(name[:1]) + name[1:] + "Item",
					"fields": buildAvroFields(f.Children),
				}
			}
			field["type"] = arrayType
		} else if f.IsNullable && f.Type != "null" {
			field["type"] = []interface{}{"null", f.Type}
			field["default"] = nil
		} else if f.Type == "null" {
			field["type"] = []interface{}{"null", "string"}
			field["default"] = nil
		} else {
			field["type"] = f.Type
		}

		avroFields = append(avroFields, field)
	}

	return avroFields
}

// ========================
// Protobuf generation
// ========================

func generateProtobufSchema(fields map[string]*inferredField, name, namespace string) string {
	var sb strings.Builder

	sb.WriteString("syntax = \"proto3\";\n")
	if namespace != "" {
		sb.WriteString(fmt.Sprintf("package %s;\n", namespace))
	}
	sb.WriteString("\n")

	writeProtobufMessage(&sb, name, fields, 0)

	return sb.String()
}

func writeProtobufMessage(sb *strings.Builder, name string, fields map[string]*inferredField, indent int) {
	prefix := strings.Repeat("  ", indent)

	sb.WriteString(fmt.Sprintf("%smessage %s {\n", prefix, name))

	// Sort field names for deterministic output
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)

	fieldNum := 1
	for _, fname := range names {
		f := fields[fname]
		protoType := avroToProtoType(f)

		if f.IsObject && f.Children != nil {
			// Nested message
			msgName := strings.ToUpper(fname[:1]) + fname[1:]
			writeProtobufMessage(sb, msgName, f.Children, indent+1)
			sb.WriteString(fmt.Sprintf("%s  %s %s = %d;\n", prefix, msgName, toSnakeCase(fname), fieldNum))
		} else if f.IsArray {
			if f.Children != nil {
				msgName := strings.ToUpper(fname[:1]) + fname[1:] + "Item"
				writeProtobufMessage(sb, msgName, f.Children, indent+1)
				sb.WriteString(fmt.Sprintf("%s  repeated %s %s = %d;\n", prefix, msgName, toSnakeCase(fname), fieldNum))
			} else {
				sb.WriteString(fmt.Sprintf("%s  repeated %s %s = %d;\n", prefix, protoType, toSnakeCase(fname), fieldNum))
			}
		} else {
			sb.WriteString(fmt.Sprintf("%s  %s %s = %d;\n", prefix, protoType, toSnakeCase(fname), fieldNum))
		}
		fieldNum++
	}

	sb.WriteString(fmt.Sprintf("%s}\n", prefix))
}

func avroToProtoType(f *inferredField) string {
	switch f.Type {
	case "string":
		return "string"
	case "int":
		return "int32"
	case "long":
		return "int64"
	case "float":
		return "float"
	case "double":
		return "double"
	case "boolean":
		return "bool"
	case "bytes":
		return "bytes"
	default:
		return "string"
	}
}

// ========================
// JSON Schema generation
// ========================

func generateJSONSchemaFromFields(fields map[string]*inferredField) string {
	schema := map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type":    "object",
	}

	props, required := buildJSONSchemaProperties(fields)
	schema["properties"] = props
	if len(required) > 0 {
		schema["required"] = required
	}

	pretty, _ := json.MarshalIndent(schema, "", "  ")
	return string(pretty)
}

func buildJSONSchemaProperties(fields map[string]*inferredField) (map[string]interface{}, []string) {
	props := make(map[string]interface{})
	var required []string

	// Sort field names for deterministic output
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		f := fields[name]
		prop := make(map[string]interface{})

		if f.IsObject && f.Children != nil {
			prop["type"] = "object"
			childProps, childRequired := buildJSONSchemaProperties(f.Children)
			prop["properties"] = childProps
			if len(childRequired) > 0 {
				prop["required"] = childRequired
			}
		} else if f.IsArray {
			prop["type"] = "array"
			if f.Children != nil {
				itemProps, _ := buildJSONSchemaProperties(f.Children)
				prop["items"] = map[string]interface{}{
					"type":       "object",
					"properties": itemProps,
				}
			} else {
				prop["items"] = map[string]interface{}{
					"type": avroToJSONSchemaType(f.ItemType),
				}
			}
		} else {
			prop["type"] = avroToJSONSchemaType(f.Type)
			if f.Format != "" {
				prop["format"] = f.Format
			}
		}

		props[name] = prop

		if !f.IsNullable {
			required = append(required, name)
		}
	}

	return props, required
}

func avroToJSONSchemaType(avroType string) string {
	switch avroType {
	case "string":
		return "string"
	case "int", "long":
		return "integer"
	case "float", "double":
		return "number"
	case "boolean":
		return "boolean"
	case "null":
		return "string" // default
	default:
		return "string"
	}
}

// InferAvroType exports the type inference for testing
func InferAvroType(name string, value interface{}) *inferredField {
	return inferType(name, value)
}

// ReadJSONSamples exports sample reading for testing
func ReadJSONSamples(reader io.Reader) ([]map[string]interface{}, error) {
	return readJSONSamples(reader)
}

// InferFieldTypes exports field inference for testing
func InferFieldTypes(samples []map[string]interface{}) map[string]*inferredField {
	return inferFieldTypes(samples)
}

// GenerateAvroSchema exports Avro generation for testing
func GenerateAvroSchema(fields map[string]*inferredField, name, namespace string) string {
	return generateAvroSchema(fields, name, namespace)
}

// GenerateProtobufSchema exports Protobuf generation for testing
func GenerateProtobufSchema(fields map[string]*inferredField, name, namespace string) string {
	return generateProtobufSchema(fields, name, namespace)
}

// GenerateJSONSchemaFromFields exports JSON Schema generation for testing
func GenerateJSONSchemaFromFields(fields map[string]*inferredField) string {
	return generateJSONSchemaFromFields(fields)
}

// needed for output formatting
func init() {
	_ = output.NewPrinter("table")
}
