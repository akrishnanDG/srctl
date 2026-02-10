package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

// Command group for schema splitting
const groupSplit = "split"

// splitCmd is the parent command for schema splitting operations
var splitCmd = &cobra.Command{
	Use:     "split",
	Aliases: []string{"decompose"},
	Short:   "Split large schemas into referenced sub-schemas",
	GroupID: groupSchema,
	Long: `Split large monolithic schemas into smaller referenced sub-schemas.

This is useful when schemas exceed the Confluent Cloud 1MB limit,
or when you want to share types across multiple topics.

Supports Avro, Protobuf, and JSON Schema formats.

Subcommands:
  analyze   - Analyze a schema and show extractable types
  extract   - Split schema and write sub-schemas to files
  register  - Split schema and register all parts to Schema Registry

See 'srctl split [command] --help' for command-specific options.
For a comprehensive guide, see docs/schema-splitting-guide.md`,
}

// Flags shared across split subcommands
var (
	splitFile          string
	splitSchemaType    string
	splitMinSize       int
	splitSubjectPrefix string
)

// splitAnalyzeCmd analyzes a schema and shows extractable types
var splitAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze a schema and show extractable types",
	Long: `Analyze a monolithic schema to identify named types that can be
extracted into separate referenced schemas.

Shows the dependency tree, estimated sizes, and registration order.

Examples:
  # Analyze an Avro schema
  srctl split analyze --file order.avsc

  # Analyze with minimum size threshold
  srctl split analyze --file order.avsc --min-size 10240

  # Analyze a Protobuf schema
  srctl split analyze --file order.proto --type PROTOBUF

  # Analyze a JSON schema
  srctl split analyze --file order.json --type JSON`,
	RunE: runSplitAnalyze,
}

// splitExtractCmd extracts sub-schemas to files
var splitExtractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Split schema and write sub-schemas to files",
	Long: `Split a monolithic schema into referenced sub-schemas and write
each part to a separate file in the output directory.

The output includes a manifest.json describing the registration order
and references.

Examples:
  # Extract to output directory
  srctl split extract --file order.avsc --output-dir ./split-schemas/

  # Extract with custom subject prefix
  srctl split extract --file order.avsc --output-dir ./split-schemas/ \
    --subject-prefix "com.example.types."

  # Extract a Protobuf schema
  srctl split extract --file order.proto --type PROTOBUF --output-dir ./split-schemas/`,
	RunE: runSplitExtract,
}

// splitRegisterCmd splits and registers to Schema Registry
var splitRegisterCmd = &cobra.Command{
	Use:   "register",
	Short: "Split schema and register all parts to Schema Registry",
	Long: `Split a monolithic schema and register all parts to Schema Registry
in the correct dependency order (leaf schemas first, root schema last).

Examples:
  # Split and register
  srctl split register --file order.avsc --subject orders-value

  # Dry run
  srctl split register --file order.avsc --subject orders-value --dry-run

  # With explicit type
  srctl split register --file order.proto --type PROTOBUF --subject orders-value`,
	RunE: runSplitRegister,
}

var (
	splitOutputDir     string
	splitSubject       string
	splitDryRun        bool
	splitCompatibility string
)

func init() {
	// Shared flags
	splitAnalyzeCmd.Flags().StringVarP(&splitFile, "file", "f", "", "Path to schema file")
	splitAnalyzeCmd.Flags().StringVarP(&splitSchemaType, "type", "t", "", "Schema type: AVRO, PROTOBUF, JSON (auto-detected from extension)")
	splitAnalyzeCmd.Flags().IntVar(&splitMinSize, "min-size", 0, "Minimum type size in bytes to extract (0 = extract all)")
	_ = splitAnalyzeCmd.MarkFlagRequired("file")

	splitExtractCmd.Flags().StringVarP(&splitFile, "file", "f", "", "Path to schema file")
	splitExtractCmd.Flags().StringVarP(&splitSchemaType, "type", "t", "", "Schema type: AVRO, PROTOBUF, JSON")
	splitExtractCmd.Flags().IntVar(&splitMinSize, "min-size", 0, "Minimum type size in bytes to extract (0 = extract all)")
	splitExtractCmd.Flags().StringVar(&splitOutputDir, "output-dir", "", "Directory to write split schemas")
	splitExtractCmd.Flags().StringVar(&splitSubjectPrefix, "subject-prefix", "", "Prefix for extracted type subject names")
	_ = splitExtractCmd.MarkFlagRequired("file")
	_ = splitExtractCmd.MarkFlagRequired("output-dir")

	splitRegisterCmd.Flags().StringVarP(&splitFile, "file", "f", "", "Path to schema file")
	splitRegisterCmd.Flags().StringVarP(&splitSchemaType, "type", "t", "", "Schema type: AVRO, PROTOBUF, JSON")
	splitRegisterCmd.Flags().IntVar(&splitMinSize, "min-size", 0, "Minimum type size in bytes to extract (0 = extract all)")
	splitRegisterCmd.Flags().StringVar(&splitSubject, "subject", "", "Subject name for the root schema")
	splitRegisterCmd.Flags().StringVar(&splitSubjectPrefix, "subject-prefix", "", "Prefix for extracted type subject names")
	splitRegisterCmd.Flags().BoolVar(&splitDryRun, "dry-run", false, "Show what would be registered without registering")
	splitRegisterCmd.Flags().StringVar(&splitCompatibility, "compatibility", "BACKWARD", "Compatibility level for extracted subjects")
	_ = splitRegisterCmd.MarkFlagRequired("file")
	_ = splitRegisterCmd.MarkFlagRequired("subject")

	// Add subcommands
	splitCmd.AddCommand(splitAnalyzeCmd)
	splitCmd.AddCommand(splitExtractCmd)
	splitCmd.AddCommand(splitRegisterCmd)

	// Add to root
	rootCmd.AddCommand(splitCmd)
}

// ========================
// Extracted type structures
// ========================

// ExtractedType represents a named type extracted from a schema
type ExtractedType struct {
	Name       string   `json:"name"`       // Fully qualified name (e.g., com.example.types.Address)
	Subject    string   `json:"subject"`    // Subject to register under
	Schema     string   `json:"schema"`     // The extracted schema content
	SchemaType string   `json:"schemaType"` // AVRO, PROTOBUF, JSON
	Size       int      `json:"size"`       // Size in bytes
	References []string `json:"references"` // Names of types this depends on
	IsRoot     bool     `json:"isRoot"`     // Whether this is the root schema
	Order      int      `json:"order"`      // Registration order (0-based)
}

// SplitResult contains the full result of a schema split operation
type SplitResult struct {
	OriginalSize      int             `json:"originalSize"`
	OriginalFile      string          `json:"originalFile"`
	SchemaType        string          `json:"schemaType"`
	Types             []ExtractedType `json:"types"`
	RegistrationOrder []string        `json:"registrationOrder"`
}

// ========================
// Command implementations
// ========================

func runSplitAnalyze(cmd *cobra.Command, args []string) error {
	content, err := os.ReadFile(splitFile)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	schemaType := splitSchemaType
	if schemaType == "" {
		schemaType = detectSchemaType(string(content), splitFile)
	}

	result, err := splitSchema(string(content), schemaType, splitFile, splitMinSize, splitSubjectPrefix)
	if err != nil {
		return fmt.Errorf("failed to analyze schema: %w", err)
	}

	output.Header("Schema Analysis: %s", splitFile)
	output.Info("Schema type: %s", schemaType)
	output.Info("Original size: %s", output.FormatBytes(int64(result.OriginalSize)))
	output.Info("Named types found: %d", len(result.Types)-1) // minus root
	fmt.Println()

	// Show extracted types
	output.SubHeader("Extractable Types")

	headers := []string{"#", "Type Name", "Subject", "Size", "Dependencies", "Role"}
	var rows [][]string

	for i, t := range result.Types {
		role := "Reference"
		if t.IsRoot {
			role = "Root"
		}
		deps := "-"
		if len(t.References) > 0 {
			// Show short names
			shortDeps := make([]string, 0, len(t.References))
			for _, r := range t.References {
				parts := strings.Split(r, ".")
				shortDeps = append(shortDeps, parts[len(parts)-1])
			}
			deps = strings.Join(shortDeps, ", ")
		}
		rows = append(rows, []string{
			fmt.Sprintf("%d", i+1),
			t.Name,
			t.Subject,
			output.FormatBytes(int64(t.Size)),
			deps,
			role,
		})
	}

	output.PrintTable(headers, rows)
	fmt.Println()

	// Show registration order
	output.SubHeader("Registration Order")
	for i, name := range result.RegistrationOrder {
		output.Step("%d. %s", i+1, name)
	}
	fmt.Println()

	// Size summary
	var totalSplit int
	var maxPart int
	for _, t := range result.Types {
		totalSplit += t.Size
		if t.Size > maxPart {
			maxPart = t.Size
		}
	}

	output.SubHeader("Size Summary")
	output.Info("Original schema: %s", output.FormatBytes(int64(result.OriginalSize)))
	output.Info("Largest part after split: %s", output.FormatBytes(int64(maxPart)))

	if maxPart < 1024*1024 {
		output.Success("All parts are under 1MB - safe for Confluent Cloud")
	} else {
		output.Warning("Largest part exceeds 1MB - may need further splitting")
	}

	return nil
}

func runSplitExtract(cmd *cobra.Command, args []string) error {
	content, err := os.ReadFile(splitFile)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	schemaType := splitSchemaType
	if schemaType == "" {
		schemaType = detectSchemaType(string(content), splitFile)
	}

	result, err := splitSchema(string(content), schemaType, splitFile, splitMinSize, splitSubjectPrefix)
	if err != nil {
		return fmt.Errorf("failed to split schema: %w", err)
	}

	// Create output directory
	if err := os.MkdirAll(splitOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	output.Header("Extracting Schema: %s", splitFile)
	output.Info("Schema type: %s", schemaType)
	output.Info("Output directory: %s", splitOutputDir)
	fmt.Println()

	// Write each type to a file
	for _, t := range result.Types {
		ext := getExtensionForType(schemaType)
		filename := sanitizeFilename(t.Subject) + ext
		filePath := filepath.Join(splitOutputDir, filename)

		if err := os.WriteFile(filePath, []byte(t.Schema), 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", filePath, err)
		}

		role := "reference"
		if t.IsRoot {
			role = "root"
		}
		output.Success("Written %s (%s, %s)", filename, role, output.FormatBytes(int64(t.Size)))
	}

	// Write manifest
	manifest, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to create manifest: %w", err)
	}

	manifestPath := filepath.Join(splitOutputDir, "manifest.json")
	if err := os.WriteFile(manifestPath, manifest, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	fmt.Println()
	output.Success("Written manifest.json (registration order and references)")
	output.Info("Total files written: %d", len(result.Types)+1)
	output.Info("Review the files and adjust subject names in manifest.json before registering")

	return nil
}

func runSplitRegister(cmd *cobra.Command, args []string) error {
	content, err := os.ReadFile(splitFile)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	schemaType := splitSchemaType
	if schemaType == "" {
		schemaType = detectSchemaType(string(content), splitFile)
	}

	result, err := splitSchema(string(content), schemaType, splitFile, splitMinSize, splitSubjectPrefix)
	if err != nil {
		return fmt.Errorf("failed to split schema: %w", err)
	}

	// Set root subject
	for i := range result.Types {
		if result.Types[i].IsRoot {
			result.Types[i].Subject = splitSubject
		}
	}

	output.Header("Split & Register: %s", splitFile)
	output.Info("Schema type: %s", schemaType)
	output.Info("Root subject: %s", splitSubject)
	output.Info("Parts to register: %d", len(result.Types))
	fmt.Println()

	if splitDryRun {
		output.SubHeader("Dry Run - Registration Plan")
		for i, name := range result.RegistrationOrder {
			for _, t := range result.Types {
				if t.Name == name {
					role := "reference"
					if t.IsRoot {
						role = "root"
					}
					subject := t.Subject
					if t.IsRoot {
						subject = splitSubject
					}
					deps := "none"
					if len(t.References) > 0 {
						deps = strings.Join(t.References, ", ")
					}
					output.Step("%d. Register %s as subject '%s' (%s, %s, deps: %s)",
						i+1, t.Name, subject, role, output.FormatBytes(int64(t.Size)), deps)
					break
				}
			}
		}
		fmt.Println()
		output.Success("Dry run complete - no schemas were registered")
		return nil
	}

	c, err := GetClient()
	if err != nil {
		return err
	}

	// Build a map for quick lookup
	typeMap := make(map[string]*ExtractedType)
	for i := range result.Types {
		typeMap[result.Types[i].Name] = &result.Types[i]
	}

	// Track registered versions for building references
	registeredVersions := make(map[string]int) // subject -> version

	// Register in dependency order
	for i, name := range result.RegistrationOrder {
		t := typeMap[name]
		subject := t.Subject
		if t.IsRoot {
			subject = splitSubject
		}

		// Build references
		var refs []client.SchemaReference
		for _, depName := range t.References {
			dep := typeMap[depName]
			refName := getReferenceName(dep, schemaType)
			version := registeredVersions[dep.Subject]
			if version == 0 {
				version = 1 // default to 1 if not yet registered
			}
			refs = append(refs, client.SchemaReference{
				Name:    refName,
				Subject: dep.Subject,
				Version: version,
			})
		}

		schema := &client.Schema{
			Schema:     t.Schema,
			SchemaType: schemaType,
			References: refs,
		}

		output.Step("[%d/%d] Registering %s as '%s'...", i+1, len(result.RegistrationOrder), t.Name, subject)

		id, err := c.RegisterSchema(subject, schema)
		if err != nil {
			return fmt.Errorf("failed to register %s: %w", subject, err)
		}

		// Get the version that was registered
		versions, err := c.GetVersions(subject, false)
		if err == nil && len(versions) > 0 {
			registeredVersions[subject] = versions[len(versions)-1]
		} else {
			registeredVersions[subject] = 1
		}

		output.Success("  Registered with schema ID %d", id)
	}

	fmt.Println()
	output.Success("All %d schemas registered successfully", len(result.Types))

	// Show summary table
	fmt.Println()
	headers := []string{"Subject", "Role", "Size", "References"}
	var rows [][]string
	for _, name := range result.RegistrationOrder {
		t := typeMap[name]
		subject := t.Subject
		if t.IsRoot {
			subject = splitSubject
		}
		role := "Reference"
		if t.IsRoot {
			role = "Root"
		}
		refStr := "-"
		if len(t.References) > 0 {
			refStr = fmt.Sprintf("%d", len(t.References))
		}
		rows = append(rows, []string{subject, role, output.FormatBytes(int64(t.Size)), refStr})
	}
	output.PrintTable(headers, rows)

	return nil
}

// ========================
// Schema splitting logic
// ========================

func splitSchema(content, schemaType, filename string, minSize int, subjectPrefix string) (*SplitResult, error) {
	switch strings.ToUpper(schemaType) {
	case "AVRO":
		return splitAvroSchema(content, minSize, subjectPrefix)
	case "PROTOBUF":
		return splitProtobufSchema(content, filename, minSize, subjectPrefix)
	case "JSON":
		return splitJSONSchema(content, minSize, subjectPrefix)
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}
}

// ========================
// Avro splitting
// ========================

func splitAvroSchema(content string, minSize int, subjectPrefix string) (*SplitResult, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected Avro schema to be a JSON object")
	}

	// Work on a deep copy so the original schema is preserved for min-size filtering.
	// extractAvroNamedTypes modifies the schema in-place (replaces inline records with
	// reference strings), so without a copy the original structure is lost.
	var schemaCopy interface{}
	copyBytes, _ := json.Marshal(schemaMap)
	json.Unmarshal(copyBytes, &schemaCopy)
	schemaCopyMap := schemaCopy.(map[string]interface{})

	// Extract all named types (records, enums, fixed)
	extractedTypes := make(map[string]map[string]interface{})
	typeDeps := make(map[string][]string)

	// Walk the schema tree and extract named types
	rootName := getAvroFullName(schemaCopyMap)
	extractAvroNamedTypes(schemaCopyMap, "", extractedTypes, typeDeps)

	if len(extractedTypes) <= 1 {
		// Only the root type - nothing to split
		return &SplitResult{
			OriginalSize:      len(content),
			OriginalFile:      "",
			SchemaType:        "AVRO",
			Types:             []ExtractedType{{Name: rootName, Subject: rootName, Schema: content, SchemaType: "AVRO", Size: len(content), IsRoot: true, Order: 0}},
			RegistrationOrder: []string{rootName},
		}, nil
	}

	// Filter by min size
	if minSize > 0 {
		for name, typeSchema := range extractedTypes {
			if name == rootName {
				continue
			}
			serialized, _ := json.Marshal(typeSchema)
			if len(serialized) < minSize {
				delete(extractedTypes, name)
				delete(typeDeps, name)
			}
		}
		// Remove deps pointing to filtered-out types
		for name, depList := range typeDeps {
			var filtered []string
			for _, dep := range depList {
				if _, exists := extractedTypes[dep]; exists {
					filtered = append(filtered, dep)
				}
			}
			typeDeps[name] = filtered
		}
	}

	// If filtering removed all non-root types, return original
	if len(extractedTypes) <= 1 {
		return &SplitResult{
			OriginalSize:      len(content),
			SchemaType:        "AVRO",
			Types:             []ExtractedType{{Name: rootName, Subject: rootName, Schema: content, SchemaType: "AVRO", Size: len(content), IsRoot: true, Order: 0}},
			RegistrationOrder: []string{rootName},
		}, nil
	}

	// When min-size filtering removed some types, rebuild from the original
	// schema and only replace types that survived the filter. This ensures the
	// root schema inlines small types and only references extracted ones.
	if minSize > 0 {
		survivedTypes := make(map[string]bool)
		for name := range extractedTypes {
			if name != rootName {
				survivedTypes[name] = true
			}
		}

		// Rebuild from original: walk the original schema tree, only extract
		// types in survivedTypes, leave everything else inline.
		var freshCopy interface{}
		json.Unmarshal(copyBytes, &freshCopy)
		freshMap := freshCopy.(map[string]interface{})

		extractedTypes = make(map[string]map[string]interface{})
		typeDeps = make(map[string][]string)
		extractAvroNamedTypesSelective(freshMap, "", extractedTypes, typeDeps, survivedTypes)

		if len(extractedTypes) <= 1 {
			return &SplitResult{
				OriginalSize:      len(content),
				SchemaType:        "AVRO",
				Types:             []ExtractedType{{Name: rootName, Subject: rootName, Schema: content, SchemaType: "AVRO", Size: len(content), IsRoot: true, Order: 0}},
				RegistrationOrder: []string{rootName},
			}, nil
		}
		schemaCopyMap = freshMap
	}

	// Build the root schema with references instead of inline types
	rootSchema := buildAvroRootSchema(schemaCopyMap, extractedTypes, rootName)

	// Post-extraction pass: scan all extracted types for string-based references
	// to other extracted types. This catches cases where a type uses another
	// extracted type by its fully qualified name (e.g., "com.example.types.Money")
	// rather than an inline definition.
	for typeName, typeSchema := range extractedTypes {
		if fields, ok := typeSchema["fields"].([]interface{}); ok {
			for _, f := range fields {
				field, ok := f.(map[string]interface{})
				if !ok {
					continue
				}
				refs := findAvroStringTypeRefs(field["type"], extractedTypes)
				for _, ref := range refs {
					if ref != typeName {
						typeDeps[typeName] = appendUnique(typeDeps[typeName], ref)
					}
				}
			}
		}
	}

	// Topological sort for registration order
	regOrder := topologicalSort(typeDeps, rootName)

	// Build result
	result := &SplitResult{
		OriginalSize:      len(content),
		SchemaType:        "AVRO",
		RegistrationOrder: regOrder,
	}

	for i, name := range regOrder {
		isRoot := name == rootName
		var schemaContent string

		if isRoot {
			serialized, _ := json.MarshalIndent(rootSchema, "", "  ")
			schemaContent = string(serialized)
		} else {
			serialized, _ := json.MarshalIndent(extractedTypes[name], "", "  ")
			schemaContent = string(serialized)
		}

		subject := name
		if subjectPrefix != "" && !isRoot {
			subject = subjectPrefix + shortName(name)
		}

		// Get filtered deps (only types that are actually extracted)
		var filteredDeps []string
		for _, dep := range typeDeps[name] {
			if _, exists := extractedTypes[dep]; exists && dep != name {
				filteredDeps = append(filteredDeps, dep)
			}
		}

		result.Types = append(result.Types, ExtractedType{
			Name:       name,
			Subject:    subject,
			Schema:     schemaContent,
			SchemaType: "AVRO",
			Size:       len(schemaContent),
			References: filteredDeps,
			IsRoot:     isRoot,
			Order:      i,
		})
	}

	return result, nil
}

func getAvroFullName(schema map[string]interface{}) string {
	name, _ := schema["name"].(string)
	namespace, _ := schema["namespace"].(string)
	if namespace != "" && !strings.Contains(name, ".") {
		return namespace + "." + name
	}
	return name
}

func extractAvroNamedTypes(schema map[string]interface{}, parentNamespace string, extracted map[string]map[string]interface{}, deps map[string][]string) {
	schemaType, _ := schema["type"].(string)
	namespace, hasNS := schema["namespace"].(string)
	if !hasNS {
		namespace = parentNamespace
	}

	fullName := getAvroFullName(schema)
	if fullName == "" {
		// Anonymous type, derive name from context
		if name, ok := schema["name"].(string); ok {
			if namespace != "" {
				fullName = namespace + "." + name
			} else {
				fullName = name
			}
		}
	}

	// Only extract named types: record, enum, fixed
	if schemaType == "record" || schemaType == "enum" || schemaType == "fixed" {
		// Store the extracted type (with explicit namespace)
		typeCopy := make(map[string]interface{})
		for k, v := range schema {
			typeCopy[k] = v
		}
		if namespace != "" {
			typeCopy["namespace"] = namespace
		}

		// For records, remove inline type definitions from fields and replace with refs
		if schemaType == "record" {
			if fields, ok := schema["fields"].([]interface{}); ok {
				newFields := make([]interface{}, 0, len(fields))
				for _, f := range fields {
					field, ok := f.(map[string]interface{})
					if !ok {
						newFields = append(newFields, f)
						continue
					}

					newField := make(map[string]interface{})
					for k, v := range field {
						newField[k] = v
					}

					fieldType := field["type"]
					depNames := extractAvroFieldDeps(fieldType, namespace, extracted, deps)
					for _, d := range depNames {
						deps[fullName] = appendUnique(deps[fullName], d)
					}

					// Replace inline types with references
					newField["type"] = replaceAvroInlineTypes(fieldType, namespace, extracted)
					newFields = append(newFields, newField)
				}
				typeCopy["fields"] = newFields
			}
		}

		extracted[fullName] = typeCopy
	}
}

// extractAvroNamedTypesSelective is like extractAvroNamedTypes but only extracts
// types whose fully qualified name is in the keepSet. All other named types are
// left inline in their parent schema.
func extractAvroNamedTypesSelective(schema map[string]interface{}, parentNamespace string, extracted map[string]map[string]interface{}, deps map[string][]string, keepSet map[string]bool) {
	schemaType, _ := schema["type"].(string)
	namespace, hasNS := schema["namespace"].(string)
	if !hasNS {
		namespace = parentNamespace
	}

	fullName := getAvroFullName(schema)

	if schemaType == "record" || schemaType == "enum" || schemaType == "fixed" {
		shouldExtract := keepSet[fullName]

		if shouldExtract {
			// Extract this type: store it and replace inline defs in its fields
			typeCopy := make(map[string]interface{})
			for k, v := range schema {
				typeCopy[k] = v
			}
			if namespace != "" {
				typeCopy["namespace"] = namespace
			}

			if schemaType == "record" {
				if fields, ok := schema["fields"].([]interface{}); ok {
					newFields := make([]interface{}, 0, len(fields))
					for _, f := range fields {
						field, ok := f.(map[string]interface{})
						if !ok {
							newFields = append(newFields, f)
							continue
						}
						newField := make(map[string]interface{})
						for k, v := range field {
							newField[k] = v
						}
						depNames := extractAvroFieldDepsSelective(field["type"], namespace, extracted, deps, keepSet)
						for _, d := range depNames {
							deps[fullName] = appendUnique(deps[fullName], d)
						}
						newField["type"] = replaceAvroInlineTypesSelective(field["type"], namespace, keepSet)
						newFields = append(newFields, newField)
					}
					typeCopy["fields"] = newFields
				}
			}
			extracted[fullName] = typeCopy
		} else if schemaType == "record" {
			// Don't extract, but recurse into fields to find extractable children
			if fields, ok := schema["fields"].([]interface{}); ok {
				for _, f := range fields {
					field, ok := f.(map[string]interface{})
					if !ok {
						continue
					}
					walkAvroFieldForSelective(field["type"], namespace, extracted, deps, keepSet, fullName)
				}
			}
		}
	}
}

func walkAvroFieldForSelective(fieldType interface{}, namespace string, extracted map[string]map[string]interface{}, deps map[string][]string, keepSet map[string]bool, parentName string) {
	switch ft := fieldType.(type) {
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			childName := getAvroFullName(ft)
			if keepSet[childName] {
				extractAvroNamedTypesSelective(ft, namespace, extracted, deps, keepSet)
				deps[parentName] = appendUnique(deps[parentName], childName)
			} else if typeName == "record" {
				// Recurse into non-extracted records to find deeper extractable types
				if fields, ok := ft["fields"].([]interface{}); ok {
					for _, f := range fields {
						field, ok := f.(map[string]interface{})
						if !ok {
							continue
						}
						walkAvroFieldForSelective(field["type"], namespace, extracted, deps, keepSet, parentName)
					}
				}
			}
		case "array":
			if items, ok := ft["items"]; ok {
				walkAvroFieldForSelective(items, namespace, extracted, deps, keepSet, parentName)
			}
		case "map":
			if values, ok := ft["values"]; ok {
				walkAvroFieldForSelective(values, namespace, extracted, deps, keepSet, parentName)
			}
		}
	case []interface{}:
		for _, ut := range ft {
			walkAvroFieldForSelective(ut, namespace, extracted, deps, keepSet, parentName)
		}
	}
}

func extractAvroFieldDepsSelective(fieldType interface{}, namespace string, extracted map[string]map[string]interface{}, deps map[string][]string, keepSet map[string]bool) []string {
	var depNames []string
	switch ft := fieldType.(type) {
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			childName := getAvroFullName(ft)
			if keepSet[childName] {
				extractAvroNamedTypesSelective(ft, namespace, extracted, deps, keepSet)
				depNames = append(depNames, childName)
			}
		case "array":
			if items, ok := ft["items"]; ok {
				depNames = append(depNames, extractAvroFieldDepsSelective(items, namespace, extracted, deps, keepSet)...)
			}
		case "map":
			if values, ok := ft["values"]; ok {
				depNames = append(depNames, extractAvroFieldDepsSelective(values, namespace, extracted, deps, keepSet)...)
			}
		}
	case []interface{}:
		for _, ut := range ft {
			depNames = append(depNames, extractAvroFieldDepsSelective(ut, namespace, extracted, deps, keepSet)...)
		}
	}
	return depNames
}

func replaceAvroInlineTypesSelective(fieldType interface{}, namespace string, keepSet map[string]bool) interface{} {
	switch ft := fieldType.(type) {
	case string:
		return ft
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			fullName := getAvroFullName(ft)
			if keepSet[fullName] {
				return fullName
			}
			return ft // Leave inline
		case "array":
			result := map[string]interface{}{"type": "array"}
			if items, ok := ft["items"]; ok {
				result["items"] = replaceAvroInlineTypesSelective(items, namespace, keepSet)
			}
			return result
		case "map":
			result := map[string]interface{}{"type": "map"}
			if values, ok := ft["values"]; ok {
				result["values"] = replaceAvroInlineTypesSelective(values, namespace, keepSet)
			}
			return result
		default:
			return ft
		}
	case []interface{}:
		result := make([]interface{}, 0, len(ft))
		for _, ut := range ft {
			result = append(result, replaceAvroInlineTypesSelective(ut, namespace, keepSet))
		}
		return result
	default:
		return ft
	}
}

func extractAvroFieldDeps(fieldType interface{}, namespace string, extracted map[string]map[string]interface{}, deps map[string][]string) []string {
	var depNames []string

	switch ft := fieldType.(type) {
	case string:
		// Primitive or named type reference - not a dep to extract
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			// This is an inline named type - extract it
			extractAvroNamedTypes(ft, namespace, extracted, deps)
			depNames = append(depNames, getAvroFullName(ft))
		case "array":
			if items, ok := ft["items"]; ok {
				depNames = append(depNames, extractAvroFieldDeps(items, namespace, extracted, deps)...)
			}
		case "map":
			if values, ok := ft["values"]; ok {
				depNames = append(depNames, extractAvroFieldDeps(values, namespace, extracted, deps)...)
			}
		}
	case []interface{}:
		// Union type
		for _, ut := range ft {
			depNames = append(depNames, extractAvroFieldDeps(ut, namespace, extracted, deps)...)
		}
	}

	return depNames
}

// findAvroStringTypeRefs finds string-based references to extracted types
// in a field's type definition. This catches cases like a field typed as
// "com.example.types.Money" where Money was already extracted elsewhere.
func findAvroStringTypeRefs(fieldType interface{}, extracted map[string]map[string]interface{}) []string {
	var refs []string

	switch ft := fieldType.(type) {
	case string:
		if _, exists := extracted[ft]; exists {
			refs = append(refs, ft)
		}
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "array":
			if items, ok := ft["items"]; ok {
				refs = append(refs, findAvroStringTypeRefs(items, extracted)...)
			}
		case "map":
			if values, ok := ft["values"]; ok {
				refs = append(refs, findAvroStringTypeRefs(values, extracted)...)
			}
		}
	case []interface{}:
		for _, ut := range ft {
			refs = append(refs, findAvroStringTypeRefs(ut, extracted)...)
		}
	}

	return refs
}

func replaceAvroInlineTypes(fieldType interface{}, namespace string, extracted map[string]map[string]interface{}) interface{} {
	switch ft := fieldType.(type) {
	case string:
		return ft // Primitive or already a reference
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			// Replace inline with reference to fully qualified name
			fullName := getAvroFullName(ft)
			if _, exists := extracted[fullName]; exists {
				return fullName
			}
			return ft
		case "array":
			result := map[string]interface{}{"type": "array"}
			if items, ok := ft["items"]; ok {
				result["items"] = replaceAvroInlineTypes(items, namespace, extracted)
			}
			return result
		case "map":
			result := map[string]interface{}{"type": "map"}
			if values, ok := ft["values"]; ok {
				result["values"] = replaceAvroInlineTypes(values, namespace, extracted)
			}
			return result
		default:
			return ft
		}
	case []interface{}:
		// Union type
		result := make([]interface{}, 0, len(ft))
		for _, ut := range ft {
			result = append(result, replaceAvroInlineTypes(ut, namespace, extracted))
		}
		return result
	default:
		return ft
	}
}

// rebuildAvroSchemaSelective walks a schema and only replaces inline record/enum/fixed
// types that exist in the keepTypes map. All other inline types are left as-is.
func rebuildAvroSchemaSelective(schema map[string]interface{}, keepTypes map[string]map[string]interface{}) {
	if fields, ok := schema["fields"].([]interface{}); ok {
		for i, f := range fields {
			field, ok := f.(map[string]interface{})
			if !ok {
				continue
			}
			field["type"] = replaceAvroSelectiveTypes(field["type"], schema["namespace"], keepTypes)
			fields[i] = field
		}
	}
}

func replaceAvroSelectiveTypes(fieldType interface{}, parentNS interface{}, keepTypes map[string]map[string]interface{}) interface{} {
	ns, _ := parentNS.(string)
	switch ft := fieldType.(type) {
	case map[string]interface{}:
		typeName, _ := ft["type"].(string)
		switch typeName {
		case "record", "enum", "fixed":
			fullName := getAvroFullName(ft)
			if _, keep := keepTypes[fullName]; keep {
				// This type should be extracted — will be handled by extractAvroNamedTypes
				return ft
			}
			// Not in keep set — recurse into its fields but leave inline
			if typeName == "record" {
				rebuildAvroSchemaSelective(ft, keepTypes)
			}
			return ft
		case "array":
			if items, ok := ft["items"]; ok {
				ft["items"] = replaceAvroSelectiveTypes(items, parentNS, keepTypes)
			}
			return ft
		case "map":
			if values, ok := ft["values"]; ok {
				ft["values"] = replaceAvroSelectiveTypes(values, ns, keepTypes)
			}
			return ft
		default:
			return ft
		}
	case []interface{}:
		result := make([]interface{}, 0, len(ft))
		for _, ut := range ft {
			result = append(result, replaceAvroSelectiveTypes(ut, parentNS, keepTypes))
		}
		return result
	default:
		return ft
	}
}

func buildAvroRootSchema(original map[string]interface{}, extracted map[string]map[string]interface{}, rootName string) map[string]interface{} {
	if rootSchema, ok := extracted[rootName]; ok {
		return rootSchema
	}
	return original
}

// ========================
// Protobuf splitting
// ========================

func splitProtobufSchema(content, filename string, minSize int, subjectPrefix string) (*SplitResult, error) {
	// Parse protobuf messages
	messages := parseProtobufMessages(content)

	if len(messages) <= 1 {
		rootName := "root"
		if len(messages) == 1 {
			rootName = messages[0].Name
		}
		return &SplitResult{
			OriginalSize:      len(content),
			SchemaType:        "PROTOBUF",
			Types:             []ExtractedType{{Name: rootName, Subject: rootName, Schema: content, SchemaType: "PROTOBUF", Size: len(content), IsRoot: true, Order: 0}},
			RegistrationOrder: []string{rootName},
		}, nil
	}

	// Extract package and syntax
	pkg := extractProtobufPackage(content)
	syntax := extractProtobufSyntax(content)

	// Build individual .proto files for each message
	var types []ExtractedType
	deps := make(map[string][]string)

	// Find which messages reference which other messages
	msgNames := make(map[string]bool)
	for _, msg := range messages {
		msgNames[msg.Name] = true
	}

	for _, msg := range messages {
		// Check which other messages this one references
		var msgDeps []string
		for _, other := range messages {
			if other.Name != msg.Name && strings.Contains(msg.Body, other.Name) {
				msgDeps = append(msgDeps, other.Name)
			}
		}
		deps[msg.Name] = msgDeps
	}

	// Determine root message (the first one, or the one that isn't referenced by others)
	rootName := messages[0].Name
	referencedBy := make(map[string]bool)
	for _, depList := range deps {
		for _, d := range depList {
			referencedBy[d] = true
		}
	}
	for _, msg := range messages {
		if !referencedBy[msg.Name] {
			rootName = msg.Name
			break
		}
	}

	// Topological sort
	regOrder := topologicalSort(deps, rootName)

	// Build proto file for each message
	for i, name := range regOrder {
		isRoot := name == rootName
		var msg protoMessage
		for _, m := range messages {
			if m.Name == name {
				msg = m
				break
			}
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("syntax = \"%s\";\n", syntax))
		if pkg != "" {
			sb.WriteString(fmt.Sprintf("package %s;\n\n", pkg))
		}

		// Add imports for dependencies
		for _, dep := range deps[name] {
			fileName := toSnakeCase(dep) + ".proto"
			sb.WriteString(fmt.Sprintf("import \"%s\";\n", fileName))
		}
		if len(deps[name]) > 0 {
			sb.WriteString("\n")
		}

		sb.WriteString(fmt.Sprintf("message %s {\n%s}\n", msg.Name, msg.Body))

		schemaContent := sb.String()

		subject := toSnakeCase(name) + ".proto"
		if subjectPrefix != "" && !isRoot {
			subject = subjectPrefix + toSnakeCase(name) + ".proto"
		}

		// Filter deps to only include extracted types
		var filteredDeps []string
		for _, d := range deps[name] {
			filteredDeps = append(filteredDeps, d)
		}

		types = append(types, ExtractedType{
			Name:       name,
			Subject:    subject,
			Schema:     schemaContent,
			SchemaType: "PROTOBUF",
			Size:       len(schemaContent),
			References: filteredDeps,
			IsRoot:     isRoot,
			Order:      i,
		})
	}

	return &SplitResult{
		OriginalSize:      len(content),
		SchemaType:        "PROTOBUF",
		Types:             types,
		RegistrationOrder: regOrder,
	}, nil
}

type protoMessage struct {
	Name string
	Body string // The body between braces
}

func parseProtobufMessages(content string) []protoMessage {
	var messages []protoMessage

	// Match top-level message definitions
	re := regexp.MustCompile(`(?m)^message\s+(\w+)\s*\{`)
	matches := re.FindAllStringSubmatchIndex(content, -1)

	for _, match := range matches {
		name := content[match[2]:match[3]]
		braceStart := match[0]
		// Find matching closing brace
		depth := 0
		bodyStart := -1
		bodyEnd := -1
		for i := braceStart; i < len(content); i++ {
			if content[i] == '{' {
				if depth == 0 {
					bodyStart = i + 1
				}
				depth++
			} else if content[i] == '}' {
				depth--
				if depth == 0 {
					bodyEnd = i
					break
				}
			}
		}
		if bodyStart >= 0 && bodyEnd >= 0 {
			messages = append(messages, protoMessage{
				Name: name,
				Body: content[bodyStart:bodyEnd],
			})
		}
	}

	return messages
}

func extractProtobufPackage(content string) string {
	re := regexp.MustCompile(`(?m)^package\s+([\w.]+)\s*;`)
	matches := re.FindStringSubmatch(content)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

func extractProtobufSyntax(content string) string {
	re := regexp.MustCompile(`syntax\s*=\s*"(proto[23])"\s*;`)
	matches := re.FindStringSubmatch(content)
	if len(matches) >= 2 {
		return matches[1]
	}
	return "proto3"
}

// ========================
// JSON Schema splitting
// ========================

func splitJSONSchema(content string, minSize int, subjectPrefix string) (*SplitResult, error) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse JSON schema: %w", err)
	}

	// Extract named object types from properties
	extracted := make(map[string]map[string]interface{})
	deps := make(map[string][]string)
	rootName := "root"
	if id, ok := schema["$id"].(string); ok {
		rootName = id
	}

	// Walk properties and extract nested objects
	extractJSONSchemaTypes(schema, rootName, extracted, deps, "")

	if len(extracted) <= 1 {
		return &SplitResult{
			OriginalSize:      len(content),
			SchemaType:        "JSON",
			Types:             []ExtractedType{{Name: rootName, Subject: rootName, Schema: content, SchemaType: "JSON", Size: len(content), IsRoot: true, Order: 0}},
			RegistrationOrder: []string{rootName},
		}, nil
	}

	// Filter by min size
	if minSize > 0 {
		for name, typeSchema := range extracted {
			if name == rootName {
				continue
			}
			serialized, _ := json.Marshal(typeSchema)
			if len(serialized) < minSize {
				delete(extracted, name)
				delete(deps, name)
			}
		}
	}

	// Topological sort
	regOrder := topologicalSort(deps, rootName)

	result := &SplitResult{
		OriginalSize:      len(content),
		SchemaType:        "JSON",
		RegistrationOrder: regOrder,
	}

	for i, name := range regOrder {
		isRoot := name == rootName
		serialized, _ := json.MarshalIndent(extracted[name], "", "  ")
		schemaContent := string(serialized)

		subject := name
		if !strings.HasSuffix(subject, ".json") {
			subject += ".json"
		}
		if subjectPrefix != "" && !isRoot {
			subject = subjectPrefix + subject
		}

		var filteredDeps []string
		for _, dep := range deps[name] {
			if _, exists := extracted[dep]; exists && dep != name {
				filteredDeps = append(filteredDeps, dep)
			}
		}

		result.Types = append(result.Types, ExtractedType{
			Name:       name,
			Subject:    subject,
			Schema:     schemaContent,
			SchemaType: "JSON",
			Size:       len(schemaContent),
			References: filteredDeps,
			IsRoot:     isRoot,
			Order:      i,
		})
	}

	return result, nil
}

func extractJSONSchemaTypes(schema map[string]interface{}, name string, extracted map[string]map[string]interface{}, deps map[string][]string, parentName string) {
	schemaType, _ := schema["type"].(string)

	if schemaType == "object" {
		// Build the extracted schema
		extractedSchema := make(map[string]interface{})
		extractedSchema["$schema"] = "http://json-schema.org/draft-07/schema#"

		refName := name
		if !strings.HasSuffix(refName, ".json") {
			refName += ".json"
		}
		extractedSchema["$id"] = refName
		extractedSchema["type"] = "object"

		// Copy additional properties
		for _, key := range []string{"required", "additionalProperties", "description", "title"} {
			if v, ok := schema[key]; ok {
				extractedSchema[key] = v
			}
		}

		// Process properties
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			newProps := make(map[string]interface{})
			for propName, propValue := range props {
				propMap, ok := propValue.(map[string]interface{})
				if !ok {
					newProps[propName] = propValue
					continue
				}

				propType, _ := propMap["type"].(string)
				if propType == "object" && len(propMap) > 1 {
					// Extract this nested object as a separate schema
					childName := propName
					extractJSONSchemaTypes(propMap, childName, extracted, deps, name)
					childRef := childName
					if !strings.HasSuffix(childRef, ".json") {
						childRef += ".json"
					}
					newProps[propName] = map[string]interface{}{"$ref": childRef}
					deps[name] = appendUnique(deps[name], childName)
				} else if propType == "array" {
					if items, ok := propMap["items"].(map[string]interface{}); ok {
						itemType, _ := items["type"].(string)
						if itemType == "object" && len(items) > 1 {
							childName := propName + "_item"
							extractJSONSchemaTypes(items, childName, extracted, deps, name)
							childRef := childName
							if !strings.HasSuffix(childRef, ".json") {
								childRef += ".json"
							}
							newProps[propName] = map[string]interface{}{
								"type":  "array",
								"items": map[string]interface{}{"$ref": childRef},
							}
							deps[name] = appendUnique(deps[name], childName)
						} else {
							newProps[propName] = propValue
						}
					} else {
						newProps[propName] = propValue
					}
				} else {
					newProps[propName] = propValue
				}
			}
			extractedSchema["properties"] = newProps
		}

		extracted[name] = extractedSchema
	}
}

// ========================
// Utility functions
// ========================

func topologicalSort(deps map[string][]string, rootName string) []string {
	// Collect all nodes
	allNodes := make(map[string]bool)
	for name := range deps {
		allNodes[name] = true
		for _, dep := range deps[name] {
			allNodes[dep] = true
		}
	}

	visited := make(map[string]bool)
	inStack := make(map[string]bool)
	var result []string

	var visit func(name string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		if inStack[name] {
			// Circular dependency - skip
			return
		}
		inStack[name] = true

		for _, dep := range deps[name] {
			if dep != name { // Skip self-references
				visit(dep)
			}
		}

		inStack[name] = false
		visited[name] = true
		result = append(result, name)
	}

	// Visit all nodes, but ensure root is visited last
	for name := range allNodes {
		if name != rootName {
			visit(name)
		}
	}
	visit(rootName)

	return result
}

func appendUnique(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

func shortName(fullName string) string {
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

func sanitizeFilename(name string) string {
	// Replace dots and special characters with underscores
	replacer := strings.NewReplacer(
		".", "_",
		"/", "_",
		":", "_",
		" ", "_",
	)
	return replacer.Replace(name)
}

func getExtensionForType(schemaType string) string {
	switch strings.ToUpper(schemaType) {
	case "AVRO":
		return ".avsc"
	case "PROTOBUF":
		return ".proto"
	case "JSON":
		return ".json"
	default:
		return ".schema"
	}
}

func getReferenceName(t *ExtractedType, schemaType string) string {
	switch strings.ToUpper(schemaType) {
	case "AVRO":
		return t.Name // Fully qualified type name
	case "PROTOBUF":
		return toSnakeCase(shortName(t.Name)) + ".proto" // Import path
	case "JSON":
		name := t.Name
		if !strings.HasSuffix(name, ".json") {
			name += ".json"
		}
		return name // $ref URI
	default:
		return t.Name
	}
}

func toSnakeCase(name string) string {
	// Convert CamelCase to snake_case
	var result strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// sortTypesByDeps sorts types so dependencies come first
func sortTypesByDeps(types []ExtractedType) {
	sort.SliceStable(types, func(i, j int) bool {
		return types[i].Order < types[j].Order
	})
}
