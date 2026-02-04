package cmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	getVersion      string
	getWithRefs     bool
	getSchemaID     int
	getPrettySchema bool
)

var getCmd = &cobra.Command{
	Use:     "get <subject>",
	Short:   "Get schema from registry",
	GroupID: groupSchema,
	Long: `Get a schema from the Schema Registry by subject name or by schema ID.

Examples:
  # Get latest schema for a subject
  srctl get user-events

  # Get specific version
  srctl get user-events --version 3

  # Get schema by ID
  srctl get --id 12345

  # Get schema with all referenced schemas
  srctl get user-events --with-refs

  # Get from specific context
  srctl get user-events --context .mycontext

  # Output as JSON with pretty-printed schema
  srctl get user-events -o json --pretty`,
	Args: func(cmd *cobra.Command, args []string) error {
		if getSchemaID > 0 {
			return nil // Subject not required when using --id
		}
		if len(args) != 1 {
			return fmt.Errorf("requires subject name argument (or use --id flag)")
		}
		return nil
	},
	RunE: runGet,
}

func init() {
	getCmd.Flags().StringVarP(&getVersion, "version", "v", "latest", "Schema version (number or 'latest')")
	getCmd.Flags().BoolVar(&getWithRefs, "with-refs", false, "Include all referenced schemas")
	getCmd.Flags().IntVar(&getSchemaID, "id", 0, "Get schema by global ID instead of subject")
	getCmd.Flags().BoolVar(&getPrettySchema, "pretty", false, "Pretty print the schema content")

	rootCmd.AddCommand(getCmd)
}

func runGet(cmd *cobra.Command, args []string) error {
	srClient, err := GetClient()
	if err != nil {
		return err
	}

	printer := output.NewPrinter(outputFormat)

	// Get by ID
	if getSchemaID > 0 {
		schema, err := srClient.GetSchemaByID(getSchemaID)
		if err != nil {
			return fmt.Errorf("failed to get schema: %w", err)
		}

		// Get subjects/versions that use this schema
		subjectVersions, err := srClient.GetSchemaSubjectVersionsByID(getSchemaID)
		if err != nil {
			output.Warning("Could not retrieve subject versions: %v", err)
		}

		result := map[string]interface{}{
			"id":         schema.ID,
			"schemaType": schema.SchemaType,
			"schema":     formatSchema(schema.Schema, getPrettySchema),
		}
		if len(schema.References) > 0 {
			result["references"] = schema.References
		}
		if len(subjectVersions) > 0 {
			result["usedBy"] = subjectVersions
		}

		return printer.Print(result)
	}

	// Get by subject
	subject := args[0]
	schema, err := srClient.GetSchema(subject, getVersion)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Collect referenced schemas if requested
	var refSchemas []map[string]interface{}
	if getWithRefs && len(schema.References) > 0 {
		refSchemas, err = collectReferences(srClient, schema.References, make(map[string]bool))
		if err != nil {
			output.Warning("Could not retrieve all references: %v", err)
		}
	}

	if outputFormat == "table" {
		printSchemaTable(schema, refSchemas)
		return nil
	}

	result := map[string]interface{}{
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
		"schemaType": schema.SchemaType,
		"schema":     formatSchema(schema.Schema, getPrettySchema),
	}
	if len(schema.References) > 0 {
		result["references"] = schema.References
	}
	if len(refSchemas) > 0 {
		result["referencedSchemas"] = refSchemas
	}

	return printer.Print(result)
}

func collectReferences(c *client.SchemaRegistryClient, refs []client.SchemaReference, visited map[string]bool) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, ref := range refs {
		key := fmt.Sprintf("%s:%d", ref.Subject, ref.Version)
		if visited[key] {
			continue
		}
		visited[key] = true

		schema, err := c.GetSchema(ref.Subject, strconv.Itoa(ref.Version))
		if err != nil {
			continue
		}

		schemaType := schema.SchemaType
		if schemaType == "" {
			schemaType = "AVRO"
		}

		refData := map[string]interface{}{
			"name":       ref.Name,
			"subject":    ref.Subject,
			"version":    ref.Version,
			"schemaId":   schema.ID,
			"schemaType": schemaType,
			"schema":     schema.Schema,
		}
		result = append(result, refData)

		// Recursively get nested references
		if len(schema.References) > 0 {
			nested, _ := collectReferences(c, schema.References, visited)
			result = append(result, nested...)
		}
	}

	return result, nil
}

func printSchemaTable(schema *client.Schema, refSchemas []map[string]interface{}) {
	fmt.Printf("\n%s\n", color.New(color.Bold).Sprint("Schema Details"))
	fmt.Println(strings.Repeat("─", 60))

	schemaType := schema.SchemaType
	if schemaType == "" {
		schemaType = "AVRO"
	}

	output.PrintTable(
		[]string{"Property", "Value"},
		[][]string{
			{"Subject", schema.Subject},
			{"Version", strconv.Itoa(schema.Version)},
			{"Schema ID", strconv.Itoa(schema.ID)},
			{"Type", schemaType},
		},
	)

	if len(schema.References) > 0 {
		output.SubHeader("References")
		var refRows [][]string
		for _, ref := range schema.References {
			refRows = append(refRows, []string{ref.Name, ref.Subject, strconv.Itoa(ref.Version)})
		}
		output.PrintTable([]string{"Name", "Subject", "Version"}, refRows)
	}

	output.SubHeader("Schema")
	// Pretty print the schema
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema.Schema), &parsed); err == nil {
		pretty, _ := json.MarshalIndent(parsed, "", "  ")
		fmt.Println(string(pretty))
	} else {
		fmt.Println(schema.Schema)
	}
}

func formatSchema(schema string, pretty bool) interface{} {
	if !pretty {
		return schema
	}

	// Try to parse and format as JSON
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err == nil {
		formatted, err := json.MarshalIndent(parsed, "", "  ")
		if err == nil {
			return string(formatted)
		}
	}
	return schema
}

// ContextsCmd for getting contexts
var contextsCmd = &cobra.Command{
	Use:   "contexts",
	Short: "List all contexts in the Schema Registry",
	Long: `List all contexts (tenants) available in the Schema Registry.

Contexts allow logical separation of schemas within a single Schema Registry cluster.

Examples:
  # List all contexts
  srctl contexts

  # Output as JSON
  srctl contexts -o json`,
	RunE: runContexts,
}

func init() {
	rootCmd.AddCommand(contextsCmd)
}

func runContexts(cmd *cobra.Command, args []string) error {
	srClient, err := GetClient()
	if err != nil {
		return err
	}

	contexts, err := srClient.GetContexts()
	if err != nil {
		return fmt.Errorf("failed to get contexts: %w", err)
	}

	printer := output.NewPrinter(outputFormat)

	if outputFormat == "table" {
		output.Header("Schema Registry Contexts")
		if len(contexts) == 0 {
			output.Info("No contexts found (using default context)")
			return nil
		}

		rows := make([][]string, len(contexts))
		for i, ctx := range contexts {
			rows[i] = []string{strconv.Itoa(i + 1), ctx}
		}
		output.PrintTable([]string{"#", "Context"}, rows)
		fmt.Printf("\nTotal: %d context(s)\n", len(contexts))
		return nil
	}

	return printer.Print(contexts)
}
