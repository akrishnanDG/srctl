package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var (
	registerFile       string
	registerSchemaType string
	registerReferences []string
	registerDryRun     bool
	registerNormalize  bool
)

var registerCmd = &cobra.Command{
	Use:     "register <subject>",
	Short:   "Register a new schema version",
	GroupID: groupSchema,
	Long: `Register a new schema version under a subject.

The schema can be provided via file (--file) or stdin.

Examples:
  # Register from file
  srctl register user-events --file ./schemas/user.avsc

  # Register with explicit type
  srctl register user-events --file ./schemas/user.proto --type PROTOBUF

  # Register with references
  srctl register user-events --file ./schemas/user.avsc \
    --ref "common.Address=address-value:1"

  # Register in specific context
  srctl register user-events --file ./schemas/user.avsc --context .mycontext

  # Dry run - check compatibility without registering
  srctl register user-events --file ./schemas/user.avsc --dry-run

  # Register from stdin
  cat schema.avsc | srctl register user-events`,
	Args: cobra.ExactArgs(1),
	RunE: runRegister,
}

func init() {
	registerCmd.Flags().StringVarP(&registerFile, "file", "f", "", "Path to schema file")
	registerCmd.Flags().StringVarP(&registerSchemaType, "type", "t", "", "Schema type: AVRO, PROTOBUF, JSON")
	registerCmd.Flags().StringArrayVar(&registerReferences, "ref", nil, "Schema references (format: name=subject:version)")
	registerCmd.Flags().BoolVar(&registerDryRun, "dry-run", false, "Check compatibility without registering")
	registerCmd.Flags().BoolVar(&registerNormalize, "normalize", false, "Normalize schema before registering")

	rootCmd.AddCommand(registerCmd)
}

func runRegister(cmd *cobra.Command, args []string) error {
	subject := args[0]

	// Read schema content
	var schemaContent string
	var err error

	if registerFile != "" {
		content, err := os.ReadFile(registerFile)
		if err != nil {
			return fmt.Errorf("failed to read schema file: %w", err)
		}
		schemaContent = string(content)
	} else {
		// Read from stdin
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			return fmt.Errorf("no schema provided. Use --file or pipe schema to stdin")
		}
		content, err := os.ReadFile("/dev/stdin")
		if err != nil {
			return fmt.Errorf("failed to read from stdin: %w", err)
		}
		schemaContent = string(content)
	}

	// Detect schema type if not specified
	schemaType := registerSchemaType
	if schemaType == "" {
		schemaType = detectSchemaType(schemaContent, registerFile)
	}

	// Parse references
	refs, err := parseReferences(registerReferences)
	if err != nil {
		return fmt.Errorf("invalid reference format: %w", err)
	}

	// Normalize if requested
	if registerNormalize && schemaType == "AVRO" {
		schemaContent, err = normalizeAvroSchema(schemaContent)
		if err != nil {
			output.Warning("Failed to normalize schema: %v", err)
		}
	}

	// Build schema object
	schema := &client.Schema{
		Schema:     schemaContent,
		SchemaType: schemaType,
		References: refs,
	}

	c, err := GetClient()
	if err != nil {
		return err
	}

	// Dry run - just check compatibility
	if registerDryRun {
		output.Header("Dry Run - Compatibility Check")
		output.Info("Subject: %s", subject)
		output.Info("Type: %s", schemaType)

		// Check if subject exists
		versions, err := c.GetVersions(subject, false)
		if err != nil || len(versions) == 0 {
			output.Success("Subject does not exist - schema will be registered as version 1")
			return nil
		}

		// Check compatibility
		compatible, err := c.CheckCompatibility(subject, schema, "latest")
		if err != nil {
			return fmt.Errorf("compatibility check failed: %w", err)
		}

		if compatible {
			output.Success("Schema is compatible with latest version")
		} else {
			output.Error("Schema is NOT compatible with latest version")
			return fmt.Errorf("schema is not compatible")
		}

		// Show diff with latest
		output.SubHeader("Comparing with latest version...")
		latestSchema, err := c.GetSchema(subject, "latest")
		if err == nil {
			showSchemaDiff(latestSchema.Schema, schemaContent)
		}

		return nil
	}

	// Actually register the schema
	output.Step("Registering schema for subject: %s", subject)
	if schemaType != "" && schemaType != "AVRO" {
		output.Info("Schema type: %s", schemaType)
	}
	if len(refs) > 0 {
		output.Info("References: %d", len(refs))
	}

	id, err := c.RegisterSchema(subject, schema)
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}

	printer := output.NewPrinter(outputFormat)

	if outputFormat == "table" {
		output.Success("Schema registered successfully!")
		fmt.Println()
		output.PrintTable(
			[]string{"Subject", "Schema ID", "Type"},
			[][]string{{subject, fmt.Sprintf("%d", id), schemaType}},
		)
		return nil
	}

	return printer.Print(map[string]interface{}{
		"subject": subject,
		"id":      id,
		"type":    schemaType,
		"context": context,
		"message": "Schema registered successfully",
	})
}

func detectSchemaType(content, filename string) string {
	// Try to detect from file extension
	if filename != "" {
		ext := strings.ToLower(filepath.Ext(filename))
		switch ext {
		case ".avsc", ".avro":
			return "AVRO"
		case ".proto":
			return "PROTOBUF"
		case ".json":
			// Could be AVRO or JSON Schema, try to detect
			if strings.Contains(content, `"$schema"`) || strings.Contains(content, `"$ref"`) {
				return "JSON"
			}
			return "AVRO"
		}
	}

	// Try to detect from content
	content = strings.TrimSpace(content)

	// Protobuf detection
	if strings.HasPrefix(content, "syntax") || strings.Contains(content, "message ") {
		return "PROTOBUF"
	}

	// JSON Schema detection
	if strings.Contains(content, `"$schema"`) {
		return "JSON"
	}

	// Default to AVRO
	return "AVRO"
}

func parseReferences(refs []string) ([]client.SchemaReference, error) {
	var result []client.SchemaReference

	for _, ref := range refs {
		// Format: name=subject:version
		// Note: subject may contain colons (e.g., :.context:subject-name)
		parts := strings.SplitN(ref, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid reference format '%s', expected: name=subject:version", ref)
		}

		name := parts[0]
		subjectVersion := parts[1]

		// Find the last colon to separate subject from version
		// This handles subjects with colons in their names (e.g., context-prefixed subjects)
		lastColon := strings.LastIndex(subjectVersion, ":")
		if lastColon == -1 {
			return nil, fmt.Errorf("invalid reference format '%s', expected: name=subject:version", ref)
		}

		subject := subjectVersion[:lastColon]
		versionStr := subjectVersion[lastColon+1:]

		version := 0
		if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
			return nil, fmt.Errorf("invalid version number in reference '%s'", ref)
		}

		result = append(result, client.SchemaReference{
			Name:    name,
			Subject: subject,
			Version: version,
		})
	}

	return result, nil
}

func normalizeAvroSchema(content string) (string, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return content, err
	}

	// Re-marshal with consistent formatting
	normalized, err := json.Marshal(schema)
	if err != nil {
		return content, err
	}

	return string(normalized), nil
}

func showSchemaDiff(old, new string) {
	// Simple placeholder for diff visualization
	// A full implementation would use a proper diff library
	output.Info("Schema diff visualization would be shown here")
}
