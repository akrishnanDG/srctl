package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

// contractCmd is the parent command for data contract operations
var contractCmd = &cobra.Command{
	Use:     "contract",
	Aliases: []string{"dc", "data-contract"},
	Short:   "Data contract operations",
	GroupID: groupContract,
	Long: `Manage data contracts in the Schema Registry.

Data contracts provide a way to define and enforce rules about
how data should be structured and evolve over time.

Subcommands:
  • get       - Get data contract rules for a subject
  • set       - Set data contract rules for a subject
  • delete    - Delete data contract rules
  • validate  - Validate a schema against data contract rules

Examples:
  # Get data contract rules
  srctl contract get user-events

  # Set data contract rules from file
  srctl contract set user-events --rules rules.json

  # Delete data contract rules
  srctl contract delete user-events`,
}

// contractGetCmd gets data contract rules
var contractGetCmd = &cobra.Command{
	Use:   "get <subject>",
	Short: "Get data contract rules for a subject",
	Long: `Get data contract rules for a subject.

Examples:
  # Get all rules for a subject
  srctl contract get user-events

  # Get rules and output as JSON
  srctl contract get user-events --json`,
	Args: cobra.ExactArgs(1),
	RunE: runContractGet,
}

// contractSetCmd sets data contract rules
var contractSetCmd = &cobra.Command{
	Use:   "set <subject>",
	Short: "Set data contract rules for a subject",
	Long: `Set data contract rules for a subject.

Rules can be defined inline or loaded from a file.

Examples:
  # Set rules from a file
  srctl contract set user-events --rules rules.json

  # Set rules inline
  srctl contract set user-events --rule "REQUIRED:$.user_id" --rule "NUMERIC:$.amount"`,
	Args: cobra.ExactArgs(1),
	RunE: runContractSet,
}

// contractDeleteCmd deletes data contract rules
var contractDeleteCmd = &cobra.Command{
	Use:   "delete <subject>",
	Short: "Delete data contract rules for a subject",
	Long: `Delete data contract rules for a subject.

Examples:
  # Delete all rules
  srctl contract delete user-events

  # Delete specific rule
  srctl contract delete user-events --rule "REQUIRED:$.user_id"`,
	Args: cobra.ExactArgs(1),
	RunE: runContractDelete,
}

// contractValidateCmd validates a schema against rules
var contractValidateCmd = &cobra.Command{
	Use:   "validate <subject>",
	Short: "Validate a schema against data contract rules",
	Long: `Validate a schema against data contract rules.

Examples:
  # Validate a schema file
  srctl contract validate user-events --schema schema.avsc

  # Validate latest version
  srctl contract validate user-events --version latest`,
	Args: cobra.ExactArgs(1),
	RunE: runContractValidate,
}

var (
	contractRulesFile string
	contractRules     []string
	contractSchema    string
	contractVersion   string
	contractJSON      bool
)

func init() {
	// Contract get flags
	contractGetCmd.Flags().BoolVar(&contractJSON, "json", false, "Output as JSON")

	// Contract set flags
	contractSetCmd.Flags().StringVar(&contractRulesFile, "rules", "", "Path to rules file (JSON)")
	contractSetCmd.Flags().StringSliceVar(&contractRules, "rule", nil, "Individual rule (format: TYPE:PATH)")

	// Contract delete flags
	contractDeleteCmd.Flags().StringSliceVar(&contractRules, "rule", nil, "Specific rule to delete")

	// Contract validate flags
	contractValidateCmd.Flags().StringVar(&contractSchema, "schema", "", "Path to schema file")
	contractValidateCmd.Flags().StringVar(&contractVersion, "version", "latest", "Schema version to validate")

	// Add subcommands
	contractCmd.AddCommand(contractGetCmd)
	contractCmd.AddCommand(contractSetCmd)
	contractCmd.AddCommand(contractDeleteCmd)
	contractCmd.AddCommand(contractValidateCmd)

	// Add to root
	rootCmd.AddCommand(contractCmd)
}

// DataContractRule represents a data contract rule
type DataContractRule struct {
	Type    string            `json:"type"`
	Mode    string            `json:"mode,omitempty"`
	Tags    []string          `json:"tags,omitempty"`
	Params  map[string]string `json:"params,omitempty"`
	Doc     string            `json:"doc,omitempty"`
	Expr    string            `json:"expr,omitempty"`
	OnError string            `json:"onError,omitempty"`
}

// DataContractRuleset represents a set of rules
type DataContractRuleset struct {
	MigrationRules []DataContractRule `json:"migrationRules,omitempty"`
	DomainRules    []DataContractRule `json:"domainRules,omitempty"`
}

// DataContract represents the full data contract config
type DataContract struct {
	Metadata map[string]string   `json:"metadata,omitempty"`
	RuleSet  DataContractRuleset `json:"ruleSet,omitempty"`
}

func runContractGet(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]
	output.Header("Data Contract Rules: %s", subject)

	// Get subject config which includes rules
	config, err := c.GetSubjectConfig(subject, true)
	if err != nil {
		return fmt.Errorf("failed to get subject config: %w", err)
	}

	if config == nil {
		output.Info("No data contract rules found for %s", subject)
		return nil
	}

	if contractJSON {
		data, _ := json.MarshalIndent(config, "", "  ")
		fmt.Println(string(data))
	} else {
		output.Info("Compatibility: %s", getCompatibility(config))
		// Note: Actual rules would be in ruleSet field
		output.Info("Use --json flag to see full configuration including rules")
	}

	return nil
}

func runContractSet(cmd *cobra.Command, args []string) error {
	_, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]

	if contractRulesFile == "" && len(contractRules) == 0 {
		return fmt.Errorf("either --rules file or --rule flags required")
	}

	output.Header("Setting Data Contract Rules: %s", subject)

	// Note: The actual implementation would need to use the Schema Registry's
	// rule-based configuration API. This is a placeholder that shows the structure.
	output.Warning("Data contract rules API requires Schema Registry with rules support")
	output.Info("Rules would be set for subject: %s", subject)

	if contractRulesFile != "" {
		data, err := os.ReadFile(contractRulesFile)
		if err != nil {
			return fmt.Errorf("failed to read rules file: %w", err)
		}
		output.Info("Rules from file: %s", contractRulesFile)
		fmt.Println(string(data))
	}

	for _, rule := range contractRules {
		output.Info("Rule: %s", rule)
	}

	return nil
}

func runContractDelete(cmd *cobra.Command, args []string) error {
	_, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]
	output.Header("Deleting Data Contract Rules: %s", subject)

	if len(contractRules) > 0 {
		for _, rule := range contractRules {
			output.Step("Deleting rule: %s", rule)
		}
	} else {
		output.Step("Deleting all rules for subject: %s", subject)
	}

	// Note: Actual implementation would call the rules API
	output.Success("Rules deleted (placeholder - requires rules API)")

	return nil
}

func runContractValidate(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	subject := args[0]
	output.Header("Validating Data Contract: %s", subject)

	var schemaContent string

	if contractSchema != "" {
		data, err := os.ReadFile(contractSchema)
		if err != nil {
			return fmt.Errorf("failed to read schema file: %w", err)
		}
		schemaContent = string(data)
		output.Info("Schema: %s", contractSchema)
	} else {
		// Get schema from registry
		schema, err := c.GetSchema(subject, contractVersion)
		if err != nil {
			return fmt.Errorf("failed to get schema: %w", err)
		}
		schemaContent = schema.Schema
		output.Info("Schema version: %s", contractVersion)
	}

	// Check compatibility (basic validation)
	schema := &client.Schema{Schema: schemaContent}
	compatible, err := c.CheckCompatibility(subject, schema, "latest")
	if err != nil {
		output.Warning("Compatibility check failed: %v", err)
	} else if compatible {
		output.Success("Schema is compatible")
	} else {
		output.Error("Schema is NOT compatible")
	}

	// Note: Full data contract validation would check rules here
	output.Info("Full data contract validation requires rules API support")

	return nil
}

func getCompatibility(config *client.Config) string {
	if config.CompatibilityLevel != "" {
		return config.CompatibilityLevel
	}
	if config.Compatibility != "" {
		return config.Compatibility
	}
	return "BACKWARD (default)"
}
