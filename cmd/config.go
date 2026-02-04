package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/output"
)

var configCmd = &cobra.Command{
	Use:     "config",
	Short:   "Manage Schema Registry compatibility configuration",
	GroupID: groupConfig,
	Long: `View and manage compatibility settings at different levels:
  • Global (registry-wide default)
  • Context level
  • Subject level

Compatibility Levels:
  NONE            - No compatibility checking
  BACKWARD        - New schema can read old data
  BACKWARD_TRANSITIVE - New schema can read all previous versions
  FORWARD         - Old schema can read new data  
  FORWARD_TRANSITIVE  - All previous versions can read new data
  FULL            - Both backward and forward compatible
  FULL_TRANSITIVE - Full compatibility with all previous versions

Examples:
  # View global configuration
  srctl config

  # View configuration for a subject
  srctl config user-events

  # Set global compatibility
  srctl config --set BACKWARD_TRANSITIVE

  # Set subject-level compatibility
  srctl config user-events --set FULL

  # View configuration summary for all levels
  srctl config --all`,
}

var (
	configSet     string
	configShowAll bool
)

func init() {
	configCmd.Flags().StringVar(&configSet, "set", "", "Set compatibility level")
	configCmd.Flags().BoolVar(&configShowAll, "all", false, "Show configuration at all levels")

	configCmd.RunE = runConfig
	rootCmd.AddCommand(configCmd)
}

func runConfig(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	printer := output.NewPrinter(outputFormat)

	// Set configuration
	if configSet != "" {
		return setConfig(c, args, configSet)
	}

	// Show all levels
	if configShowAll {
		return showAllConfig(c)
	}

	// Show specific subject config
	if len(args) > 0 {
		return showSubjectConfig(c, args[0], printer)
	}

	// Show global config
	return showGlobalConfig(c, printer)
}

func setConfig(c *client.SchemaRegistryClient, args []string, level string) error {
	level = strings.ToUpper(level)

	validLevels := map[string]bool{
		"NONE": true, "BACKWARD": true, "BACKWARD_TRANSITIVE": true,
		"FORWARD": true, "FORWARD_TRANSITIVE": true,
		"FULL": true, "FULL_TRANSITIVE": true,
	}

	if !validLevels[level] {
		return fmt.Errorf("invalid compatibility level: %s", level)
	}

	if len(args) > 0 {
		// Set subject-level config
		subject := args[0]
		output.Step("Setting compatibility for subject: %s", subject)
		if err := c.SetSubjectConfig(subject, level); err != nil {
			return fmt.Errorf("failed to set subject config: %w", err)
		}
		output.Success("Compatibility set to %s for subject %s", level, subject)
	} else {
		// Set global config
		output.Step("Setting global compatibility")
		if err := c.SetConfig(level); err != nil {
			return fmt.Errorf("failed to set global config: %w", err)
		}
		output.Success("Global compatibility set to %s", level)
	}

	return nil
}

func showGlobalConfig(c *client.SchemaRegistryClient, printer *output.Printer) error {
	config, err := c.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get config: %w", err)
	}

	level := config.CompatibilityLevel
	if level == "" {
		level = config.Compatibility
	}
	if level == "" {
		level = "BACKWARD" // Default
	}

	if outputFormat == "table" {
		output.Header("Global Configuration")
		output.PrintTable(
			[]string{"Setting", "Value"},
			[][]string{
				{"Compatibility Level", level},
			},
		)
		return nil
	}

	return printer.Print(map[string]string{
		"level":         "global",
		"compatibility": level,
	})
}

func showSubjectConfig(c *client.SchemaRegistryClient, subject string, printer *output.Printer) error {
	// Get global config
	globalConfig, err := c.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get global config: %w", err)
	}

	globalLevel := globalConfig.CompatibilityLevel
	if globalLevel == "" {
		globalLevel = globalConfig.Compatibility
	}
	if globalLevel == "" {
		globalLevel = "BACKWARD"
	}

	// Get subject-specific config
	subjectConfig, err := c.GetSubjectConfig(subject, false)
	var subjectLevel string
	var effectiveLevel string

	if err != nil || subjectConfig == nil {
		subjectLevel = "(not set - using global)"
		effectiveLevel = globalLevel
	} else {
		subjectLevel = subjectConfig.CompatibilityLevel
		if subjectLevel == "" {
			subjectLevel = subjectConfig.Compatibility
		}
		effectiveLevel = subjectLevel
	}

	if outputFormat == "table" {
		output.Header("Configuration for: %s", subject)
		output.PrintTable(
			[]string{"Level", "Compatibility"},
			[][]string{
				{"Global", globalLevel},
				{"Subject", subjectLevel},
				{"Effective", effectiveLevel},
			},
		)
		return nil
	}

	return printer.Print(map[string]interface{}{
		"subject":       subject,
		"global":        globalLevel,
		"subject_level": subjectLevel,
		"effective":     effectiveLevel,
	})
}

func showAllConfig(c *client.SchemaRegistryClient) error {
	output.Header("Configuration Summary")

	// Global config
	globalConfig, err := c.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get global config: %w", err)
	}

	globalLevel := globalConfig.CompatibilityLevel
	if globalLevel == "" {
		globalLevel = globalConfig.Compatibility
	}
	if globalLevel == "" {
		globalLevel = "BACKWARD"
	}

	output.SubHeader("Global Configuration")
	output.PrintTable(
		[]string{"Setting", "Value"},
		[][]string{{"Compatibility Level", globalLevel}},
	)

	// Subject-level configs
	subjects, err := c.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	var rows [][]string
	for _, subj := range subjects {
		config, err := c.GetSubjectConfig(subj, false)
		if err != nil || config == nil {
			continue // No specific config
		}

		level := config.CompatibilityLevel
		if level == "" {
			level = config.Compatibility
		}
		if level != "" {
			rows = append(rows, []string{subj, level})
		}
	}

	if len(rows) > 0 {
		output.SubHeader("Subject-Level Overrides")
		output.PrintTable([]string{"Subject", "Compatibility"}, rows)
	} else {
		output.Info("\nNo subject-level configuration overrides")
	}

	return nil
}

// Mode command
var modeCmd = &cobra.Command{
	Use:   "mode",
	Short: "Manage Schema Registry mode",
	Long: `View and manage the mode at different levels.

Modes:
  READWRITE  - Normal operation (default)
  READONLY   - Only allow reads
  IMPORT     - Allow importing schemas with specific IDs

Examples:
  # View global mode
  srctl mode

  # View mode for a subject
  srctl mode user-events

  # Set global mode
  srctl mode --set READONLY

  # Set subject-level mode
  srctl mode user-events --set IMPORT

  # View mode at all levels
  srctl mode --all`,
	RunE: runMode,
}

var (
	modeSet     string
	modeShowAll bool
)

func init() {
	modeCmd.Flags().StringVar(&modeSet, "set", "", "Set mode (READWRITE, READONLY, IMPORT)")
	modeCmd.Flags().BoolVar(&modeShowAll, "all", false, "Show mode at all levels")

	rootCmd.AddCommand(modeCmd)
}

func runMode(cmd *cobra.Command, args []string) error {
	c, err := GetClient()
	if err != nil {
		return err
	}

	printer := output.NewPrinter(outputFormat)

	// Set mode
	if modeSet != "" {
		return setMode(c, args, modeSet)
	}

	// Show all levels
	if modeShowAll {
		return showAllMode(c)
	}

	// Show specific subject mode
	if len(args) > 0 {
		return showSubjectMode(c, args[0], printer)
	}

	// Show global mode
	return showGlobalMode(c, printer)
}

func setMode(c *client.SchemaRegistryClient, args []string, mode string) error {
	mode = strings.ToUpper(mode)

	validModes := map[string]bool{
		"READWRITE": true, "READONLY": true, "IMPORT": true,
	}

	if !validModes[mode] {
		return fmt.Errorf("invalid mode: %s (valid: READWRITE, READONLY, IMPORT)", mode)
	}

	if len(args) > 0 {
		subject := args[0]
		output.Step("Setting mode for subject: %s", subject)
		if err := c.SetSubjectMode(subject, mode); err != nil {
			return fmt.Errorf("failed to set subject mode: %w", err)
		}
		output.Success("Mode set to %s for subject %s", mode, subject)
	} else {
		output.Step("Setting global mode")
		if err := c.SetMode(mode); err != nil {
			return fmt.Errorf("failed to set global mode: %w", err)
		}
		output.Success("Global mode set to %s", mode)
	}

	return nil
}

func showGlobalMode(c *client.SchemaRegistryClient, printer *output.Printer) error {
	mode, err := c.GetMode()
	if err != nil {
		return fmt.Errorf("failed to get mode: %w", err)
	}

	modeStr := mode.Mode
	if modeStr == "" {
		modeStr = "READWRITE"
	}

	if outputFormat == "table" {
		output.Header("Global Mode")
		output.PrintTable(
			[]string{"Setting", "Value"},
			[][]string{{"Mode", modeStr}},
		)
		return nil
	}

	return printer.Print(map[string]string{
		"level": "global",
		"mode":  modeStr,
	})
}

func showSubjectMode(c *client.SchemaRegistryClient, subject string, printer *output.Printer) error {
	// Get global mode
	globalMode, err := c.GetMode()
	if err != nil {
		return fmt.Errorf("failed to get global mode: %w", err)
	}

	globalModeStr := globalMode.Mode
	if globalModeStr == "" {
		globalModeStr = "READWRITE"
	}

	// Get subject mode
	subjectMode, err := c.GetSubjectMode(subject, false)
	var subjectModeStr string
	var effectiveMode string

	if err != nil || subjectMode == nil {
		subjectModeStr = "(not set - using global)"
		effectiveMode = globalModeStr
	} else {
		subjectModeStr = subjectMode.Mode
		effectiveMode = subjectModeStr
	}

	if outputFormat == "table" {
		output.Header("Mode for: %s", subject)
		output.PrintTable(
			[]string{"Level", "Mode"},
			[][]string{
				{"Global", globalModeStr},
				{"Subject", subjectModeStr},
				{"Effective", effectiveMode},
			},
		)
		return nil
	}

	return printer.Print(map[string]interface{}{
		"subject":       subject,
		"global":        globalModeStr,
		"subject_level": subjectModeStr,
		"effective":     effectiveMode,
	})
}

func showAllMode(c *client.SchemaRegistryClient) error {
	output.Header("Mode Summary")

	// Global mode
	globalMode, err := c.GetMode()
	if err != nil {
		return fmt.Errorf("failed to get global mode: %w", err)
	}

	globalModeStr := globalMode.Mode
	if globalModeStr == "" {
		globalModeStr = "READWRITE"
	}

	output.SubHeader("Global Mode")
	output.PrintTable(
		[]string{"Setting", "Value"},
		[][]string{{"Mode", globalModeStr}},
	)

	// Subject-level modes
	subjects, err := c.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}

	var rows [][]string
	for _, subj := range subjects {
		mode, err := c.GetSubjectMode(subj, false)
		if err != nil || mode == nil {
			continue
		}
		if mode.Mode != "" {
			rows = append(rows, []string{subj, mode.Mode})
		}
	}

	if len(rows) > 0 {
		output.SubHeader("Subject-Level Mode Overrides")
		output.PrintTable([]string{"Subject", "Mode"}, rows)
	} else {
		output.Info("\nNo subject-level mode overrides")
	}

	return nil
}
