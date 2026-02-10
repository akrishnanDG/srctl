package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/config"
)

var (
	// Global flags
	registryURL  string
	username     string
	password     string
	registryName string
	context      string
	outputFormat string

	rootCmd = &cobra.Command{
		Use:   "srctl",
		Short: "Schema Registry Control - Advanced CLI for Confluent Schema Registry",
		Long: `srctl is a powerful CLI tool for Confluent Schema Registry that provides
advanced capabilities beyond the standard SR CLI.

Configure your registries in ~/.srctl/srctl.yaml or use environment variables:
  SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO

For large registries with many subjects, increase --workers for faster operations.
See 'srctl [command] --help' for command-specific options.`,
		Version: "1.0.0",
	}
)

// Command group IDs
const (
	groupSchema     = "schema"
	groupBulk       = "bulk"
	groupCrossReg   = "crossreg"
	groupContract   = "contract"
	groupConfig     = "config"
)

func init() {
	cobra.OnInitialize(initConfig)

	// Define command groups
	rootCmd.AddGroup(
		&cobra.Group{ID: groupSchema, Title: "Schema Operations:"},
		&cobra.Group{ID: groupBulk, Title: "Bulk & Backup Operations:"},
		&cobra.Group{ID: groupCrossReg, Title: "Cross-Registry Operations:"},
		&cobra.Group{ID: groupContract, Title: "Data Contracts:"},
		&cobra.Group{ID: groupConfig, Title: "Configuration & Analysis:"},
	)

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&registryURL, "url", "u", "", "Schema Registry URL (overrides config)")
	rootCmd.PersistentFlags().StringVar(&username, "username", "", "Basic auth username")
	rootCmd.PersistentFlags().StringVar(&password, "password", "", "Basic auth password")
	rootCmd.PersistentFlags().StringVarP(&registryName, "registry", "r", "", "Registry name from config")
	rootCmd.PersistentFlags().StringVarP(&context, "context", "c", "", "Schema Registry context (e.g., '.mycontext')")
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "table", "Output format: table, json, yaml, plain")
}

func initConfig() {
	if err := config.LoadConfig(); err != nil {
		// Only warn for actual config errors, not missing config files
		fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
	}
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// GetClient returns a configured Schema Registry client based on flags and config
func GetClient() (*client.SchemaRegistryClient, error) {
	var url, user, pass, ctx string

	// Priority: CLI flags > specific registry from config > default registry > env vars

	if registryURL != "" {
		url = registryURL
		user = username
		pass = password
	} else if registryName != "" {
		reg := config.GetRegistry(registryName)
		if reg == nil {
			return nil, fmt.Errorf("registry '%s' not found in config", registryName)
		}
		url = reg.URL
		user = reg.Username
		pass = reg.Password
		ctx = reg.Context
	} else {
		reg := config.GetDefaultRegistry()
		if reg != nil {
			url = reg.URL
			user = reg.Username
			pass = reg.Password
			ctx = reg.Context
		} else {
			// Try environment variables
			url = os.Getenv("SCHEMA_REGISTRY_URL")
			if authInfo := os.Getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"); authInfo != "" {
				// Format: user:password
				if idx := len(authInfo) - len(authInfo); idx >= 0 {
					// Simple split on first colon
					for i, c := range authInfo {
						if c == ':' {
							user = authInfo[:i]
							pass = authInfo[i+1:]
							break
						}
					}
				}
			}
		}
	}

	// Override user/pass from flags if provided
	if username != "" {
		user = username
	}
	if password != "" {
		pass = password
	}

	// Context flag overrides config
	if context != "" {
		ctx = context
	}

	if url == "" {
		return nil, fmt.Errorf("no Schema Registry URL configured. Use --url flag, set SCHEMA_REGISTRY_URL env var, or configure in ~/.srctl/srctl.yaml")
	}

	var auth *client.AuthConfig
	if user != "" {
		auth = &client.AuthConfig{
			Username: user,
			Password: pass,
		}
	}

	c := client.NewClient(url, auth)
	if ctx != "" {
		c = c.WithContext(ctx)
	}

	return c, nil
}

// GetClientForRegistry returns a client for a specific registry by name
func GetClientForRegistry(name string) (*client.SchemaRegistryClient, error) {
	reg := config.GetRegistry(name)
	if reg == nil {
		return nil, fmt.Errorf("registry '%s' not found in config", name)
	}

	var auth *client.AuthConfig
	if reg.Username != "" {
		auth = &client.AuthConfig{
			Username: reg.Username,
			Password: reg.Password,
		}
	}

	c := client.NewClient(reg.URL, auth)
	if reg.Context != "" {
		c = c.WithContext(reg.Context)
	}

	return c, nil
}

