package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Registry represents a configured schema registry
type Registry struct {
	Name     string `mapstructure:"name"`
	URL      string `mapstructure:"url"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Context  string `mapstructure:"context"`
	Default  bool   `mapstructure:"default"`
}

// Config represents the application configuration
type Config struct {
	Registries     []Registry `mapstructure:"registries"`
	DefaultOutput  string     `mapstructure:"default_output"`
	DefaultContext string     `mapstructure:"default_context"`
}

// Global configuration instance
var AppConfig Config

// LoadConfig loads configuration from file and environment
func LoadConfig() error {
	// Set config file name and type
	viper.SetConfigName("srctl")
	viper.SetConfigType("yaml")

	// Search paths for config file
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.srctl")
	viper.AddConfigPath("/etc/srctl")

	// Set defaults
	viper.SetDefault("default_output", "table")
	viper.SetDefault("default_context", ".")

	// Environment variable support
	viper.SetEnvPrefix("SRCTL")
	viper.AutomaticEnv()

	// Support common SR environment variables
	if url := os.Getenv("SCHEMA_REGISTRY_URL"); url != "" {
		viper.SetDefault("registries", []Registry{
			{
				Name:     "default",
				URL:      url,
				Username: os.Getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"),
				Default:  true,
			},
		})
	}

	// Read config file if exists (silently ignore if not found)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found is normal, use defaults/env
			return nil
		}
		return fmt.Errorf("error reading config file: %w", err)
	}

	// Unmarshal to struct
	if err := viper.Unmarshal(&AppConfig); err != nil {
		return fmt.Errorf("unable to decode config: %w", err)
	}

	return nil
}

// GetDefaultRegistry returns the default registry configuration
func GetDefaultRegistry() *Registry {
	for i := range AppConfig.Registries {
		if AppConfig.Registries[i].Default {
			return &AppConfig.Registries[i]
		}
	}
	// Return first registry if no default is set
	if len(AppConfig.Registries) > 0 {
		return &AppConfig.Registries[0]
	}
	return nil
}

// GetRegistry returns a registry by name
func GetRegistry(name string) *Registry {
	for i := range AppConfig.Registries {
		if AppConfig.Registries[i].Name == name {
			return &AppConfig.Registries[i]
		}
	}
	return nil
}

// SaveConfig saves the current configuration to file
func SaveConfig() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}

	configDir := filepath.Join(homeDir, ".srctl")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, "srctl.yaml")

	viper.Set("registries", AppConfig.Registries)
	viper.Set("default_output", AppConfig.DefaultOutput)
	viper.Set("default_context", AppConfig.DefaultContext)

	if err := viper.WriteConfigAs(configPath); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// InitConfig creates an initial configuration file
func InitConfig(registry Registry) error {
	AppConfig.Registries = append(AppConfig.Registries, registry)
	AppConfig.DefaultOutput = "table"
	return SaveConfig()
}
