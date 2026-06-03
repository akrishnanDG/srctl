package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// KafkaSASLConfig holds SASL authentication for Kafka
type KafkaSASLConfig struct {
	Mechanism string `mapstructure:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// KafkaTLSConfig holds TLS settings for Kafka
type KafkaTLSConfig struct {
	Enabled    bool `mapstructure:"enabled"`
	SkipVerify bool `mapstructure:"skip_verify"`
}

// KafkaConfig holds Kafka connection settings for a registry
type KafkaConfig struct {
	Brokers []string        `mapstructure:"brokers"`
	SASL    KafkaSASLConfig `mapstructure:"sasl"`
	TLS     KafkaTLSConfig  `mapstructure:"tls"`
}

// Registry represents a configured schema registry
type Registry struct {
	Name     string      `mapstructure:"name"`
	URL      string      `mapstructure:"url"`
	Username string      `mapstructure:"username"`
	Password string      `mapstructure:"password"`
	Context  string      `mapstructure:"context"`
	Default  bool        `mapstructure:"default"`
	Kafka    KafkaConfig `mapstructure:"kafka"`
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
		reg := Registry{
			Name:    "default",
			URL:     url,
			Default: true,
		}
		if authInfo := os.Getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"); authInfo != "" {
			if idx := strings.Index(authInfo, ":"); idx >= 0 {
				reg.Username = authInfo[:idx]
				reg.Password = authInfo[idx+1:]
			} else {
				reg.Username = authInfo
			}
		}
		viper.SetDefault("registries", []Registry{reg})
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
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, "srctl.yaml")

	viper.Set("registries", AppConfig.Registries)
	viper.Set("default_output", AppConfig.DefaultOutput)
	viper.Set("default_context", AppConfig.DefaultContext)

	// Pre-create the target with 0600 and truncate it so the file never exists
	// with world-readable permissions, even briefly. viper.WriteConfigAs writes
	// into this existing file without changing its mode (no chmod TOCTOU window).
	f, err := os.OpenFile(configPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}

	if err := viper.WriteConfigAs(configPath); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	// Defensively re-assert 0600 in case the writer altered the mode; this is now
	// a no-op narrowing (file already started restricted) rather than a window.
	if err := os.Chmod(configPath, 0600); err != nil {
		return fmt.Errorf("failed to set config file permissions: %w", err)
	}

	return nil
}

// InitConfig creates an initial configuration file
func InitConfig(registry Registry) error {
	AppConfig.Registries = append(AppConfig.Registries, registry)
	AppConfig.DefaultOutput = "table"
	return SaveConfig()
}
