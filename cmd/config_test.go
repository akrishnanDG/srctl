package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestGetConfigWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.GlobalConfig = &client.Config{CompatibilityLevel: "BACKWARD_TRANSITIVE"}

	config, err := mock.GetConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "BACKWARD_TRANSITIVE" {
		t.Errorf("expected BACKWARD_TRANSITIVE, got %s", config.CompatibilityLevel)
	}
}

func TestSetConfigWithMock(t *testing.T) {
	mock := client.NewMockClient()

	err := mock.SetConfig("FULL_TRANSITIVE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the config was set
	config, _ := mock.GetConfig()
	if config.CompatibilityLevel != "FULL_TRANSITIVE" {
		t.Errorf("expected FULL_TRANSITIVE, got %s", config.CompatibilityLevel)
	}
}

func TestGetSubjectConfigWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.SubjectConfigs["test-subject"] = &client.Config{CompatibilityLevel: "FULL"}

	config, err := mock.GetSubjectConfig("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "FULL" {
		t.Errorf("expected FULL, got %s", config.CompatibilityLevel)
	}
}

func TestGetSubjectConfigDefaultToGlobal(t *testing.T) {
	mock := client.NewMockClient()
	mock.GlobalConfig = &client.Config{CompatibilityLevel: "BACKWARD"}

	// Subject has no specific config
	config, err := mock.GetSubjectConfig("test-subject", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "BACKWARD" {
		t.Errorf("expected BACKWARD (from global), got %s", config.CompatibilityLevel)
	}
}

func TestSetSubjectConfigWithMock(t *testing.T) {
	mock := client.NewMockClient()

	err := mock.SetSubjectConfig("test-subject", "NONE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the config was set
	config, _ := mock.GetSubjectConfig("test-subject", false)
	if config.CompatibilityLevel != "NONE" {
		t.Errorf("expected NONE, got %s", config.CompatibilityLevel)
	}
}

func TestGetModeWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.GlobalMode = &client.Mode{Mode: "READONLY"}

	mode, err := mock.GetMode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mode.Mode != "READONLY" {
		t.Errorf("expected READONLY, got %s", mode.Mode)
	}
}

func TestSetModeWithMock(t *testing.T) {
	mock := client.NewMockClient()

	err := mock.SetMode("IMPORT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the mode was set
	mode, _ := mock.GetMode()
	if mode.Mode != "IMPORT" {
		t.Errorf("expected IMPORT, got %s", mode.Mode)
	}
}

func TestGetSubjectModeWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.SubjectModes["test-subject"] = &client.Mode{Mode: "IMPORT"}

	mode, err := mock.GetSubjectMode("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mode.Mode != "IMPORT" {
		t.Errorf("expected IMPORT, got %s", mode.Mode)
	}
}

func TestSetSubjectModeWithMock(t *testing.T) {
	mock := client.NewMockClient()

	err := mock.SetSubjectMode("test-subject", "READONLY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the mode was set
	mode, _ := mock.GetSubjectMode("test-subject", false)
	if mode.Mode != "READONLY" {
		t.Errorf("expected READONLY, got %s", mode.Mode)
	}
}

func TestCompatibilityLevels(t *testing.T) {
	validLevels := []string{
		"NONE",
		"BACKWARD",
		"BACKWARD_TRANSITIVE",
		"FORWARD",
		"FORWARD_TRANSITIVE",
		"FULL",
		"FULL_TRANSITIVE",
	}

	mock := client.NewMockClient()

	for _, level := range validLevels {
		t.Run(level, func(t *testing.T) {
			err := mock.SetConfig(level)
			if err != nil {
				t.Errorf("failed to set compatibility level %s: %v", level, err)
			}

			config, _ := mock.GetConfig()
			if config.CompatibilityLevel != level {
				t.Errorf("expected %s, got %s", level, config.CompatibilityLevel)
			}
		})
	}
}

func TestModes(t *testing.T) {
	validModes := []string{
		"READWRITE",
		"READONLY",
		"IMPORT",
	}

	mock := client.NewMockClient()

	for _, mode := range validModes {
		t.Run(mode, func(t *testing.T) {
			err := mock.SetMode(mode)
			if err != nil {
				t.Errorf("failed to set mode %s: %v", mode, err)
			}

			m, _ := mock.GetMode()
			if m.Mode != mode {
				t.Errorf("expected %s, got %s", mode, m.Mode)
			}
		})
	}
}

func TestConfigError(t *testing.T) {
	mock := client.NewMockClient()
	mock.ConfigError = client.NewError("config error")

	_, err := mock.GetConfig()
	if err == nil {
		t.Error("expected error")
	}

	err = mock.SetConfig("FULL")
	if err == nil {
		t.Error("expected error")
	}
}

func TestModeError(t *testing.T) {
	mock := client.NewMockClient()
	mock.ModeError = client.NewError("mode error")

	_, err := mock.GetMode()
	if err == nil {
		t.Error("expected error")
	}

	err = mock.SetMode("READONLY")
	if err == nil {
		t.Error("expected error")
	}
}
