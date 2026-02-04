package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestContractCommand(t *testing.T) {
	// Test that the contract command exists
	if contractCmd == nil {
		t.Error("expected contractCmd to be defined")
	}

	if contractCmd.Use != "contract" {
		t.Errorf("expected Use to be 'contract', got '%s'", contractCmd.Use)
	}
}

func TestContractSubcommands(t *testing.T) {
	// Test that subcommands exist
	subcommands := []string{"get", "set", "delete", "validate"}

	for _, subcmd := range subcommands {
		found := false
		for _, cmd := range contractCmd.Commands() {
			if cmd.Name() == subcmd {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected subcommand '%s' to exist", subcmd)
		}
	}
}

func TestContractGetSubject(t *testing.T) {
	mock := client.NewMockClient()

	// Set up subject with config
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})
	mock.SubjectConfigs["user-events"] = &client.Config{CompatibilityLevel: "FULL"}

	config, err := mock.GetSubjectConfig("user-events", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "FULL" {
		t.Errorf("expected FULL, got %s", config.CompatibilityLevel)
	}
}

func TestContractAliases(t *testing.T) {
	// Test that contract command has aliases
	aliases := contractCmd.Aliases

	expectedAliases := []string{"dc", "data-contract"}
	for _, expected := range expectedAliases {
		found := false
		for _, alias := range aliases {
			if alias == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected alias '%s' to exist", expected)
		}
	}
}

func TestContractCommandExists(t *testing.T) {
	// Test that subcommands exist with proper structure
	if contractGetCmd == nil {
		t.Error("expected contractGetCmd to be defined")
	}

	if contractSetCmd == nil {
		t.Error("expected contractSetCmd to be defined")
	}

	if contractDeleteCmd == nil {
		t.Error("expected contractDeleteCmd to be defined")
	}

	if contractValidateCmd == nil {
		t.Error("expected contractValidateCmd to be defined")
	}
}

func TestDataContractRules(t *testing.T) {
	// Test basic rule structure concepts
	rules := []map[string]interface{}{
		{
			"name": "rule1",
			"kind": "TRANSFORM",
			"type": "ENCRYPT",
		},
		{
			"name": "rule2",
			"kind": "CONDITION",
			"type": "PATTERN",
		},
	}

	if len(rules) != 2 {
		t.Errorf("expected 2 rules, got %d", len(rules))
	}

	// Check rule structure
	rule1 := rules[0]
	if rule1["kind"] != "TRANSFORM" {
		t.Errorf("expected kind TRANSFORM, got %v", rule1["kind"])
	}
}

func TestContractGroupID(t *testing.T) {
	// Test that contract command has correct group ID
	if contractCmd.GroupID != groupContract {
		t.Errorf("expected GroupID '%s', got '%s'", groupContract, contractCmd.GroupID)
	}
}
