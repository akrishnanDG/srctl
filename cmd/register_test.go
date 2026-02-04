package cmd

import (
	"os"
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestRegisterSchema(t *testing.T) {
	mock := client.NewMockClient()

	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}

	// Verify schema was registered
	subjects, _ := mock.GetSubjects(false)
	found := false
	for _, s := range subjects {
		if s == "user-events" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected subject to be registered")
	}
}

func TestRegisterSchemaNewVersion(t *testing.T) {
	mock := client.NewMockClient()

	// Add existing schema
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	// Register new version
	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"int"}`,
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 100 {
		t.Error("expected new schema ID for new version")
	}

	// Verify new version
	versions, _ := mock.GetVersions("user-events", false)
	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}
}

func TestRegisterSchemaWithReferences(t *testing.T) {
	mock := client.NewMockClient()

	// Add referenced schema
	mock.AddSubject("common-types", []client.Schema{
		{Subject: "common-types", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"record","name":"Common","fields":[]}`},
	})

	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[]}`,
		References: []client.SchemaReference{
			{Name: "common.avsc", Subject: "common-types", Version: 1},
		},
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}
}

func TestRegisterSchemaError(t *testing.T) {
	mock := client.NewMockClient()
	mock.RegisterError = client.NewError("incompatible schema")

	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"string"}`,
	}

	_, err := mock.RegisterSchema("user-events", schema)
	if err == nil {
		t.Error("expected error for incompatible schema")
	}
}

func TestCheckCompatibility(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"string"}`,
	}

	compatible, err := mock.CheckCompatibility("user-events", schema, "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !compatible {
		t.Error("expected schema to be compatible")
	}
}

func TestRegisterProtobufSchema(t *testing.T) {
	mock := client.NewMockClient()

	schema := &client.Schema{
		SchemaType: "PROTOBUF",
		Schema: `
syntax = "proto3";
package example;

message User {
  int32 id = 1;
  string name = 2;
}
`,
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}

	// Verify schema type
	registered, _ := mock.GetSchema("user-events", "1")
	if registered.SchemaType != "PROTOBUF" {
		t.Errorf("expected PROTOBUF, got %s", registered.SchemaType)
	}
}

func TestRegisterJSONSchema(t *testing.T) {
	mock := client.NewMockClient()

	schema := &client.Schema{
		SchemaType: "JSON",
		Schema: `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"}
  }
}`,
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}

	// Verify schema type
	registered, _ := mock.GetSchema("user-events", "1")
	if registered.SchemaType != "JSON" {
		t.Errorf("expected JSON, got %s", registered.SchemaType)
	}
}

func TestReadSchemaFromFile(t *testing.T) {
	schemaContent := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`

	path, cleanup := createTempFile(schemaContent)
	defer cleanup()

	// Read file content
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(content) != schemaContent {
		t.Errorf("schema content mismatch")
	}
}
