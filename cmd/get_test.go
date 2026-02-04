package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestGetSchemaBySubject(t *testing.T) {
	mock := client.NewMockClient()

	expectedSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: expectedSchema},
	})

	schema, err := mock.GetSchema("user-events", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.Subject != "user-events" {
		t.Errorf("expected subject 'user-events', got '%s'", schema.Subject)
	}

	if schema.Version != 1 {
		t.Errorf("expected version 1, got %d", schema.Version)
	}

	if schema.ID != 100 {
		t.Errorf("expected ID 100, got %d", schema.ID)
	}

	if schema.Schema != expectedSchema {
		t.Error("schema content mismatch")
	}
}

func TestGetSchemaLatestVersion(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		{Subject: "user-events", Version: 2, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
		{Subject: "user-events", Version: 3, ID: 102, SchemaType: "AVRO", Schema: `{"type":"long"}`},
	})

	schema, err := mock.GetSchema("user-events", "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.Version != 3 {
		t.Errorf("expected latest version 3, got %d", schema.Version)
	}

	if schema.ID != 102 {
		t.Errorf("expected ID 102, got %d", schema.ID)
	}
}

func TestGetSchemaByID(t *testing.T) {
	mock := client.NewMockClient()

	expectedSchema := `{"type":"record","name":"User","fields":[]}`
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: expectedSchema},
	})

	schema, err := mock.GetSchemaByID(100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.ID != 100 {
		t.Errorf("expected ID 100, got %d", schema.ID)
	}

	if schema.Schema != expectedSchema {
		t.Error("schema content mismatch")
	}
}

func TestGetSchemaSubjectVersionsByID(t *testing.T) {
	mock := client.NewMockClient()

	// Same schema ID can be used by multiple subjects/versions
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})
	mock.AddSubject("user-events-v2", []client.Schema{
		{Subject: "user-events-v2", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	versions, err := mock.GetSchemaSubjectVersionsByID(100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 2 {
		t.Errorf("expected 2 subject/versions, got %d", len(versions))
	}
}

func TestGetSchemaNotFound(t *testing.T) {
	mock := client.NewMockClient()

	_, err := mock.GetSchema("nonexistent", "1")
	if err == nil {
		t.Error("expected error for nonexistent subject")
	}
}

func TestGetSchemaVersionNotFound(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	_, err := mock.GetSchema("user-events", "99")
	if err == nil {
		t.Error("expected error for nonexistent version")
	}
}

func TestGetSchemaByIDNotFound(t *testing.T) {
	mock := client.NewMockClient()

	_, err := mock.GetSchemaByID(99999)
	if err == nil {
		t.Error("expected error for nonexistent schema ID")
	}
}

func TestGetSchemaTypes(t *testing.T) {
	mock := client.NewMockClient()

	// Add schemas of different types
	mock.AddSubject("avro-subject", []client.Schema{
		{Subject: "avro-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})
	mock.AddSubject("proto-subject", []client.Schema{
		{Subject: "proto-subject", Version: 1, ID: 101, SchemaType: "PROTOBUF", Schema: `syntax="proto3";`},
	})
	mock.AddSubject("json-subject", []client.Schema{
		{Subject: "json-subject", Version: 1, ID: 102, SchemaType: "JSON", Schema: `{"type":"object"}`},
	})

	types, err := mock.GetSchemaTypes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(types) != 3 {
		t.Errorf("expected 3 schema types, got %d", len(types))
	}
}

func TestGetContexts(t *testing.T) {
	mock := client.NewMockClient()
	mock.Contexts = []string{".", ".staging", ".production"}

	contexts, err := mock.GetContexts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(contexts) != 3 {
		t.Errorf("expected 3 contexts, got %d", len(contexts))
	}

	// Check default context
	hasDefault := false
	for _, ctx := range contexts {
		if ctx == "." {
			hasDefault = true
			break
		}
	}
	if !hasDefault {
		t.Error("expected default context '.'")
	}
}

func TestGetSchemaWithReferences(t *testing.T) {
	mock := client.NewMockClient()

	schema := client.Schema{
		Subject:    "user-events",
		Version:    1,
		ID:         100,
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[]}`,
		References: []client.SchemaReference{
			{Name: "common.avsc", Subject: "common-types", Version: 1},
		},
	}
	mock.AddSubject("user-events", []client.Schema{schema})

	retrieved, err := mock.GetSchema("user-events", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(retrieved.References) != 1 {
		t.Errorf("expected 1 reference, got %d", len(retrieved.References))
	}

	if retrieved.References[0].Subject != "common-types" {
		t.Errorf("expected reference to 'common-types', got '%s'", retrieved.References[0].Subject)
	}
}
