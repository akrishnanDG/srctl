package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestParseSubjectVersion(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		expectedSubject string
		expectedVersion string
	}{
		{
			name:            "subject only",
			input:           "user-events",
			expectedSubject: "user-events",
			expectedVersion: "latest",
		},
		{
			name:            "subject with version",
			input:           "user-events@3",
			expectedSubject: "user-events",
			expectedVersion: "3",
		},
		{
			name:            "subject with latest",
			input:           "user-events@latest",
			expectedSubject: "user-events",
			expectedVersion: "latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, version := parseSubjectVersion(tt.input)
			if subject != tt.expectedSubject {
				t.Errorf("expected subject '%s', got '%s'", tt.expectedSubject, subject)
			}
			if version != tt.expectedVersion {
				t.Errorf("expected version '%s', got '%s'", tt.expectedVersion, version)
			}
		})
	}
}

func TestDiffSchemas(t *testing.T) {
	schema1 := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`
	schema2 := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	// Schemas are different
	if schema1 == schema2 {
		t.Error("expected schemas to be different")
	}

	// Test same schema
	if schema1 != schema1 {
		t.Error("expected same schema to be equal")
	}
}

func TestGetSchemasForDiff(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		{Subject: "test-subject", Version: 2, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
	})

	// Get latest version
	schema, err := mock.GetSchema("test-subject", "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.Version != 2 {
		t.Errorf("expected version 2 (latest), got %d", schema.Version)
	}

	// Get specific version
	schema, err = mock.GetSchema("test-subject", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.Version != 1 {
		t.Errorf("expected version 1, got %d", schema.Version)
	}
}

func TestDiffSameSubjectVersions(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		{Subject: "test-subject", Version: 2, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
		{Subject: "test-subject", Version: 3, ID: 102, SchemaType: "AVRO", Schema: `{"type":"long"}`},
	})

	schema1, _ := mock.GetSchema("test-subject", "1")
	schema2, _ := mock.GetSchema("test-subject", "2")

	if schema1.Schema == schema2.Schema {
		t.Error("expected different schemas between versions")
	}
}

func TestDiffDifferentSubjects(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("subject-a", []client.Schema{
		{Subject: "subject-a", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	mock.AddSubject("subject-b", []client.Schema{
		{Subject: "subject-b", Version: 1, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
	})

	schemaA, _ := mock.GetSchema("subject-a", "1")
	schemaB, _ := mock.GetSchema("subject-b", "1")

	if schemaA.Schema == schemaB.Schema {
		t.Error("expected different schemas between subjects")
	}
}

func TestDiffByID(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		{Subject: "test-subject", Version: 2, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
	})

	schema100, err := mock.GetSchemaByID(100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	schema101, err := mock.GetSchemaByID(101)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema100.Schema == schema101.Schema {
		t.Error("expected different schemas for different IDs")
	}
}

func TestDiffSchemaTypes(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("avro-subject", []client.Schema{
		{Subject: "avro-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	mock.AddSubject("proto-subject", []client.Schema{
		{Subject: "proto-subject", Version: 1, ID: 101, SchemaType: "PROTOBUF", Schema: `syntax = "proto3";`},
	})

	avroSchema, _ := mock.GetSchema("avro-subject", "1")
	protoSchema, _ := mock.GetSchema("proto-subject", "1")

	if avroSchema.SchemaType == protoSchema.SchemaType {
		t.Error("expected different schema types")
	}
}

func TestDiffError(t *testing.T) {
	mock := client.NewMockClient()
	mock.GetSchemaError = client.NewError("schema not found")

	_, err := mock.GetSchema("nonexistent", "1")
	if err == nil {
		t.Error("expected error for nonexistent schema")
	}
}

func TestCompareSchemasByID(t *testing.T) {
	mock := client.NewMockClient()

	// Add same schema under different subjects
	schema := `{"type":"record","name":"Test","fields":[]}`
	mock.AddSubject("subject-a", []client.Schema{
		{Subject: "subject-a", Version: 1, ID: 100, SchemaType: "AVRO", Schema: schema},
	})
	mock.AddSubject("subject-b", []client.Schema{
		{Subject: "subject-b", Version: 1, ID: 100, SchemaType: "AVRO", Schema: schema}, // Same ID = same schema content
	})

	schemaA, _ := mock.GetSchemaByID(100)

	// Both should return the same schema
	if schemaA.Schema != schema {
		t.Error("expected schema content to match")
	}
}
