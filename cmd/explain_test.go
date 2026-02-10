package cmd

import (
	"testing"
)

func TestExplainAvroRecord(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "doc": "A user record",
  "fields": [
    {"name": "id", "type": "string", "doc": "Unique identifier"},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}`
	var exp SchemaExplanation
	explainAvro(schema, &exp)

	if exp.Name != "User" {
		t.Errorf("expected name 'User', got '%s'", exp.Name)
	}
	if exp.Namespace != "com.example" {
		t.Errorf("expected namespace 'com.example', got '%s'", exp.Namespace)
	}
	if exp.Doc != "A user record" {
		t.Errorf("expected doc 'A user record', got '%s'", exp.Doc)
	}
	if len(exp.Fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(exp.Fields))
	}

	// Check first field
	if exp.Fields[0].Name != "id" {
		t.Errorf("expected field 'id', got '%s'", exp.Fields[0].Name)
	}
	if exp.Fields[0].Doc != "Unique identifier" {
		t.Errorf("expected doc 'Unique identifier', got '%s'", exp.Fields[0].Doc)
	}

	// Check nullable field
	if !exp.Fields[3].IsNullable {
		t.Error("expected email to be nullable")
	}
	if !exp.Fields[3].HasDefault {
		t.Error("expected email to have a default")
	}
}

func TestExplainAvroEnum(t *testing.T) {
	schema := `{
  "type": "enum",
  "name": "Status",
  "namespace": "com.example",
  "symbols": ["ACTIVE", "INACTIVE", "SUSPENDED"]
}`
	var exp SchemaExplanation
	explainAvro(schema, &exp)

	if exp.Name != "Status" {
		t.Errorf("expected name 'Status', got '%s'", exp.Name)
	}
	if exp.RecordType != "enum" {
		t.Errorf("expected recordType 'enum', got '%s'", exp.RecordType)
	}
	if len(exp.Symbols) != 3 {
		t.Errorf("expected 3 symbols, got %d", len(exp.Symbols))
	}
}

func TestExplainAvroWithInlineRecord(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "address", "type": {
      "type": "record",
      "name": "Address",
      "fields": [
        {"name": "street", "type": "string"}
      ]
    }}
  ]
}`
	var exp SchemaExplanation
	explainAvro(schema, &exp)

	if len(exp.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(exp.Fields))
	}
	if exp.Fields[1].Type != "record(Address)" {
		t.Errorf("expected type 'record(Address)', got '%s'", exp.Fields[1].Type)
	}
}

func TestExplainAvroWithReference(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": "com.example.types.Customer"}
  ]
}`
	var exp SchemaExplanation
	explainAvro(schema, &exp)

	if !exp.Fields[1].IsReference {
		t.Error("expected customer field to be marked as reference")
	}
}

func TestExplainProtobuf(t *testing.T) {
	schema := `syntax = "proto3";
package com.example;

message User {
  string id = 1;
  string name = 2;
  int32 age = 3;
}`
	var exp SchemaExplanation
	explainProtobuf(schema, &exp)

	if exp.Name != "User" {
		t.Errorf("expected name 'User', got '%s'", exp.Name)
	}
	if exp.Namespace != "com.example" {
		t.Errorf("expected namespace 'com.example', got '%s'", exp.Namespace)
	}
	if len(exp.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(exp.Fields))
	}
}

func TestExplainJSONSchema(t *testing.T) {
	schema := `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "user.json",
  "description": "A user object",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "email": {"type": "string", "format": "email"}
  },
  "required": ["id", "name"]
}`
	var exp SchemaExplanation
	explainJSONSchema(schema, &exp)

	if exp.Name != "user.json" {
		t.Errorf("expected name 'user.json', got '%s'", exp.Name)
	}
	if exp.Doc != "A user object" {
		t.Errorf("expected doc 'A user object', got '%s'", exp.Doc)
	}
	if len(exp.Fields) != 4 {
		t.Errorf("expected 4 fields, got %d", len(exp.Fields))
	}

	// Check that non-required fields are marked nullable
	for _, f := range exp.Fields {
		if f.Name == "age" && !f.IsNullable {
			t.Error("expected 'age' (not required) to be marked nullable/optional")
		}
		if f.Name == "id" && f.IsNullable {
			t.Error("expected 'id' (required) to not be nullable")
		}
	}
}

func TestExplainNullableFields(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Test",
  "fields": [
    {"name": "required_field", "type": "string"},
    {"name": "nullable_field", "type": ["null", "string"], "default": null},
    {"name": "union_field", "type": ["null", "int", "string"]}
  ]
}`
	var exp SchemaExplanation
	explainAvro(schema, &exp)

	if exp.Fields[0].IsNullable {
		t.Error("required_field should not be nullable")
	}
	if !exp.Fields[1].IsNullable {
		t.Error("nullable_field should be nullable")
	}
	if exp.Fields[1].Type != "nullable string" {
		t.Errorf("expected 'nullable string', got '%s'", exp.Fields[1].Type)
	}
	if !exp.Fields[2].IsNullable {
		t.Error("union_field should be nullable")
	}
}

func TestDescribeAvroType(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string", "string", "string"},
		{"int", "int", "int"},
		{"nullable", []interface{}{"null", "string"}, "nullable string"},
		{"array", map[string]interface{}{"type": "array", "items": "string"}, "array<string>"},
		{"map", map[string]interface{}{"type": "map", "values": "int"}, "map<int>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := describeAvroType(tt.input)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}
