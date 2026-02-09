package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateAvroSyntaxValid(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}`
	result := validateSchemaSyntax(schema, "AVRO", "user.avsc")
	if !result.Valid {
		t.Errorf("expected valid schema, got invalid with issues: %v", result.Issues)
	}
}

func TestValidateAvroSyntaxInvalidJSON(t *testing.T) {
	schema := `{not valid json`
	result := validateSchemaSyntax(schema, "AVRO", "bad.avsc")
	if result.Valid {
		t.Error("expected invalid result for bad JSON")
	}
	if len(result.Issues) == 0 {
		t.Error("expected issues for bad JSON")
	}
	if result.Issues[0].Severity != "ERROR" {
		t.Errorf("expected ERROR severity, got %s", result.Issues[0].Severity)
	}
}

func TestValidateAvroSyntaxMissingType(t *testing.T) {
	schema := `{"name": "Test"}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")
	if result.Valid {
		t.Error("expected invalid result for missing type")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Message == "Missing required field: 'type'" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'Missing required field: type' issue")
	}
}

func TestValidateAvroSyntaxMissingName(t *testing.T) {
	schema := `{"type": "record", "fields": [{"name": "id", "type": "string"}]}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")
	if result.Valid {
		t.Error("expected invalid result for missing name")
	}
}

func TestValidateAvroSyntaxMissingFields(t *testing.T) {
	schema := `{"type": "record", "name": "Test"}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")
	if result.Valid {
		t.Error("expected invalid result for missing fields")
	}
}

func TestValidateAvroSyntaxDuplicateFieldNames(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Test",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "id", "type": "int"}
  ]
}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")
	if result.Valid {
		t.Error("expected invalid result for duplicate field names")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Severity == "ERROR" && issue.Field == "id" {
			found = true
		}
	}
	if !found {
		t.Error("expected duplicate field name error")
	}
}

func TestValidateAvroSyntaxMissingNamespace(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Test",
  "fields": [{"name": "id", "type": "string"}]
}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")
	if !result.Valid {
		t.Error("missing namespace should be a warning, not an error")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Severity == "WARNING" && issue.Message == "Record missing 'namespace'" {
			found = true
		}
	}
	if !found {
		t.Error("expected namespace warning")
	}
}

func TestValidateAvroEnum(t *testing.T) {
	schema := `{
  "type": "enum",
  "name": "Status",
  "namespace": "com.example",
  "symbols": ["ACTIVE", "INACTIVE"]
}`
	result := validateSchemaSyntax(schema, "AVRO", "status.avsc")
	if !result.Valid {
		t.Errorf("expected valid enum, got issues: %v", result.Issues)
	}
}

func TestValidateAvroEnumDuplicateSymbols(t *testing.T) {
	schema := `{
  "type": "enum",
  "name": "Status",
  "symbols": ["ACTIVE", "ACTIVE"]
}`
	result := validateSchemaSyntax(schema, "AVRO", "status.avsc")
	if result.Valid {
		t.Error("expected invalid for duplicate symbols")
	}
}

func TestValidateAvroEnumMissingSymbols(t *testing.T) {
	schema := `{"type": "enum", "name": "Status"}`
	result := validateSchemaSyntax(schema, "AVRO", "status.avsc")
	if result.Valid {
		t.Error("expected invalid for missing symbols")
	}
}

func TestValidateProtobufSyntaxValid(t *testing.T) {
	schema := `syntax = "proto3";
package com.example;

message User {
  string id = 1;
  string name = 2;
  int32 age = 3;
}`
	result := validateSchemaSyntax(schema, "PROTOBUF", "user.proto")
	if !result.Valid {
		t.Errorf("expected valid protobuf, got issues: %v", result.Issues)
	}
}

func TestValidateProtobufSyntaxNoMessage(t *testing.T) {
	schema := `syntax = "proto3";
package com.example;
`
	result := validateSchemaSyntax(schema, "PROTOBUF", "empty.proto")
	if result.Valid {
		t.Error("expected invalid for no messages")
	}
}

func TestValidateProtobufSyntaxUnmatchedBraces(t *testing.T) {
	schema := `syntax = "proto3";
message User {
  string id = 1;
`
	result := validateSchemaSyntax(schema, "PROTOBUF", "bad.proto")
	if result.Valid {
		t.Error("expected invalid for unmatched braces")
	}
}

func TestValidateProtobufDuplicateFieldNumbers(t *testing.T) {
	schema := `syntax = "proto3";
message User {
  string id = 1;
  string name = 1;
}`
	result := validateSchemaSyntax(schema, "PROTOBUF", "dup.proto")
	if result.Valid {
		t.Error("expected invalid for duplicate field numbers")
	}
}

func TestValidateJSONSchemaSyntaxValid(t *testing.T) {
	schema := `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"}
  }
}`
	result := validateSchemaSyntax(schema, "JSON", "user.json")
	if !result.Valid {
		t.Errorf("expected valid JSON schema, got issues: %v", result.Issues)
	}
}

func TestValidateJSONSchemaSyntaxMissingSchema(t *testing.T) {
	schema := `{
  "type": "object",
  "properties": {
    "id": {"type": "string"}
  }
}`
	result := validateSchemaSyntax(schema, "JSON", "user.json")
	if !result.Valid {
		t.Error("missing $schema should be a warning")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Severity == "WARNING" && issue.Message == "Missing '$schema' declaration" {
			found = true
		}
	}
	if !found {
		t.Error("expected missing $schema warning")
	}
}

func TestValidateJSONSchemaSyntaxInvalidType(t *testing.T) {
	schema := `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "invalid_type"
}`
	result := validateSchemaSyntax(schema, "JSON", "bad.json")
	if result.Valid {
		t.Error("expected invalid for bad type")
	}
}

func TestValidateCompatibilityBackwardFieldRemoved(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "BACKWARD")
	if len(issues) == 0 {
		t.Error("expected compatibility issues for removed field")
	}
	found := false
	for _, issue := range issues {
		if issue.Severity == "ERROR" && issue.Field == "name" {
			found = true
		}
	}
	if !found {
		t.Error("expected error about removed 'name' field")
	}
}

func TestValidateCompatibilityBackwardFieldAdded(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "BACKWARD")
	for _, issue := range issues {
		if issue.Severity == "ERROR" {
			t.Errorf("unexpected error: %s", issue.Message)
		}
	}
}

func TestValidateCompatibilityBackwardFieldAddedNoDefault(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "BACKWARD")
	found := false
	for _, issue := range issues {
		if issue.Severity == "WARNING" && issue.Field == "email" {
			found = true
		}
	}
	if !found {
		t.Error("expected warning about new field without default")
	}
}

func TestValidateCompatibilityBackwardTypeChanged(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "age", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "BACKWARD")
	found := false
	for _, issue := range issues {
		if issue.Severity == "ERROR" && issue.Field == "age" {
			found = true
		}
	}
	if !found {
		t.Error("expected error about type change")
	}
}

func TestValidateCompatibilityForward(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "FORWARD")
	found := false
	for _, issue := range issues {
		if issue.Severity == "ERROR" && issue.Field == "email" {
			found = true
		}
	}
	if !found {
		t.Error("expected FORWARD compatibility error for new field")
	}
}

func TestValidateCompatibilityFull(t *testing.T) {
	oldSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"}
  ]
}`
	newSchema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(newSchema, oldSchema, "FULL")
	if len(issues) < 2 {
		t.Errorf("expected at least 2 issues for FULL compat, got %d", len(issues))
	}
}

func TestValidateCompatibilityNoIssues(t *testing.T) {
	schema := `{
  "type": "record", "name": "Test", "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"}
  ]
}`
	issues := checkAvroCompatibility(schema, schema, "BACKWARD")
	for _, issue := range issues {
		if issue.Severity == "ERROR" {
			t.Errorf("unexpected error for identical schemas: %s", issue.Message)
		}
	}
}

func TestValidateCompatibilityJSONSchema(t *testing.T) {
	oldSchema := `{
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"}
  }
}`
	newSchema := `{
  "type": "object",
  "properties": {
    "id": {"type": "string"}
  }
}`
	issues := checkJSONSchemaCompatibility(newSchema, oldSchema, "BACKWARD")
	found := false
	for _, issue := range issues {
		if issue.Severity == "ERROR" && issue.Field == "name" {
			found = true
		}
	}
	if !found {
		t.Error("expected error about removed 'name' property")
	}
}

func TestValidateDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	validSchema := `{"type": "record", "name": "Valid", "namespace": "com.example", "fields": [{"name": "id", "type": "string"}]}`
	os.WriteFile(filepath.Join(tmpDir, "valid.avsc"), []byte(validSchema), 0644)

	invalidSchema := `{"type": "record"}`
	os.WriteFile(filepath.Join(tmpDir, "invalid.avsc"), []byte(invalidSchema), 0644)

	validProto := `syntax = "proto3";
message Test {
  string id = 1;
}`
	os.WriteFile(filepath.Join(tmpDir, "valid.proto"), []byte(validProto), 0644)

	files := []string{
		filepath.Join(tmpDir, "valid.avsc"),
		filepath.Join(tmpDir, "invalid.avsc"),
		filepath.Join(tmpDir, "valid.proto"),
	}

	var validCount, invalidCount int
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("failed to read %s: %v", f, err)
		}
		schemaType := detectSchemaType(string(content), f)
		result := validateSchemaSyntax(string(content), schemaType, f)
		if result.Valid {
			validCount++
		} else {
			invalidCount++
		}
	}

	if validCount != 2 {
		t.Errorf("expected 2 valid schemas, got %d", validCount)
	}
	if invalidCount != 1 {
		t.Errorf("expected 1 invalid schema, got %d", invalidCount)
	}
}

func TestValidateActionableFeedback(t *testing.T) {
	schema := `{"type": "record"}`
	result := validateSchemaSyntax(schema, "AVRO", "test.avsc")

	for _, issue := range result.Issues {
		if issue.Severity == "ERROR" && issue.Fix == "" {
			t.Errorf("ERROR issue missing fix suggestion: %s", issue.Message)
		}
	}
}

func TestExtractAvroFieldsDeep(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": {
      "type": "record",
      "name": "Customer",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"}
      ]
    }}
  ]
}`
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		t.Fatal(err)
	}

	fields := extractAvroFieldsDeep(parsed, "")

	if _, ok := fields["id"]; !ok {
		t.Error("expected 'id' field")
	}
	if _, ok := fields["customer"]; !ok {
		t.Error("expected 'customer' field")
	}
	if _, ok := fields["customer.name"]; !ok {
		t.Error("expected 'customer.name' field")
	}
	if _, ok := fields["customer.email"]; !ok {
		t.Error("expected 'customer.email' field")
	}
}
