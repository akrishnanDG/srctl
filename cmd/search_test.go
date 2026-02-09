package cmd

import (
	"encoding/json"
	"testing"
)

func TestSearchAvroFieldByName(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}`
	matches := searchAvroFields(schema, "email", "")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for 'email', got %d", len(matches))
	}
	if len(matches) > 0 && matches[0].FieldPath != "email" {
		t.Errorf("expected fieldPath 'email', got '%s'", matches[0].FieldPath)
	}
}

func TestSearchAvroFieldNoMatch(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"}
  ]
}`
	matches := searchAvroFields(schema, "email", "")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches for 'email', got %d", len(matches))
	}
}

func TestSearchAvroFieldNested(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": {
      "type": "record",
      "name": "Customer",
      "fields": [
        {"name": "email", "type": "string"},
        {"name": "name", "type": "string"}
      ]
    }}
  ]
}`
	matches := searchAvroFields(schema, "email", "")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for nested 'email', got %d", len(matches))
	}
	if len(matches) > 0 && matches[0].FieldPath != "customer.email" {
		t.Errorf("expected fieldPath 'customer.email', got '%s'", matches[0].FieldPath)
	}
}

func TestSearchAvroFieldGlob(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "email", "type": "string"},
    {"name": "emailVerified", "type": "boolean"}
  ]
}`
	matches := searchAvroFields(schema, "email*", "")
	if len(matches) != 2 {
		t.Errorf("expected 2 matches for 'email*', got %d", len(matches))
	}
}

func TestSearchAvroFieldWithTypeFilter(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}`
	matches := searchAvroFields(schema, "*", "string")
	if len(matches) != 2 {
		t.Errorf("expected 2 string fields, got %d", len(matches))
	}
}

func TestSearchProtobufFields(t *testing.T) {
	schema := `syntax = "proto3";
message User {
  string id = 1;
  string email = 2;
  int32 age = 3;
}`
	matches := searchProtobufFields(schema, "email", "")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for 'email', got %d", len(matches))
	}
}

func TestSearchProtobufFieldsWithType(t *testing.T) {
	schema := `syntax = "proto3";
message User {
  string id = 1;
  string email = 2;
  int32 age = 3;
}`
	matches := searchProtobufFields(schema, "*", "string")
	if len(matches) != 2 {
		t.Errorf("expected 2 string fields, got %d", len(matches))
	}
}

func TestSearchJSONSchemaFields(t *testing.T) {
	schema := `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "email": {"type": "string"},
    "age": {"type": "integer"}
  }
}`
	matches := searchJSONSchemaFields(schema, "email", "")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for 'email', got %d", len(matches))
	}
}

func TestSearchJSONSchemaFieldsNested(t *testing.T) {
	schema := `{
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "address": {
      "type": "object",
      "properties": {
        "street": {"type": "string"},
        "city": {"type": "string"}
      }
    }
  }
}`
	matches := searchJSONSchemaFields(schema, "street", "")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for nested 'street', got %d", len(matches))
	}
	if len(matches) > 0 && matches[0].FieldPath != "address.street" {
		t.Errorf("expected 'address.street', got '%s'", matches[0].FieldPath)
	}
}

func TestSearchText(t *testing.T) {
	content := `{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "customerId", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}`
	matches := searchSchemaText(content, "customerId")
	if len(matches) == 0 {
		t.Error("expected text match for 'customerId'")
	}
	if len(matches) > 0 && matches[0].MatchType != "text" {
		t.Errorf("expected matchType 'text', got '%s'", matches[0].MatchType)
	}
}

func TestSearchTextCaseInsensitive(t *testing.T) {
	content := `{"name": "CustomerID"}`
	matches := searchSchemaText(content, "customerid")
	if len(matches) == 0 {
		t.Error("expected case-insensitive text match")
	}
}

func TestSearchTextNoMatch(t *testing.T) {
	content := `{"name": "User"}`
	matches := searchSchemaText(content, "nonexistent")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestMatchFieldPattern(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		pattern  string
		expected bool
	}{
		{"exact match", "email", "email", true},
		{"case insensitive", "Email", "email", true},
		{"no match", "name", "email", false},
		{"glob star", "email", "e*", true},
		{"glob question", "email", "emai?", true},
		{"glob no match", "email", "x*", false},
		{"nested field match", "customer.email", "email", true},
		{"nested glob", "customer.email", "e*", true},
		{"full path no match", "customer.name", "email", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchFieldPattern(tt.field, tt.pattern)
			if result != tt.expected {
				t.Errorf("matchFieldPattern(%q, %q) = %v, want %v", tt.field, tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestExtractAllAvroFieldPaths(t *testing.T) {
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
        {"name": "address", "type": {
          "type": "record",
          "name": "Address",
          "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"}
          ]
        }}
      ]
    }},
    {"name": "items", "type": {"type": "array", "items": {
      "type": "record",
      "name": "Item",
      "fields": [
        {"name": "productId", "type": "string"}
      ]
    }}}
  ]
}`
	var parsed interface{}
	json.Unmarshal([]byte(schema), &parsed)

	fields := extractAllAvroFieldPaths(parsed, "")

	expectedFields := []string{"id", "customer", "customer.name", "customer.address", "customer.address.street", "customer.address.city", "items"}
	for _, expected := range expectedFields {
		found := false
		for _, f := range fields {
			if f.Path == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected field path '%s' not found", expected)
		}
	}
}

func TestExtractAllJSONSchemaFieldPaths(t *testing.T) {
	schema := `{
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "address": {
      "type": "object",
      "properties": {
        "street": {"type": "string"},
        "city": {"type": "string"}
      }
    }
  }
}`
	var parsed map[string]interface{}
	json.Unmarshal([]byte(schema), &parsed)

	fields := extractAllJSONSchemaFieldPaths(parsed, "")

	expectedFields := []string{"id", "address", "address.street", "address.city"}
	for _, expected := range expectedFields {
		found := false
		for _, f := range fields {
			if f.Path == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected field path '%s' not found", expected)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"this is a long string", 10, "this is..."},
		{"exact", 5, "exact"},
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.maxLen)
		if result != tt.expected {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
		}
	}
}

func TestCountTotalMatches(t *testing.T) {
	results := []SearchResult{
		{Matches: []SearchMatch{{}, {}}},
		{Matches: []SearchMatch{{}}},
		{Matches: []SearchMatch{}},
	}

	count := countTotalMatches(results)
	if count != 3 {
		t.Errorf("expected 3 total matches, got %d", count)
	}
}
