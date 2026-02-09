package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSplitAvroSchema(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.events",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customer", "type": {
      "type": "record",
      "name": "Customer",
      "namespace": "com.example.types",
      "fields": [
        {"name": "customerId", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "address", "type": {
          "type": "record",
          "name": "Address",
          "namespace": "com.example.types",
          "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "zip", "type": "string"}
          ]
        }}
      ]
    }},
    {"name": "items", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "LineItem",
        "namespace": "com.example.types",
        "fields": [
          {"name": "productId", "type": "string"},
          {"name": "quantity", "type": "int"},
          {"name": "price", "type": "double"}
        ]
      }
    }}
  ]
}`

	result, err := splitAvroSchema(schema, 0, "")
	if err != nil {
		t.Fatalf("splitAvroSchema failed: %v", err)
	}

	// Should have 4 types: Address, Customer, LineItem, Order
	if len(result.Types) != 4 {
		t.Errorf("expected 4 types, got %d", len(result.Types))
		for _, typ := range result.Types {
			t.Logf("  type: %s (root=%v)", typ.Name, typ.IsRoot)
		}
	}

	// Root should be Order
	var rootFound bool
	for _, typ := range result.Types {
		if typ.IsRoot {
			rootFound = true
			if typ.Name != "com.example.events.Order" {
				t.Errorf("expected root name 'com.example.events.Order', got '%s'", typ.Name)
			}
		}
	}
	if !rootFound {
		t.Error("no root type found")
	}

	// Registration order should have dependencies before dependents
	orderMap := make(map[string]int)
	for i, name := range result.RegistrationOrder {
		orderMap[name] = i
	}

	// Address should come before Customer
	if orderMap["com.example.types.Address"] >= orderMap["com.example.types.Customer"] {
		t.Error("Address should be registered before Customer")
	}

	// Customer should come before Order
	if orderMap["com.example.types.Customer"] >= orderMap["com.example.events.Order"] {
		t.Error("Customer should be registered before Order")
	}

	// LineItem should come before Order
	if orderMap["com.example.types.LineItem"] >= orderMap["com.example.events.Order"] {
		t.Error("LineItem should be registered before Order")
	}

	// Order should be last
	if orderMap["com.example.events.Order"] != len(result.RegistrationOrder)-1 {
		t.Error("Order (root) should be registered last")
	}
}

func TestSplitAvroSchemaNoSplitNeeded(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Simple",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "value", "type": "int"}
  ]
}`

	result, err := splitAvroSchema(schema, 0, "")
	if err != nil {
		t.Fatalf("splitAvroSchema failed: %v", err)
	}

	if len(result.Types) != 1 {
		t.Errorf("expected 1 type (no split needed), got %d", len(result.Types))
	}

	if !result.Types[0].IsRoot {
		t.Error("single type should be root")
	}
}

func TestSplitAvroSchemaWithMinSize(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "address", "type": {
      "type": "record",
      "name": "Address",
      "namespace": "com.example",
      "fields": [
        {"name": "street", "type": "string"}
      ]
    }}
  ]
}`

	// With very high min size, small types should not be extracted
	result, err := splitAvroSchema(schema, 100000, "")
	if err != nil {
		t.Fatalf("splitAvroSchema failed: %v", err)
	}

	// Should only have root since Address is too small
	if len(result.Types) != 1 {
		t.Errorf("expected 1 type with high min-size, got %d", len(result.Types))
	}
}

func TestSplitAvroSchemaWithUnion(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Event",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "address", "type": ["null", {
      "type": "record",
      "name": "Address",
      "namespace": "com.example",
      "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"}
      ]
    }]}
  ]
}`

	result, err := splitAvroSchema(schema, 0, "")
	if err != nil {
		t.Fatalf("splitAvroSchema failed: %v", err)
	}

	if len(result.Types) != 2 {
		t.Errorf("expected 2 types, got %d", len(result.Types))
	}
}

func TestSplitProtobufSchema(t *testing.T) {
	schema := `syntax = "proto3";
package com.example.events;

message Order {
  string order_id = 1;
  Customer customer = 2;
  repeated LineItem items = 3;
}

message Customer {
  string customer_id = 1;
  string name = 2;
}

message LineItem {
  string product_id = 1;
  int32 quantity = 2;
  double price = 3;
}`

	result, err := splitProtobufSchema(schema, "order.proto", 0, "")
	if err != nil {
		t.Fatalf("splitProtobufSchema failed: %v", err)
	}

	if len(result.Types) != 3 {
		t.Errorf("expected 3 types, got %d", len(result.Types))
		for _, typ := range result.Types {
			t.Logf("  type: %s (root=%v)", typ.Name, typ.IsRoot)
		}
	}

	// Order should be root (it references Customer and LineItem but isn't referenced)
	var rootFound bool
	for _, typ := range result.Types {
		if typ.IsRoot {
			rootFound = true
			if typ.Name != "Order" {
				t.Errorf("expected root name 'Order', got '%s'", typ.Name)
			}
		}
	}
	if !rootFound {
		t.Error("no root type found")
	}
}

func TestSplitJSONSchema(t *testing.T) {
	schema := `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "order.json",
  "type": "object",
  "properties": {
    "orderId": {"type": "string"},
    "customer": {
      "type": "object",
      "properties": {
        "customerId": {"type": "string"},
        "name": {"type": "string"},
        "address": {
          "type": "object",
          "properties": {
            "street": {"type": "string"},
            "city": {"type": "string"},
            "zip": {"type": "string"}
          }
        }
      }
    },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "productId": {"type": "string"},
          "quantity": {"type": "integer"},
          "price": {"type": "number"}
        }
      }
    }
  }
}`

	result, err := splitJSONSchema(schema, 0, "")
	if err != nil {
		t.Fatalf("splitJSONSchema failed: %v", err)
	}

	if len(result.Types) < 3 {
		t.Errorf("expected at least 3 types, got %d", len(result.Types))
		for _, typ := range result.Types {
			t.Logf("  type: %s (root=%v)", typ.Name, typ.IsRoot)
		}
	}

	// Root should be order.json
	var rootFound bool
	for _, typ := range result.Types {
		if typ.IsRoot {
			rootFound = true
		}
	}
	if !rootFound {
		t.Error("no root type found")
	}
}

func TestTopologicalSort(t *testing.T) {
	deps := map[string][]string{
		"A": {"B", "C"},
		"B": {"D"},
		"C": {},
		"D": {},
	}

	result := topologicalSort(deps, "A")

	// D should come before B
	orderMap := make(map[string]int)
	for i, name := range result {
		orderMap[name] = i
	}

	if orderMap["D"] >= orderMap["B"] {
		t.Error("D should come before B")
	}
	if orderMap["B"] >= orderMap["A"] {
		t.Error("B should come before A")
	}
	if orderMap["C"] >= orderMap["A"] {
		t.Error("C should come before A")
	}
	// A should be last (root)
	if orderMap["A"] != len(result)-1 {
		t.Error("A (root) should be last")
	}
}

func TestSplitExtractWritesFiles(t *testing.T) {
	// Create a temp directory
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "order.avsc")

	schema := `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "address", "type": {
      "type": "record",
      "name": "Address",
      "namespace": "com.example",
      "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"}
      ]
    }}
  ]
}`

	if err := os.WriteFile(schemaFile, []byte(schema), 0644); err != nil {
		t.Fatalf("failed to write test schema: %v", err)
	}

	result, err := splitAvroSchema(schema, 0, "")
	if err != nil {
		t.Fatalf("splitAvroSchema failed: %v", err)
	}

	// Write to output dir
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}

	for _, typ := range result.Types {
		ext := getExtensionForType("AVRO")
		filename := sanitizeFilename(typ.Subject) + ext
		filePath := filepath.Join(outputDir, filename)
		if err := os.WriteFile(filePath, []byte(typ.Schema), 0644); err != nil {
			t.Fatalf("failed to write %s: %v", filePath, err)
		}
	}

	// Write manifest
	manifest, _ := json.MarshalIndent(result, "", "  ")
	manifestPath := filepath.Join(outputDir, "manifest.json")
	if err := os.WriteFile(manifestPath, manifest, 0644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// Verify files exist
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("failed to read output dir: %v", err)
	}

	if len(files) < 3 { // At least 2 schema files + manifest
		t.Errorf("expected at least 3 files, got %d", len(files))
	}

	// Verify manifest is valid JSON
	manifestContent, _ := os.ReadFile(manifestPath)
	var parsedManifest SplitResult
	if err := json.Unmarshal(manifestContent, &parsedManifest); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
}

func TestGetReferenceName(t *testing.T) {
	tests := []struct {
		name       string
		typ        ExtractedType
		schemaType string
		expected   string
	}{
		{
			name:       "Avro reference",
			typ:        ExtractedType{Name: "com.example.Address"},
			schemaType: "AVRO",
			expected:   "com.example.Address",
		},
		{
			name:       "Protobuf reference",
			typ:        ExtractedType{Name: "Address"},
			schemaType: "PROTOBUF",
			expected:   "address.proto",
		},
		{
			name:       "JSON reference without extension",
			typ:        ExtractedType{Name: "address"},
			schemaType: "JSON",
			expected:   "address.json",
		},
		{
			name:       "JSON reference with extension",
			typ:        ExtractedType{Name: "address.json"},
			schemaType: "JSON",
			expected:   "address.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getReferenceName(&tt.typ, tt.schemaType)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Address", "address"},
		{"LineItem", "line_item"},
		{"Customer", "customer"},
		{"OrderLineItem", "order_line_item"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		result := toSnakeCase(tt.input)
		if result != tt.expected {
			t.Errorf("toSnakeCase(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"com.example.Address", "com_example_Address"},
		{"address.proto", "address_proto"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		result := sanitizeFilename(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
