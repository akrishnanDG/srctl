package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateAvroFromJSON(t *testing.T) {
	sample := map[string]interface{}{
		"orderId": "123",
		"amount":  49.99,
		"active":  true,
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateAvroSchema(fields, "Order", "com.example")

	// Should be valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		t.Fatalf("generated schema is not valid JSON: %v", err)
	}

	if parsed["name"] != "Order" {
		t.Errorf("expected name 'Order', got '%v'", parsed["name"])
	}
	if parsed["namespace"] != "com.example" {
		t.Errorf("expected namespace 'com.example', got '%v'", parsed["namespace"])
	}
	if parsed["type"] != "record" {
		t.Errorf("expected type 'record', got '%v'", parsed["type"])
	}

	fieldList, ok := parsed["fields"].([]interface{})
	if !ok {
		t.Fatal("expected fields to be an array")
	}
	if len(fieldList) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fieldList))
	}
}

func TestGenerateAvroNestedObject(t *testing.T) {
	sample := map[string]interface{}{
		"id": "123",
		"address": map[string]interface{}{
			"street": "123 Main St",
			"city":   "Springfield",
		},
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateAvroSchema(fields, "User", "com.example")

	var parsed map[string]interface{}
	json.Unmarshal([]byte(schema), &parsed)

	fieldList := parsed["fields"].([]interface{})
	for _, f := range fieldList {
		field := f.(map[string]interface{})
		if field["name"] == "address" {
			// Should be a nested record
			fieldType, ok := field["type"].(map[string]interface{})
			if !ok {
				t.Fatal("address type should be a record object")
			}
			if fieldType["type"] != "record" {
				t.Errorf("expected nested record, got '%v'", fieldType["type"])
			}
		}
	}
}

func TestGenerateAvroArray(t *testing.T) {
	sample := map[string]interface{}{
		"id":    "123",
		"items": []interface{}{"a", "b", "c"},
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateAvroSchema(fields, "Order", "com.example")

	if !strings.Contains(schema, "array") {
		t.Error("expected array type in schema")
	}
}

func TestGenerateAvroNullable(t *testing.T) {
	sample := map[string]interface{}{
		"id":    "123",
		"notes": nil,
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateAvroSchema(fields, "Order", "com.example")

	// null value should produce nullable type
	if !strings.Contains(schema, "null") {
		t.Error("expected nullable type for nil value")
	}
}

func TestGenerateAvroMultipleSamples(t *testing.T) {
	samples := []map[string]interface{}{
		{"id": "1", "name": "Alice", "score": float64(95)},
		{"id": "2", "name": "Bob"}, // missing score - should be nullable
		{"id": "3", "name": "Carol", "score": float64(88)},
	}

	fields := InferFieldTypes(samples)

	// Score should be nullable since it's missing in sample 2
	if score, ok := fields["score"]; ok {
		if !score.IsNullable {
			t.Error("score should be nullable since it's missing in some samples")
		}
	} else {
		t.Error("expected 'score' field to be inferred")
	}
}

func TestGenerateProtobuf(t *testing.T) {
	sample := map[string]interface{}{
		"orderId": "123",
		"amount":  49.99,
		"count":   float64(5),
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateProtobufSchema(fields, "Order", "com.example")

	if !strings.Contains(schema, "syntax = \"proto3\"") {
		t.Error("expected proto3 syntax")
	}
	if !strings.Contains(schema, "package com.example") {
		t.Error("expected package declaration")
	}
	if !strings.Contains(schema, "message Order") {
		t.Error("expected message Order")
	}
	if !strings.Contains(schema, "double amount") {
		t.Error("expected double amount field")
	}
	if !strings.Contains(schema, "string order_id") {
		t.Error("expected string order_id field")
	}
}

func TestGenerateJSONSchema(t *testing.T) {
	sample := map[string]interface{}{
		"orderId": "123",
		"amount":  49.99,
		"active":  true,
	}

	fields := InferFieldTypes([]map[string]interface{}{sample})
	schema := GenerateJSONSchemaFromFields(fields)

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		t.Fatalf("generated schema is not valid JSON: %v", err)
	}

	if parsed["type"] != "object" {
		t.Errorf("expected type 'object', got '%v'", parsed["type"])
	}

	props, ok := parsed["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("expected properties object")
	}

	if len(props) != 3 {
		t.Errorf("expected 3 properties, got %d", len(props))
	}

	// Check types
	if amountProp, ok := props["amount"].(map[string]interface{}); ok {
		if amountProp["type"] != "number" {
			t.Errorf("expected amount type 'number', got '%v'", amountProp["type"])
		}
	}

	if activeProp, ok := props["active"].(map[string]interface{}); ok {
		if activeProp["type"] != "boolean" {
			t.Errorf("expected active type 'boolean', got '%v'", activeProp["type"])
		}
	}
}

func TestInferAvroType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"string", "hello", "string"},
		{"integer", float64(42), "long"},
		{"float", 3.14, "double"},
		{"boolean", true, "boolean"},
		{"null", nil, "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferAvroType("test", tt.value)
			if result.Type != tt.expected {
				t.Errorf("expected type '%s', got '%s'", tt.expected, result.Type)
			}
		})
	}
}

func TestInferAvroTypeObject(t *testing.T) {
	value := map[string]interface{}{
		"street": "123 Main",
		"city":   "Springfield",
	}

	result := InferAvroType("address", value)
	if !result.IsObject {
		t.Error("expected IsObject to be true")
	}
	if result.Type != "record" {
		t.Errorf("expected type 'record', got '%s'", result.Type)
	}
	if len(result.Children) != 2 {
		t.Errorf("expected 2 children, got %d", len(result.Children))
	}
}

func TestInferAvroTypeArray(t *testing.T) {
	value := []interface{}{"a", "b", "c"}

	result := InferAvroType("tags", value)
	if !result.IsArray {
		t.Error("expected IsArray to be true")
	}
	if result.ItemType != "string" {
		t.Errorf("expected itemType 'string', got '%s'", result.ItemType)
	}
}

func TestReadJSONSamples(t *testing.T) {
	input := `{"id": "1", "name": "Alice"}
{"id": "2", "name": "Bob"}
{"id": "3", "name": "Carol"}`

	samples, err := ReadJSONSamples(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 3 {
		t.Errorf("expected 3 samples, got %d", len(samples))
	}
}

func TestReadJSONSamplesSingleObject(t *testing.T) {
	input := `{"id": "1", "name": "Alice"}`

	samples, err := ReadJSONSamples(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 1 {
		t.Errorf("expected 1 sample, got %d", len(samples))
	}
}
