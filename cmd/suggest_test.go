package cmd

import (
	"encoding/json"
	"testing"
)

func TestSuggestAddField(t *testing.T) {
	schema := `{"type":"record","name":"User","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}`

	fields := extractAvroFields(func() interface{} {
		var s interface{}
		json.Unmarshal([]byte(schema), &s)
		return s
	}())

	s := Suggestion{Compatibility: "BACKWARD"}
	result := suggestAddField(s, nil, fields, "email", "BACKWARD")

	if !result.Compatible {
		t.Error("adding a field should be compatible")
	}
	if result.FieldDef == "" {
		t.Error("should produce a field definition")
	}
	if result.Proposal == "" {
		t.Error("should produce a proposal")
	}
	// Should suggest nullable with default for BACKWARD
	if result.FieldDef == "" || !contains(result.FieldDef, "null") {
		t.Error("BACKWARD compat should suggest nullable field")
	}
}

func TestSuggestAddFieldAlreadyExists(t *testing.T) {
	fields := map[string]string{"id": "string", "name": "string"}

	s := Suggestion{}
	result := suggestAddField(s, nil, fields, "id", "BACKWARD")

	if result.Compatible {
		t.Error("adding existing field should not be compatible")
	}
	if result.Warning == "" {
		t.Error("should warn about existing field")
	}
}

func TestSuggestRemoveField(t *testing.T) {
	fields := map[string]string{"id": "string", "name": "string"}

	s := Suggestion{}
	result := suggestRemoveField(s, nil, fields, "name", "BACKWARD")

	if result.Compatible {
		t.Error("removing field should not be compatible under BACKWARD")
	}
	if result.Warning == "" {
		t.Error("should produce a warning")
	}
	if len(result.Alternatives) == 0 {
		t.Error("should suggest alternatives")
	}
}

func TestSuggestRemoveFieldNone(t *testing.T) {
	fields := map[string]string{"id": "string", "name": "string"}

	s := Suggestion{}
	result := suggestRemoveField(s, nil, fields, "name", "NONE")

	if !result.Compatible {
		t.Error("removing field should be compatible under NONE")
	}
}

func TestSuggestRemoveNonexistent(t *testing.T) {
	fields := map[string]string{"id": "string"}

	s := Suggestion{}
	result := suggestRemoveField(s, nil, fields, "email", "BACKWARD")

	if result.Warning == "" {
		t.Error("should warn about nonexistent field")
	}
}

func TestSuggestRenameField(t *testing.T) {
	fields := map[string]string{"id": "string", "email": "string"}

	s := Suggestion{}
	result := suggestRenameField(s, nil, fields, "email", "emailAddress", "BACKWARD")

	if result.Compatible {
		t.Error("rename should not be compatible")
	}
	if result.Warning == "" {
		t.Error("should warn about rename being breaking")
	}
	if len(result.Alternatives) == 0 {
		t.Error("should suggest alternatives")
	}
}

func TestSuggestChangeTypePromotion(t *testing.T) {
	fields := map[string]string{"id": "string", "count": "int"}

	s := Suggestion{}
	result := suggestChangeType(s, nil, fields, "count", "long", "BACKWARD")

	if !result.Compatible {
		t.Error("int -> long promotion should be compatible")
	}
}

func TestSuggestChangeTypeBreaking(t *testing.T) {
	fields := map[string]string{"id": "string", "count": "int"}

	s := Suggestion{}
	result := suggestChangeType(s, nil, fields, "count", "string", "BACKWARD")

	if result.Compatible {
		t.Error("int -> string should not be compatible")
	}
	if len(result.Alternatives) == 0 {
		t.Error("should suggest alternatives")
	}
}

func TestSuggestChangeTypeNonexistent(t *testing.T) {
	fields := map[string]string{"id": "string"}

	s := Suggestion{}
	result := suggestChangeType(s, nil, fields, "missing", "string", "BACKWARD")

	if result.Warning == "" {
		t.Error("should warn about nonexistent field")
	}
}

func TestParseChangeDescription(t *testing.T) {
	tests := []struct {
		desc       string
		action     string
		fieldName  string
		targetName string
	}{
		{"add discount code", "add", "discountCode", ""},
		{"add a new field trackingNumber", "add", "trackingnumber", ""},
		{"remove the notes field", "remove", "notes", ""},
		{"delete email", "remove", "email", ""},
		{"rename email to emailAddress", "rename", "email", "emailaddress"},
		{"change type of amount to string", "changeType", "amount", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			action, fieldName, targetName := parseChangeDescription(tt.desc)
			if action != tt.action {
				t.Errorf("action: expected '%s', got '%s'", tt.action, action)
			}
			if fieldName != tt.fieldName {
				t.Errorf("fieldName: expected '%s', got '%s'", tt.fieldName, fieldName)
			}
			if targetName != tt.targetName {
				t.Errorf("targetName: expected '%s', got '%s'", tt.targetName, targetName)
			}
		})
	}
}

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"discount code", "discountCode"},
		{"shipping address", "shippingAddress"},
		{"id", "id"},
		{"tracking number", "trackingNumber"},
	}

	for _, tt := range tests {
		result := toCamelCase(tt.input)
		if result != tt.expected {
			t.Errorf("toCamelCase(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestIsAvroTypePromotion(t *testing.T) {
	tests := []struct {
		old      string
		new      string
		expected bool
	}{
		{"int", "long", true},
		{"int", "float", true},
		{"int", "double", true},
		{"long", "double", true},
		{"float", "double", true},
		{"string", "bytes", true},
		{"bytes", "string", true},
		{"int", "string", false},
		{"string", "int", false},
		{"double", "int", false},
	}

	for _, tt := range tests {
		t.Run(tt.old+"->"+tt.new, func(t *testing.T) {
			result := isAvroTypePromotion(tt.old, tt.new)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSuggestAddFieldForward(t *testing.T) {
	fields := map[string]string{"id": "string"}

	s := Suggestion{}
	result := suggestAddField(s, nil, fields, "email", "FORWARD")

	if !result.Compatible {
		t.Error("adding field should be compatible under FORWARD")
	}
	// FORWARD doesn't need default
	if contains(result.FieldDef, "null") {
		t.Error("FORWARD should not require nullable")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsStr(s, substr)
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
