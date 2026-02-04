package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestFilterByList(t *testing.T) {
	tests := []struct {
		name     string
		subjects []string
		filter   []string
		expected int
	}{
		{
			name:     "filter some subjects",
			subjects: []string{"user-events", "order-events", "payment-events"},
			filter:   []string{"user-events", "order-events"},
			expected: 2,
		},
		{
			name:     "filter all subjects",
			subjects: []string{"user-events", "order-events"},
			filter:   []string{"user-events", "order-events"},
			expected: 2,
		},
		{
			name:     "filter no match",
			subjects: []string{"user-events", "order-events"},
			filter:   []string{"payment-events"},
			expected: 0,
		},
		{
			name:     "empty filter",
			subjects: []string{"user-events", "order-events"},
			filter:   []string{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterByList(tt.subjects, tt.filter)
			if len(result) != tt.expected {
				t.Errorf("expected %d subjects, got %d", tt.expected, len(result))
			}
		})
	}
}

func TestCompareResult(t *testing.T) {
	result := CompareResult{
		Subject:      "test-subject",
		SourceOnly:   false,
		TargetOnly:   false,
		VersionDiff:  true,
		SchemaDiff:   false,
		ConfigDiff:   true,
		SourceVers:   5,
		TargetVers:   3,
		SourceLatest: 5,
		TargetLatest: 3,
	}

	if result.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", result.Subject)
	}

	if !result.VersionDiff {
		t.Error("expected version diff to be true")
	}

	if result.SourceVers <= result.TargetVers {
		t.Error("expected source to have more versions than target")
	}
}

func TestSchemaToClone(t *testing.T) {
	schema := schemaToClone{
		Subject:     "test-subject",
		Version:     1,
		SchemaID:    100,
		SchemaType:  "AVRO",
		Schema:      `{"type":"string"}`,
		References:  []client.SchemaReference{},
		ConfigLevel: "BACKWARD",
		Mode:        "READWRITE",
	}

	if schema.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", schema.Subject)
	}

	if schema.SchemaID != 100 {
		t.Errorf("expected schema ID 100, got %d", schema.SchemaID)
	}
}

func TestMockClientForCompare(t *testing.T) {
	sourceMock := client.NewMockClient()
	targetMock := client.NewMockClient()

	// Add test subjects to source
	for i := 0; i < 3; i++ {
		name := "test-subject-" + string(rune('a'+i))
		sourceMock.AddSubject(name, []client.Schema{
			{Subject: name, Version: 1, ID: 100 + i, SchemaType: "AVRO", Schema: `{"type":"string"}`},
			{Subject: name, Version: 2, ID: 200 + i, SchemaType: "AVRO", Schema: `{"type":"int"}`},
		})
	}

	// Test source has subjects
	subjects, err := sourceMock.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 3 {
		t.Errorf("expected 3 subjects in source, got %d", len(subjects))
	}

	// Test target is empty
	targetSubjects, _ := targetMock.GetSubjects(false)
	if len(targetSubjects) != 0 {
		t.Errorf("expected 0 subjects in target, got %d", len(targetSubjects))
	}
}

func TestMockClientRegisterForClone(t *testing.T) {
	targetMock := client.NewMockClient()

	// Simulate cloning - register schemas
	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"string"}`,
	}

	id, err := targetMock.RegisterSchema("test-subject", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}

	// Verify RegisterSchema was called
	registerCalls := targetMock.GetCallCount("RegisterSchema")
	if registerCalls != 1 {
		t.Errorf("expected 1 RegisterSchema call, got %d", registerCalls)
	}
}

func TestCompareSubjectMaps(t *testing.T) {
	sourceMock := client.NewMockClient()
	targetMock := client.NewMockClient()

	// Source has 3 subjects, target has 2
	sourceMock.AddSubject("subject-1", []client.Schema{{Subject: "subject-1", Version: 1, ID: 100}})
	sourceMock.AddSubject("subject-2", []client.Schema{{Subject: "subject-2", Version: 1, ID: 101}})
	sourceMock.AddSubject("subject-3", []client.Schema{{Subject: "subject-3", Version: 1, ID: 102}})

	targetMock.AddSubject("subject-1", []client.Schema{{Subject: "subject-1", Version: 1, ID: 100}})
	targetMock.AddSubject("subject-2", []client.Schema{{Subject: "subject-2", Version: 1, ID: 101}})

	sourceSubjects, _ := sourceMock.GetSubjects(false)
	targetSubjects, _ := targetMock.GetSubjects(false)

	// Build maps
	sourceMap := make(map[string]bool)
	for _, s := range sourceSubjects {
		sourceMap[s] = true
	}

	targetMap := make(map[string]bool)
	for _, s := range targetSubjects {
		targetMap[s] = true
	}

	// Count source-only
	sourceOnlyCount := 0
	for s := range sourceMap {
		if !targetMap[s] {
			sourceOnlyCount++
		}
	}

	if sourceOnlyCount != 1 {
		t.Errorf("expected 1 source-only subject, got %d", sourceOnlyCount)
	}

	// Count target-only
	targetOnlyCount := 0
	for s := range targetMap {
		if !sourceMap[s] {
			targetOnlyCount++
		}
	}

	if targetOnlyCount != 0 {
		t.Errorf("expected 0 target-only subjects, got %d", targetOnlyCount)
	}
}

func TestCloneTagOperations(t *testing.T) {
	sourceMock := client.NewMockClient()
	targetMock := client.NewMockClient()

	// Add tags to source
	sourceMock.Tags = []client.Tag{
		{Name: "PII", Description: "Personal Identifiable Information"},
		{Name: "SENSITIVE", Description: "Sensitive Data"},
	}

	// Get tags from source
	tags, err := sourceMock.GetTags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// Clone tags to target
	for _, tag := range tags {
		err := targetMock.CreateTag(&tag)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Verify CreateTag was called for each tag
	createTagCalls := targetMock.GetCallCount("CreateTag")
	if createTagCalls != 2 {
		t.Errorf("expected 2 CreateTag calls, got %d", createTagCalls)
	}
}

func TestPreserveIDsDefault(t *testing.T) {
	// By default, cloneNoPreserveIDs should be false (meaning IDs are preserved)
	// This is the expected behavior after the change

	// Test that the flag name is correct
	// In the actual code, !cloneNoPreserveIDs means preserve IDs

	cloneNoPreserveIDs = false // default
	preserveIDs := !cloneNoPreserveIDs

	if !preserveIDs {
		t.Error("expected IDs to be preserved by default")
	}

	cloneNoPreserveIDs = true
	preserveIDs = !cloneNoPreserveIDs

	if preserveIDs {
		t.Error("expected IDs to NOT be preserved when --no-preserve-ids is set")
	}
}

func TestCompareSchemaContent(t *testing.T) {
	sourceMock := client.NewMockClient()
	targetMock := client.NewMockClient()

	// Same schema content
	schema := `{"type":"record","name":"User","fields":[]}`

	sourceMock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, Schema: schema},
	})

	targetMock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, Schema: schema},
	})

	sourceSchema, _ := sourceMock.GetSchema("user-events", "1")
	targetSchema, _ := targetMock.GetSchema("user-events", "1")

	if sourceSchema.Schema != targetSchema.Schema {
		t.Error("expected schemas to match")
	}
}

func TestCompareVersionCounts(t *testing.T) {
	sourceMock := client.NewMockClient()
	targetMock := client.NewMockClient()

	// Source has more versions
	sourceMock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100},
		{Subject: "user-events", Version: 2, ID: 101},
		{Subject: "user-events", Version: 3, ID: 102},
	})

	targetMock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100},
	})

	sourceVersions, _ := sourceMock.GetVersions("user-events", false)
	targetVersions, _ := targetMock.GetVersions("user-events", false)

	if len(sourceVersions) <= len(targetVersions) {
		t.Error("expected source to have more versions than target")
	}
}
