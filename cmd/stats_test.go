package cmd

import (
	"fmt"
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestIsInternalSubject(t *testing.T) {
	tests := []struct {
		name     string
		subject  string
		expected bool
	}{
		{
			name:     "ksql internal subject",
			subject:  "_confluent-ksql-default_query_123",
			expected: true,
		},
		{
			name:     "ksql internal subject 2",
			subject:  "_confluent-ksql-pksqlc_abc123",
			expected: true,
		},
		{
			name:     "regular subject",
			subject:  "user-events",
			expected: false,
		},
		{
			name:     "regular subject with underscore",
			subject:  "_schemas",
			expected: false,
		},
		{
			name:     "regular subject starting with confluent",
			subject:  "confluent-control-center",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isInternalSubject(tt.subject)
			if result != tt.expected {
				t.Errorf("isInternalSubject(%s) = %v, want %v", tt.subject, result, tt.expected)
			}
		})
	}
}

func TestSubjectResultStruct(t *testing.T) {
	result := subjectResult{
		Subject:          "test-subject",
		VersionCount:     5,
		TotalSize:        1024,
		SchemaIDs:        []int{100, 101, 102, 103, 104},
		TypeCounts:       map[string]int{"AVRO": 5},
		TotalRefCount:    2,
		VersionsWithRefs: 1,
		MinID:            100,
		MaxID:            104,
		MinSize:          100,
		MaxSize:          300,
		MaxSizeInfo:      "test-subject (v3)",
		Errors:           []string{},
		IsInternal:       false,
	}

	if result.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", result.Subject)
	}

	if result.VersionCount != 5 {
		t.Errorf("expected 5 versions, got %d", result.VersionCount)
	}

	if result.TypeCounts["AVRO"] != 5 {
		t.Errorf("expected 5 AVRO schemas, got %d", result.TypeCounts["AVRO"])
	}

	if result.TotalRefCount != 2 {
		t.Errorf("expected 2 total refs, got %d", result.TotalRefCount)
	}
}

func TestRegistryStats(t *testing.T) {
	stats := RegistryStats{
		TotalSubjects:   100,
		ActiveSubjects:  90,
		DeletedSubjects: 10,
		TotalVersions:   500,
		ActiveVersions:  450,
		DeletedVersions: 50,
		UniqueSchemaIDs: 400,
	}

	if stats.TotalSubjects != 100 {
		t.Errorf("expected 100 total subjects, got %d", stats.TotalSubjects)
	}

	if stats.ActiveSubjects+stats.DeletedSubjects != stats.TotalSubjects {
		t.Error("active + deleted should equal total subjects")
	}
}

func TestMockClientForStats(t *testing.T) {
	mock := client.NewMockClient()

	// Add test subject with multiple versions
	schemas := []client.Schema{
		{
			Subject:    "test-subject",
			Version:    1,
			ID:         100,
			SchemaType: "AVRO",
			Schema:     `{"type":"record","name":"Test1","fields":[]}`,
		},
		{
			Subject:    "test-subject",
			Version:    2,
			ID:         101,
			SchemaType: "AVRO",
			Schema:     `{"type":"record","name":"Test2","fields":[{"name":"id","type":"int"}]}`,
		},
		{
			Subject:    "test-subject",
			Version:    3,
			ID:         102,
			SchemaType: "PROTOBUF",
			Schema:     `syntax = "proto3"; message Test {}`,
		},
	}
	mock.AddSubject("test-subject", schemas)

	// Test getting versions
	versions, err := mock.GetVersions("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}

	// Test getting schema
	schema, err := mock.GetSchema("test-subject", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.SchemaType != "AVRO" {
		t.Errorf("expected AVRO, got %s", schema.SchemaType)
	}
}

func TestStatsError(t *testing.T) {
	mock := client.NewMockClient()
	mock.GetVersionsError = fmt.Errorf("subject not found")

	_, err := mock.GetVersions("nonexistent-subject", false)
	if err == nil {
		t.Error("expected error when subject not found")
	}
}

func TestStatsParallel(t *testing.T) {
	mock := client.NewMockClient()

	// Add multiple test subjects
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("test-subject-%d", i)
		mock.AddSubject(name, []client.Schema{
			{Subject: name, Version: 1, ID: 100 + i, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		})
	}

	subjects, _ := mock.GetSubjects(false)
	if len(subjects) != 5 {
		t.Errorf("expected 5 subjects, got %d", len(subjects))
	}
}

func TestStatsSizeFormatting(t *testing.T) {
	// Test that size values are reasonable
	stats := RegistryStats{
		TotalSchemaSize: 1024 * 1024, // 1 MB
		LargestSchema:   "test-subject (v1)",
	}

	if stats.TotalSchemaSize != 1024*1024 {
		t.Errorf("expected 1MB total size, got %d", stats.TotalSchemaSize)
	}

	if stats.LargestSchema != "test-subject (v1)" {
		t.Errorf("unexpected largest schema: %s", stats.LargestSchema)
	}
}

func TestInternalSubjectFiltering(t *testing.T) {
	subjects := []string{
		"user-events",
		"_confluent-ksql-default_query_1",
		"order-events",
		"_confluent-ksql-default_query_2",
		"payment-events",
	}

	var userSubjects []string
	var internalCount int

	for _, subj := range subjects {
		if isInternalSubject(subj) {
			internalCount++
		} else {
			userSubjects = append(userSubjects, subj)
		}
	}

	if len(userSubjects) != 3 {
		t.Errorf("expected 3 user subjects, got %d", len(userSubjects))
	}

	if internalCount != 2 {
		t.Errorf("expected 2 internal subjects, got %d", internalCount)
	}
}

func TestStatsCommand(t *testing.T) {
	if statsCmd == nil {
		t.Fatal("expected statsCmd to be defined")
	}

	if statsCmd.Use != "stats" {
		t.Errorf("expected Use to be 'stats', got '%s'", statsCmd.Use)
	}

	// Check for workers flag
	workersFlag := statsCmd.Flags().Lookup("workers")
	if workersFlag == nil {
		t.Error("expected --workers flag to exist")
	}
}

func TestStatsTypeCounts(t *testing.T) {
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

	// Verify each schema type
	avroSchema, _ := mock.GetSchema("avro-subject", "1")
	if avroSchema.SchemaType != "AVRO" {
		t.Errorf("expected AVRO, got %s", avroSchema.SchemaType)
	}

	protoSchema, _ := mock.GetSchema("proto-subject", "1")
	if protoSchema.SchemaType != "PROTOBUF" {
		t.Errorf("expected PROTOBUF, got %s", protoSchema.SchemaType)
	}

	jsonSchema, _ := mock.GetSchema("json-subject", "1")
	if jsonSchema.SchemaType != "JSON" {
		t.Errorf("expected JSON, got %s", jsonSchema.SchemaType)
	}
}
