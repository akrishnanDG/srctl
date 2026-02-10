package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestDanglingReference(t *testing.T) {
	ref := DanglingReference{
		ParentSubject:  "user-events",
		ParentVersion:  1,
		ParentSchemaID: 100,
		RefName:        "common.avsc",
		RefSubject:     "common-types",
		RefVersion:     1,
		Reason:         "Subject soft-deleted",
	}

	if ref.ParentSubject != "user-events" {
		t.Errorf("expected parent subject 'user-events', got '%s'", ref.ParentSubject)
	}

	if ref.RefSubject != "common-types" {
		t.Errorf("expected ref subject 'common-types', got '%s'", ref.RefSubject)
	}

	if ref.Reason != "Subject soft-deleted" {
		t.Errorf("expected reason 'Subject soft-deleted', got '%s'", ref.Reason)
	}
}

func TestDanglingReport(t *testing.T) {
	report := DanglingReport{
		TotalSubjects:    100,
		TotalVersions:    500,
		SchemasWithRefs:  50,
		DanglingCount:    5,
		AffectedSubjects: 3,
		DanglingRefs: []DanglingReference{
			{ParentSubject: "user-events", RefSubject: "common-types", Reason: "Subject soft-deleted"},
		},
		DeletedSubjects: []string{"common-types", "old-schema"},
	}

	if report.TotalSubjects != 100 {
		t.Errorf("expected 100 total subjects, got %d", report.TotalSubjects)
	}

	if report.DanglingCount != 5 {
		t.Errorf("expected 5 dangling refs, got %d", report.DanglingCount)
	}

	if len(report.DeletedSubjects) != 2 {
		t.Errorf("expected 2 deleted subjects, got %d", len(report.DeletedSubjects))
	}
}

func TestDanglingCommand(t *testing.T) {
	if danglingCmd == nil {
		t.Fatal("expected danglingCmd to be defined")
	}

	if danglingCmd.Use != "dangling" {
		t.Errorf("expected Use to be 'dangling', got '%s'", danglingCmd.Use)
	}

	// Check for workers flag
	workersFlag := danglingCmd.Flags().Lookup("workers")
	if workersFlag == nil {
		t.Error("expected --workers flag to exist")
	}

	// Check for json flag
	jsonFlag := danglingCmd.Flags().Lookup("json")
	if jsonFlag == nil {
		t.Error("expected --json flag to exist")
	}
}

func TestDanglingGroupID(t *testing.T) {
	if danglingCmd.GroupID != groupConfig {
		t.Errorf("expected GroupID '%s', got '%s'", groupConfig, danglingCmd.GroupID)
	}
}

func TestMockClientForDangling(t *testing.T) {
	mock := client.NewMockClient()

	// Add active subject with reference to deleted subject
	mock.AddSubject("user-events", []client.Schema{
		{
			Subject:    "user-events",
			Version:    1,
			ID:         100,
			SchemaType: "AVRO",
			Schema:     `{"type":"record","name":"User","fields":[]}`,
			References: []client.SchemaReference{
				{Name: "common.avsc", Subject: "common-types", Version: 1},
			},
		},
	})

	// Add the referenced subject (which would be deleted in real scenario)
	mock.AddSubject("common-types", []client.Schema{
		{Subject: "common-types", Version: 1, ID: 101, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	// Verify we can get subjects
	subjects, err := mock.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}

	// Verify we can get schema with references
	schema, err := mock.GetSchema("user-events", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schema.References) != 1 {
		t.Errorf("expected 1 reference, got %d", len(schema.References))
	}

	if schema.References[0].Subject != "common-types" {
		t.Errorf("expected reference to 'common-types', got '%s'", schema.References[0].Subject)
	}
}

func TestDanglingDetection(t *testing.T) {
	// Test the logic for detecting dangling references
	activeMap := map[string]bool{
		"user-events":  true,
		"order-events": true,
		// "common-types" is NOT in activeMap (soft-deleted)
	}

	ref := client.SchemaReference{
		Name:    "common.avsc",
		Subject: "common-types",
		Version: 1,
	}

	// Check if referenced subject is active
	isActive := activeMap[ref.Subject]

	if isActive {
		t.Error("expected common-types to be inactive (deleted)")
	}
}

func TestDanglingResult(t *testing.T) {
	result := danglingResult{
		Subject:      "user-events",
		VersionCount: 3,
		RefsFound:    2,
		DanglingRefs: []DanglingReference{
			{ParentSubject: "user-events", ParentVersion: 1, RefSubject: "common-types", Reason: "Subject soft-deleted"},
		},
	}

	if result.Subject != "user-events" {
		t.Errorf("expected subject 'user-events', got '%s'", result.Subject)
	}

	if result.VersionCount != 3 {
		t.Errorf("expected 3 versions, got %d", result.VersionCount)
	}

	if len(result.DanglingRefs) != 1 {
		t.Errorf("expected 1 dangling ref, got %d", len(result.DanglingRefs))
	}
}
