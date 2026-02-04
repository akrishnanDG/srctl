package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srctl/srctl/internal/client"
)

func TestBackupManifest(t *testing.T) {
	manifest := BackupManifest{
		Version:      "1.0",
		CreatedAt:    time.Now().UTC(),
		RegistryURL:  "http://localhost:8081",
		Context:      ".production",
		BySchemaID:   true,
		IncludesTags: true,
	}
	manifest.Statistics.Subjects = 100
	manifest.Statistics.Schemas = 500
	manifest.Statistics.TotalIDs = 400
	manifest.Statistics.TagDefinitions = 5
	manifest.Statistics.TagAssignments = 50

	if manifest.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", manifest.Version)
	}

	if manifest.Statistics.Subjects != 100 {
		t.Errorf("expected 100 subjects, got %d", manifest.Statistics.Subjects)
	}

	if !manifest.IncludesTags {
		t.Error("expected IncludesTags to be true")
	}
}

func TestSubjectBackup(t *testing.T) {
	backup := SubjectBackup{
		Subject:       "test-subject",
		Compatibility: "BACKWARD",
		Mode:          "READWRITE",
		Versions: []SchemaVersionBackup{
			{Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
			{Version: 2, SchemaID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
		},
	}

	if backup.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", backup.Subject)
	}

	if len(backup.Versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(backup.Versions))
	}

	if backup.Versions[0].SchemaID != 100 {
		t.Errorf("expected schema ID 100, got %d", backup.Versions[0].SchemaID)
	}
}

func TestIDMapping(t *testing.T) {
	mapping := IDMapping{
		SchemaID:   100,
		Subject:    "test-subject",
		Version:    1,
		SchemaType: "AVRO",
	}

	if mapping.SchemaID != 100 {
		t.Errorf("expected schema ID 100, got %d", mapping.SchemaID)
	}

	if mapping.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", mapping.Subject)
	}
}

func TestTagBackup(t *testing.T) {
	tagBackup := TagBackup{
		Definitions: []client.Tag{
			{Name: "PII", Description: "Personal Information"},
			{Name: "SENSITIVE", Description: "Sensitive Data"},
		},
		Assignments: []TagAssignmentBackup{
			{Subject: "user-events", Version: 0, TagNames: []string{"PII"}},
			{Subject: "user-events", Version: 1, TagNames: []string{"SENSITIVE"}},
		},
	}

	if len(tagBackup.Definitions) != 2 {
		t.Errorf("expected 2 tag definitions, got %d", len(tagBackup.Definitions))
	}

	if len(tagBackup.Assignments) != 2 {
		t.Errorf("expected 2 tag assignments, got %d", len(tagBackup.Assignments))
	}

	// Check subject-level vs schema-level assignment
	subjectLevel := tagBackup.Assignments[0]
	schemaLevel := tagBackup.Assignments[1]

	if subjectLevel.Version != 0 {
		t.Error("expected subject-level assignment to have version 0")
	}

	if schemaLevel.Version == 0 {
		t.Error("expected schema-level assignment to have non-zero version")
	}
}

func TestSaveJSON(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	path := filepath.Join(dir, "test.json")
	err := saveJSON(path, testData)
	if err != nil {
		t.Fatalf("failed to save JSON: %v", err)
	}

	// Read back and verify
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	var loaded map[string]string
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if loaded["key1"] != "value1" {
		t.Errorf("expected key1=value1, got key1=%s", loaded["key1"])
	}
}

func TestGetDirSize(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	// Create a test file
	testFile := filepath.Join(dir, "test.txt")
	content := "Hello, World!"
	os.WriteFile(testFile, []byte(content), 0644)

	size, err := getDirSize(dir)
	if err != nil {
		t.Fatalf("failed to get dir size: %v", err)
	}

	if size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), size)
	}
}

func TestBackupMockClientOperations(t *testing.T) {
	mock := client.NewMockClient()

	// Add test subject with config
	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		{Subject: "test-subject", Version: 2, ID: 101, SchemaType: "AVRO", Schema: `{"type":"int"}`},
	})
	mock.SubjectConfigs["test-subject"] = &client.Config{CompatibilityLevel: "FULL"}
	mock.SubjectModes["test-subject"] = &client.Mode{Mode: "READWRITE"}

	// Test getting versions
	versions, err := mock.GetVersions("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}

	// Test getting schema
	schema, err := mock.GetSchema("test-subject", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.ID != 100 {
		t.Errorf("expected ID 100, got %d", schema.ID)
	}

	// Test getting config
	config, err := mock.GetSubjectConfig("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "FULL" {
		t.Errorf("expected FULL, got %s", config.CompatibilityLevel)
	}
}

func TestBackupMultipleSubjects(t *testing.T) {
	mock := client.NewMockClient()

	// Add multiple test subjects
	for i := 0; i < 5; i++ {
		name := "test-subject-" + string(rune('a'+i))
		mock.AddSubject(name, []client.Schema{
			{Subject: name, Version: 1, ID: 100 + i, SchemaType: "AVRO", Schema: `{"type":"string"}`},
		})
	}

	subjects, err := mock.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 5 {
		t.Errorf("expected 5 subjects, got %d", len(subjects))
	}
}

func TestBackupTagsOperations(t *testing.T) {
	mock := client.NewMockClient()

	// Add tags
	mock.Tags = []client.Tag{
		{Name: "PII", Description: "Personal Information"},
	}

	// Test getting tags
	tags, err := mock.GetTags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tags) != 1 {
		t.Errorf("expected 1 tag, got %d", len(tags))
	}

	// Test creating tag
	err = mock.CreateTag(&client.Tag{Name: "SENSITIVE"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tags, _ = mock.GetTags()
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}
}

func TestBackupResult(t *testing.T) {
	result := backupResult{
		Subject:      "test-subject",
		VersionCount: 5,
		IDMappings: []IDMapping{
			{SchemaID: 100, Subject: "test-subject", Version: 1},
			{SchemaID: 101, Subject: "test-subject", Version: 2},
		},
		Error: nil,
	}

	if result.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", result.Subject)
	}

	if result.VersionCount != 5 {
		t.Errorf("expected 5 versions, got %d", result.VersionCount)
	}

	if len(result.IDMappings) != 2 {
		t.Errorf("expected 2 ID mappings, got %d", len(result.IDMappings))
	}
}

func TestRestoreTagOperations(t *testing.T) {
	mock := client.NewMockClient()

	// Test creating tag
	tag := &client.Tag{Name: "PII", Description: "Personal Information"}
	err := mock.CreateTag(tag)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify call was recorded
	calls := mock.GetCalls()
	found := false
	for _, call := range calls {
		if call.Method == "CreateTag" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CreateTag to be called")
	}

	// Test assigning tag
	err = mock.AssignTagToSubject("test-subject", "PII")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assignCalls := mock.GetCallCount("AssignTagToSubject")
	if assignCalls != 1 {
		t.Errorf("expected 1 AssignTagToSubject call, got %d", assignCalls)
	}
}
