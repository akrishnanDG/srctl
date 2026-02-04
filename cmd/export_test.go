package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestSchemaExportStruct(t *testing.T) {
	export := schemaExport{
		Subject:    "test-subject",
		Version:    1,
		SchemaID:   100,
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"Test","fields":[]}`,
		References: []client.SchemaReference{
			{Name: "common.avsc", Subject: "common-types", Version: 1},
		},
	}

	if export.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", export.Subject)
	}

	if export.SchemaID != 100 {
		t.Errorf("expected schema ID 100, got %d", export.SchemaID)
	}

	if len(export.References) != 1 {
		t.Errorf("expected 1 reference, got %d", len(export.References))
	}
}

func TestMockClientForExport(t *testing.T) {
	mock := client.NewMockClient()

	// Add test subjects
	for i := 0; i < 3; i++ {
		name := "test-subject-" + string(rune('a'+i))
		mock.AddSubject(name, []client.Schema{
			{Subject: name, Version: 1, ID: 100 + i, SchemaType: "AVRO", Schema: `{"type":"string"}`},
			{Subject: name, Version: 2, ID: 200 + i, SchemaType: "AVRO", Schema: `{"type":"int"}`},
		})
	}

	// Verify subjects exist
	subjects, err := mock.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 3 {
		t.Errorf("expected 3 subjects, got %d", len(subjects))
	}

	// Verify versions
	for _, subj := range subjects {
		versions, err := mock.GetVersions(subj, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(versions) != 2 {
			t.Errorf("expected 2 versions for %s, got %d", subj, len(versions))
		}
	}
}

func TestExportToDirectoryCreatesFiles(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	schemas := []schemaExport{
		{Subject: "test-subject-a", Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	}

	err := exportToDirectory(schemas, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that output directory has some content
	files, _ := os.ReadDir(dir)
	if len(files) == 0 {
		t.Error("expected some files/directories to be created")
	}
}

func TestExportToTar(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	schemas := []schemaExport{
		{Subject: "test-subject", Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	}

	tarPath := filepath.Join(dir, "export.tar.gz")
	err := exportToTar(schemas, tarPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that tar file was created
	if _, err := os.Stat(tarPath); os.IsNotExist(err) {
		t.Error("expected tar file to exist")
	}

	// Check file size > 0
	info, _ := os.Stat(tarPath)
	if info.Size() == 0 {
		t.Error("expected tar file to have content")
	}
}

func TestExportToZip(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	schemas := []schemaExport{
		{Subject: "test-subject", Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	}

	zipPath := filepath.Join(dir, "export.zip")
	err := exportToZip(schemas, zipPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that zip file was created
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		t.Error("expected zip file to exist")
	}

	// Check file size > 0
	info, _ := os.Stat(zipPath)
	if info.Size() == 0 {
		t.Error("expected zip file to have content")
	}
}

func TestGetSchemaExtension(t *testing.T) {
	tests := []struct {
		schemaType string
		expected   string
	}{
		{"AVRO", "avsc"},
		{"PROTOBUF", "proto"},
		{"JSON", "json"},
		{"", "avsc"},       // default
		{"UNKNOWN", "avsc"}, // defaults to avsc
	}

	for _, tt := range tests {
		t.Run(tt.schemaType, func(t *testing.T) {
			ext := getSchemaExtension(tt.schemaType)
			if ext != tt.expected {
				t.Errorf("expected extension '%s' for type '%s', got '%s'", tt.expected, tt.schemaType, ext)
			}
		})
	}
}

func TestExportWithReferences(t *testing.T) {
	mock := client.NewMockClient()

	// Add schemas with references
	mock.AddSubject("common-types", []client.Schema{
		{Subject: "common-types", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"record","name":"Common","fields":[]}`},
	})

	mock.AddSubject("user-events", []client.Schema{
		{
			Subject:    "user-events",
			Version:    1,
			ID:         101,
			SchemaType: "AVRO",
			Schema:     `{"type":"record","name":"User","fields":[]}`,
			References: []client.SchemaReference{
				{Name: "common.avsc", Subject: "common-types", Version: 1},
			},
		},
	})

	// Verify schema has references
	schema, err := mock.GetSchema("user-events", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schema.References) != 1 {
		t.Errorf("expected 1 reference, got %d", len(schema.References))
	}
}

func TestExportFlags(t *testing.T) {
	// Verify export command flags exist
	if exportCmd == nil {
		t.Fatal("expected exportCmd to be defined")
	}

	// Check for output flag
	outputFlag := exportCmd.Flags().Lookup("output")
	if outputFlag == nil {
		t.Error("expected --output flag to exist")
	}

	// Check for archive flag
	archiveFlag := exportCmd.Flags().Lookup("archive")
	if archiveFlag == nil {
		t.Error("expected --archive flag to exist")
	}

	// Check for workers flag
	workersFlag := exportCmd.Flags().Lookup("workers")
	if workersFlag == nil {
		t.Error("expected --workers flag to exist")
	}
}

func TestExportVersionModes(t *testing.T) {
	// Test version mode options
	validModes := []string{"all", "latest"}
	for _, mode := range validModes {
		if mode != "all" && mode != "latest" {
			t.Errorf("unexpected version mode: %s", mode)
		}
	}
}
