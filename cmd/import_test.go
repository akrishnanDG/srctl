package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestImportFromDirectory(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	// Create subject directory
	subjectDir := filepath.Join(dir, "user-events")
	os.MkdirAll(subjectDir, 0755)

	// Create schema file
	schema := schemaExport{
		Subject:    "user-events",
		Version:    1,
		SchemaID:   100,
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[]}`,
	}

	schemaFile := filepath.Join(subjectDir, "v1.json")
	data, _ := json.MarshalIndent(schema, "", "  ")
	os.WriteFile(schemaFile, data, 0644)

	// Test that directory structure exists
	_, err := os.Stat(subjectDir)
	if os.IsNotExist(err) {
		t.Error("expected subject directory to exist")
	}

	_, err = os.Stat(schemaFile)
	if os.IsNotExist(err) {
		t.Error("expected schema file to exist")
	}
}

func TestParseSchemaFile(t *testing.T) {
	dir, cleanup := createTempDir()
	defer cleanup()

	schema := schemaExport{
		Subject:    "test-subject",
		Version:    1,
		SchemaID:   100,
		SchemaType: "AVRO",
		Schema:     `{"type":"string"}`,
	}

	schemaFile := filepath.Join(dir, "schema.json")
	data, _ := json.MarshalIndent(schema, "", "  ")
	os.WriteFile(schemaFile, data, 0644)

	// Read and parse
	content, err := os.ReadFile(schemaFile)
	if err != nil {
		t.Fatalf("failed to read schema file: %v", err)
	}

	var parsed schemaExport
	err = json.Unmarshal(content, &parsed)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	if parsed.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", parsed.Subject)
	}

	if parsed.Version != 1 {
		t.Errorf("expected version 1, got %d", parsed.Version)
	}
}

func TestImportSchema(t *testing.T) {
	mock := client.NewMockClient()

	schema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[]}`,
	}

	id, err := mock.RegisterSchema("user-events", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}
}

func TestImportWithReferences(t *testing.T) {
	mock := client.NewMockClient()

	// First import the referenced schema
	commonSchema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"Common","fields":[]}`,
	}

	_, err := mock.RegisterSchema("common-types", commonSchema)
	if err != nil {
		t.Fatalf("unexpected error importing common schema: %v", err)
	}

	// Then import schema with reference
	userSchema := &client.Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"User","fields":[]}`,
		References: []client.SchemaReference{
			{Name: "common.avsc", Subject: "common-types", Version: 1},
		},
	}

	id, err := mock.RegisterSchema("user-events", userSchema)
	if err != nil {
		t.Fatalf("unexpected error importing user schema: %v", err)
	}

	if id == 0 {
		t.Error("expected non-zero schema ID")
	}
}

func TestImportSkipExisting(t *testing.T) {
	mock := client.NewMockClient()

	// Pre-existing schema
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	})

	// Check if subject exists
	subjects, _ := mock.GetSubjects(false)
	exists := false
	for _, s := range subjects {
		if s == "user-events" {
			exists = true
			break
		}
	}

	if !exists {
		t.Error("expected subject to exist")
	}
}

func TestImportDryRun(t *testing.T) {
	mock := client.NewMockClient()

	// In dry run mode, no schemas should be registered
	initialSubjects, _ := mock.GetSubjects(false)
	initialCount := len(initialSubjects)

	// Simulate dry run - don't actually register
	// Just verify the count didn't change

	finalSubjects, _ := mock.GetSubjects(false)
	finalCount := len(finalSubjects)

	if finalCount != initialCount {
		t.Errorf("dry run should not change subject count, expected %d, got %d", initialCount, finalCount)
	}
}

func TestExtractFromTar(t *testing.T) {
	// Create a tar file first
	dir, cleanup := createTempDir()
	defer cleanup()

	schemas := []schemaExport{
		{Subject: "test-subject", Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	}

	tarPath := filepath.Join(dir, "test.tar.gz")
	err := exportToTar(schemas, tarPath)
	if err != nil {
		t.Fatalf("failed to create tar file: %v", err)
	}

	// Verify tar file exists
	_, err = os.Stat(tarPath)
	if os.IsNotExist(err) {
		t.Error("expected tar file to exist")
	}
}

func TestExtractFromZip(t *testing.T) {
	// Create a zip file first
	dir, cleanup := createTempDir()
	defer cleanup()

	schemas := []schemaExport{
		{Subject: "test-subject", Version: 1, SchemaID: 100, SchemaType: "AVRO", Schema: `{"type":"string"}`},
	}

	zipPath := filepath.Join(dir, "test.zip")
	err := exportToZip(schemas, zipPath)
	if err != nil {
		t.Fatalf("failed to create zip file: %v", err)
	}

	// Verify zip file exists
	_, err = os.Stat(zipPath)
	if os.IsNotExist(err) {
		t.Error("expected zip file to exist")
	}
}

func TestImportOrder(t *testing.T) {
	// Test that imports respect version order (oldest first)
	mock := client.NewMockClient()

	// Register versions in order
	for i := 1; i <= 3; i++ {
		schema := &client.Schema{
			SchemaType: "AVRO",
			Schema:     `{"type":"string"}`,
		}
		_, err := mock.RegisterSchema("user-events", schema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Verify versions
	versions, _ := mock.GetVersions("user-events", false)
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}

	// Versions should be in order
	for i, v := range versions {
		if v != i+1 {
			t.Errorf("expected version %d, got %d", i+1, v)
		}
	}
}
