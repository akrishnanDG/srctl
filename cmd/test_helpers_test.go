package cmd

import (
	"bytes"
	"io"
	"os"

	"github.com/srctl/srctl/internal/client"
)

// testClient is a mock client for testing commands
var testClient *client.MockSchemaRegistryClient

// setupTestClient creates and configures a mock client for testing
func setupTestClient() *client.MockSchemaRegistryClient {
	testClient = client.NewMockClient()
	return testClient
}

// addTestSubject adds a test subject with the given number of versions
func addTestSubject(mock *client.MockSchemaRegistryClient, name string, numVersions int) {
	schemas := make([]client.Schema, numVersions)
	for i := 0; i < numVersions; i++ {
		schemas[i] = client.Schema{
			Subject:    name,
			Version:    i + 1,
			ID:         100 + i,
			SchemaType: "AVRO",
			Schema:     `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`,
		}
	}
	mock.AddSubject(name, schemas)
}

// addTestSubjectWithSchema adds a test subject with a specific schema
func addTestSubjectWithSchema(mock *client.MockSchemaRegistryClient, name string, schemaContent string, numVersions int) {
	schemas := make([]client.Schema, numVersions)
	for i := 0; i < numVersions; i++ {
		schemas[i] = client.Schema{
			Subject:    name,
			Version:    i + 1,
			ID:         100 + i,
			SchemaType: "AVRO",
			Schema:     schemaContent,
		}
	}
	mock.AddSubject(name, schemas)
}

// captureOutput captures stdout and stderr for testing
func captureOutput(f func()) (string, string) {
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()

	os.Stdout = wOut
	os.Stderr = wErr

	f()

	wOut.Close()
	wErr.Close()

	os.Stdout = oldStdout
	os.Stderr = oldStderr

	var bufOut, bufErr bytes.Buffer
	io.Copy(&bufOut, rOut)
	io.Copy(&bufErr, rErr)

	return bufOut.String(), bufErr.String()
}

// createTempFile creates a temporary file with the given content
func createTempFile(content string) (string, func()) {
	f, err := os.CreateTemp("", "srctl-test-*")
	if err != nil {
		panic(err)
	}

	if _, err := f.WriteString(content); err != nil {
		panic(err)
	}
	f.Close()

	return f.Name(), func() {
		os.Remove(f.Name())
	}
}

// createTempDir creates a temporary directory
func createTempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "srctl-test-*")
	if err != nil {
		panic(err)
	}

	return dir, func() {
		os.RemoveAll(dir)
	}
}
