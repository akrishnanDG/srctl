package cmd

import (
	"os"

	"github.com/srctl/srctl/internal/client"
)

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
