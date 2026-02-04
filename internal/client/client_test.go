package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		auth     *AuthConfig
		wantAuth bool
	}{
		{
			name:     "client without auth",
			baseURL:  "http://localhost:8081",
			auth:     nil,
			wantAuth: false,
		},
		{
			name:    "client with auth",
			baseURL: "http://localhost:8081",
			auth: &AuthConfig{
				Username: "user",
				Password: "pass",
			},
			wantAuth: true,
		},
		{
			name:     "client with trailing slash",
			baseURL:  "http://localhost:8081/",
			auth:     nil,
			wantAuth: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.baseURL, tt.auth)

			if client == nil {
				t.Fatal("expected non-nil client")
			}

			// Check that trailing slash is removed
			if client.BaseURL[len(client.BaseURL)-1] == '/' {
				t.Error("expected trailing slash to be removed")
			}

			if tt.wantAuth && client.Auth == nil {
				t.Error("expected auth to be set")
			}
			if !tt.wantAuth && client.Auth != nil {
				t.Error("expected auth to be nil")
			}
		})
	}
}

func TestWithContext(t *testing.T) {
	client := NewClient("http://localhost:8081", nil)

	ctxClient := client.WithContext(".mycontext")

	if ctxClient.Context != ".mycontext" {
		t.Errorf("expected context '.mycontext', got '%s'", ctxClient.Context)
	}

	// Original client should not be modified
	if client.Context != "" {
		t.Error("original client context should be empty")
	}
}

func TestBuildURL(t *testing.T) {
	tests := []struct {
		name     string
		context  string
		path     string
		expected string
	}{
		{
			name:     "no context",
			context:  "",
			path:     "/subjects",
			expected: "http://localhost:8081/subjects",
		},
		{
			name:     "default context",
			context:  ".",
			path:     "/subjects",
			expected: "http://localhost:8081/subjects",
		},
		{
			name:     "with context",
			context:  ".mycontext",
			path:     "/subjects",
			expected: "http://localhost:8081/contexts/.mycontext/subjects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient("http://localhost:8081", nil)
			client.Context = tt.context

			got := client.buildURL(tt.path)
			if got != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, got)
			}
		})
	}
}

func TestGetSubjects(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			t.Errorf("expected path '/subjects', got '%s'", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"subject1", "subject2", "subject3"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	subjects, err := client.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 3 {
		t.Errorf("expected 3 subjects, got %d", len(subjects))
	}
}

func TestGetSubjectsWithDeleted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("deleted") != "true" {
			t.Error("expected deleted=true query parameter")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"subject1", "subject2"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	_, err := client.GetSubjects(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetVersions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/test-subject/versions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]int{1, 2, 3, 4, 5})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	versions, err := client.GetVersions("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 5 {
		t.Errorf("expected 5 versions, got %d", len(versions))
	}
}

func TestGetSchema(t *testing.T) {
	expectedSchema := Schema{
		Subject:    "test-subject",
		Version:    1,
		ID:         100,
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"Test","fields":[]}`,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/test-subject/versions/1" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expectedSchema)
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	schema, err := client.GetSchema("test-subject", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.ID != expectedSchema.ID {
		t.Errorf("expected ID %d, got %d", expectedSchema.ID, schema.ID)
	}
	if schema.Schema != expectedSchema.Schema {
		t.Errorf("schema content mismatch")
	}
}

func TestGetSchemaByID(t *testing.T) {
	expectedSchema := Schema{
		SchemaType: "AVRO",
		Schema:     `{"type":"record","name":"Test","fields":[]}`,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/100" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expectedSchema)
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	schema, err := client.GetSchemaByID(100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema.ID != 100 {
		t.Errorf("expected ID 100, got %d", schema.ID)
	}
}

func TestRegisterSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/subjects/test-subject/versions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		if body["schema"] == nil {
			t.Error("expected schema in body")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"id": 101})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	schema := &Schema{
		Schema: `{"type":"record","name":"Test","fields":[]}`,
	}

	id, err := client.RegisterSchema("test-subject", schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id != 101 {
		t.Errorf("expected ID 101, got %d", id)
	}
}

func TestDeleteSubject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]int{1, 2, 3})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	versions, err := client.DeleteSubject("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestDeleteSubjectPermanent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("permanent") != "true" {
			t.Error("expected permanent=true query parameter")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]int{1, 2})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	_, err := client.DeleteSubject("test-subject", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/subjects/test-subject/versions/2" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(2)
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	version, err := client.DeleteVersion("test-subject", "2", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}
}

func TestGetConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Config{CompatibilityLevel: "BACKWARD"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	config, err := client.GetConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config.CompatibilityLevel != "BACKWARD" {
		t.Errorf("expected BACKWARD, got %s", config.CompatibilityLevel)
	}
}

func TestSetConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("expected PUT, got %s", r.Method)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)

		if body["compatibility"] != "FULL" {
			t.Errorf("expected compatibility FULL, got %s", body["compatibility"])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Config{CompatibilityLevel: "FULL"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	err := client.SetConfig("FULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Mode{Mode: "READWRITE"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	mode, err := client.GetMode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mode.Mode != "READWRITE" {
		t.Errorf("expected READWRITE, got %s", mode.Mode)
	}
}

func TestCheckCompatibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"is_compatible": true})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	schema := &Schema{Schema: `{"type":"string"}`}
	compatible, err := client.CheckCompatibility("test-subject", schema, "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !compatible {
		t.Error("expected compatible to be true")
	}
}

func TestGetContexts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contexts" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{".", ".staging", ".production"})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	contexts, err := client.GetContexts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(contexts) != 3 {
		t.Errorf("expected 3 contexts, got %d", len(contexts))
	}
}

func TestGetSchemaReferencedBy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/test-subject/versions/1/referencedby" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]int{100, 101, 102})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	refs, err := client.GetSchemaReferencedBy("test-subject", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(refs) != 3 {
		t.Errorf("expected 3 references, got %d", len(refs))
	}
}

func TestErrorHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":40401,"message":"Subject not found"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	_, err := client.GetSubjects(false)
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestBasicAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok {
			t.Error("expected basic auth to be set")
		}
		if user != "testuser" || pass != "testpass" {
			t.Errorf("unexpected credentials: %s:%s", user, pass)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer server.Close()

	client := NewClient(server.URL, &AuthConfig{
		Username: "testuser",
		Password: "testpass",
	})

	_, err := client.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
