package replicator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/kafka"
)

func TestApplySchemaEvent(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{
		TargetClient: mock,
		PreserveIDs:  false,
	})

	event := &kafka.SchemaEvent{
		Type:       kafka.KeyTypeSchema,
		Subject:    "test-value",
		Version:    1,
		SchemaID:   100,
		Schema:     `{"type":"record","name":"Test","fields":[]}`,
		SchemaType: "AVRO",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("RegisterSchema") != 1 {
		t.Errorf("expected 1 RegisterSchema call, got %d", mock.GetCallCount("RegisterSchema"))
	}
	// Should NOT set subject mode when not preserving IDs
	if mock.GetCallCount("SetSubjectMode") != 0 {
		t.Errorf("expected 0 SetSubjectMode calls, got %d", mock.GetCallCount("SetSubjectMode"))
	}
}

func TestApplySchemaEvent_PreserveIDs(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{
		TargetClient: mock,
		PreserveIDs:  true,
	})

	event := &kafka.SchemaEvent{
		Type:       kafka.KeyTypeSchema,
		Subject:    "test-value",
		Version:    1,
		SchemaID:   100,
		Schema:     `{"type":"record","name":"Test","fields":[]}`,
		SchemaType: "AVRO",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("RegisterSchema") != 1 {
		t.Errorf("expected 1 RegisterSchema call, got %d", mock.GetCallCount("RegisterSchema"))
	}
	// Should set IMPORT then READWRITE
	if mock.GetCallCount("SetSubjectMode") != 2 {
		t.Errorf("expected 2 SetSubjectMode calls, got %d", mock.GetCallCount("SetSubjectMode"))
	}
}

func TestApplySchemaEvent_WithReferences(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{
		TargetClient: mock,
		PreserveIDs:  false,
	})

	event := &kafka.SchemaEvent{
		Type:       kafka.KeyTypeSchema,
		Subject:    "order-value",
		Version:    1,
		SchemaID:   200,
		Schema:     `{}`,
		SchemaType: "AVRO",
		References: []kafka.ReferenceValue{
			{Name: "common.Address", Subject: "address-value", Version: 1},
		},
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := mock.GetCalls()
	for _, call := range calls {
		if call.Method == "RegisterSchema" {
			schema := call.Args[1].(*client.Schema)
			if len(schema.References) != 1 {
				t.Errorf("expected 1 reference, got %d", len(schema.References))
			}
			if schema.References[0].Name != "common.Address" {
				t.Errorf("expected ref name common.Address, got %s", schema.References[0].Name)
			}
		}
	}
}

func TestApplySchemaEvent_Deleted(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("old-value", []client.Schema{{Subject: "old-value", Version: 1, ID: 1}})

	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:    kafka.KeyTypeSchema,
		Subject: "old-value",
		Version: 1,
		Deleted: true,
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("DeleteSubject") != 1 {
		t.Errorf("expected 1 DeleteSubject call, got %d", mock.GetCallCount("DeleteSubject"))
	}
}

func TestApplySchemaEvent_Tombstone(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("removed-value", []client.Schema{{Subject: "removed-value", Version: 1, ID: 1}})

	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:      kafka.KeyTypeSchema,
		Subject:   "removed-value",
		Version:   1,
		Tombstone: true,
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("DeleteSubject") != 1 {
		t.Errorf("expected 1 DeleteSubject call, got %d", mock.GetCallCount("DeleteSubject"))
	}
}

func TestApplySchemaEvent_DeleteNotFound(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	// Deleting a subject that doesn't exist on target should not error
	event := &kafka.SchemaEvent{
		Type:      kafka.KeyTypeSchema,
		Subject:   "nonexistent-value",
		Version:   1,
		Tombstone: true,
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error (should be ignored for not found): %v", err)
	}
}

func TestApplyConfigEvent_Subject(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:          kafka.KeyTypeConfig,
		Subject:       "user-value",
		Compatibility: "FULL_TRANSITIVE",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("SetSubjectConfig") != 1 {
		t.Errorf("expected 1 SetSubjectConfig call, got %d", mock.GetCallCount("SetSubjectConfig"))
	}
}

func TestApplyConfigEvent_Global(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:          kafka.KeyTypeConfig,
		Subject:       "",
		Compatibility: "BACKWARD",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("SetConfig") != 1 {
		t.Errorf("expected 1 SetConfig call, got %d", mock.GetCallCount("SetConfig"))
	}
}

func TestApplyModeEvent_Subject(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:    kafka.KeyTypeMode,
		Subject: "user-value",
		Mode:    "READONLY",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("SetSubjectMode") != 1 {
		t.Errorf("expected 1 SetSubjectMode call, got %d", mock.GetCallCount("SetSubjectMode"))
	}
}

func TestApplyModeEvent_GlobalSkipped(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	// Global mode changes should be skipped
	event := &kafka.SchemaEvent{
		Type:    kafka.KeyTypeMode,
		Subject: "",
		Mode:    "READWRITE",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("SetMode") != 0 {
		t.Errorf("expected 0 SetMode calls (global mode skipped), got %d", mock.GetCallCount("SetMode"))
	}
	if mock.GetCallCount("SetSubjectMode") != 0 {
		t.Errorf("expected 0 SetSubjectMode calls, got %d", mock.GetCallCount("SetSubjectMode"))
	}
}

func TestApplyDeleteEvent(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("deleted-value", []client.Schema{{Subject: "deleted-value", Version: 1, ID: 1}})

	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:    kafka.KeyTypeDeleteSubject,
		Subject: "deleted-value",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.GetCallCount("DeleteSubject") != 1 {
		t.Errorf("expected 1 DeleteSubject call, got %d", mock.GetCallCount("DeleteSubject"))
	}
}

func TestApplyDeleteEvent_NotFound(t *testing.T) {
	mock := client.NewMockClient()
	r := New(Config{TargetClient: mock})

	event := &kafka.SchemaEvent{
		Type:    kafka.KeyTypeDeleteSubject,
		Subject: "nonexistent",
	}

	err := r.applyEvent(nil, event)
	if err != nil {
		t.Fatalf("unexpected error (not found should be ignored): %v", err)
	}
}

func TestApplySchemaEvent_RegisterError(t *testing.T) {
	mock := client.NewMockClient()
	mock.RegisterError = fmt.Errorf("connection refused")
	r := New(Config{
		TargetClient: mock,
		PreserveIDs:  false,
	})

	event := &kafka.SchemaEvent{
		Type:       kafka.KeyTypeSchema,
		Subject:    "test-value",
		Version:    1,
		Schema:     `{}`,
		SchemaType: "AVRO",
	}

	err := r.applyEvent(nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		s       string
		pattern string
		want    bool
	}{
		{"user-events-value", "user-*", true},
		{"order-events-value", "user-*", false},
		{"user-events-value", "*-value", true},
		{"user-events-value", "*events*", true},
		{"user-events-value", "*", true},
		{"user-events-value", "user-events-value", true},
		{"user-events-value", "order-events-value", false},
		{"abc", "a*c", true},
		{"abc", "a*b*c", true},
		{"abc", "x*", false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.s, tt.pattern), func(t *testing.T) {
			got := matchGlob(tt.s, tt.pattern)
			if got != tt.want {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestFilterSubjects(t *testing.T) {
	subjects := []string{"user-events-value", "order-events-value", "user-profile-value", "payment-value"}

	filtered := filterSubjects(subjects, "user-*")
	if len(filtered) != 2 {
		t.Errorf("expected 2 filtered subjects, got %d", len(filtered))
	}

	filtered = filterSubjects(subjects, "*-value")
	if len(filtered) != 4 {
		t.Errorf("expected 4 filtered subjects, got %d", len(filtered))
	}

	filtered = filterSubjects(subjects, "payment-value")
	if len(filtered) != 1 {
		t.Errorf("expected 1 filtered subject, got %d", len(filtered))
	}
}

func TestStats_Concurrent(t *testing.T) {
	stats := &Stats{StartTime: time.Now()}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats.IncrSchemas()
			stats.IncrConfigs()
			stats.IncrErrors()
			stats.IncrProcessed()
			stats.IncrFiltered()
			stats.SetOffset(int64(i))
			stats.SetLastEventTime(time.Now())
			_ = stats.Snapshot()
		}()
	}
	wg.Wait()

	snap := stats.Snapshot()
	if snap.SchemasReplicated != 100 {
		t.Errorf("expected 100 schemas, got %d", snap.SchemasReplicated)
	}
	if snap.ConfigsReplicated != 100 {
		t.Errorf("expected 100 configs, got %d", snap.ConfigsReplicated)
	}
	if snap.Errors != 100 {
		t.Errorf("expected 100 errors, got %d", snap.Errors)
	}
}

func TestIsAlreadyExistsError(t *testing.T) {
	if !isAlreadyExistsError(fmt.Errorf("schema already exists")) {
		t.Error("expected true for 'already exists'")
	}
	if !isAlreadyExistsError(fmt.Errorf("schema already registered")) {
		t.Error("expected true for 'already registered'")
	}
	if isAlreadyExistsError(fmt.Errorf("connection refused")) {
		t.Error("expected false for 'connection refused'")
	}
}
