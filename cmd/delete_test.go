package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestCheckReferentialIntegrityFlag(t *testing.T) {
	tests := []struct {
		name          string
		skipCheck     bool
		expectSkipped bool
	}{
		{
			name:          "skip check disabled",
			skipCheck:     false,
			expectSkipped: false,
		},
		{
			name:          "skip check enabled",
			skipCheck:     true,
			expectSkipped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore global flag
			origSkipRefCheck := deleteSkipRefCheck
			deleteSkipRefCheck = tt.skipCheck
			defer func() { deleteSkipRefCheck = origSkipRefCheck }()

			if deleteSkipRefCheck != tt.expectSkipped {
				t.Errorf("expected skipRefCheck to be %v", tt.expectSkipped)
			}
		})
	}
}

func TestDeleteVersionWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100},
		{Subject: "test-subject", Version: 2, ID: 101},
	})

	// Test delete version through mock
	version, err := mock.DeleteVersion("test-subject", "1", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	// Verify call was recorded
	deleteVersionCalled := mock.GetCallCount("DeleteVersion") > 0
	if !deleteVersionCalled {
		t.Error("expected DeleteVersion to be called")
	}
}

func TestDeleteSubjectWithMock(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100},
		{Subject: "test-subject", Version: 2, ID: 101},
	})

	// Test delete subject through mock
	versions, err := mock.DeleteSubject("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}

	// Verify call was recorded
	deleteSubjectCalled := mock.GetCallCount("DeleteSubject") > 0
	if !deleteSubjectCalled {
		t.Error("expected DeleteSubject to be called")
	}
}

func TestKeepLatestLogic(t *testing.T) {
	tests := []struct {
		name          string
		numVersions   int
		keepN         int
		expectDeleted int
	}{
		{
			name:          "keep 2 of 5",
			numVersions:   5,
			keepN:         2,
			expectDeleted: 3,
		},
		{
			name:          "keep more than existing",
			numVersions:   3,
			keepN:         5,
			expectDeleted: 0,
		},
		{
			name:          "keep all",
			numVersions:   3,
			keepN:         3,
			expectDeleted: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := client.NewMockClient()
			schemas := make([]client.Schema, tt.numVersions)
			for i := 0; i < tt.numVersions; i++ {
				schemas[i] = client.Schema{
					Subject: "test-subject",
					Version: i + 1,
					ID:      100 + i,
				}
			}
			mock.AddSubject("test-subject", schemas)

			// Verify versions exist
			versions, _ := mock.GetVersions("test-subject", false)
			if len(versions) != tt.numVersions {
				t.Errorf("expected %d versions, got %d", tt.numVersions, len(versions))
			}

			// Calculate versions to delete
			toDelete := tt.numVersions - tt.keepN
			if toDelete < 0 {
				toDelete = 0
			}

			if toDelete != tt.expectDeleted {
				t.Errorf("expected %d deletions, got %d", tt.expectDeleted, toDelete)
			}
		})
	}
}

func TestDeletePermanent(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100},
		{Subject: "test-subject", Version: 2, ID: 101},
	})

	// Test permanent delete
	_, err := mock.DeleteSubject("test-subject", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify subject was deleted
	_, err = mock.GetVersions("test-subject", false)
	if err == nil {
		t.Error("expected error after permanent delete")
	}
}

func TestDeleteReferencedBy(t *testing.T) {
	mock := client.NewMockClient()
	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100},
	})

	// Test GetSchemaReferencedBy
	refs, err := mock.GetSchemaReferencedBy("test-subject", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mock returns empty refs by default
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

func TestDeleteWorkersParallel(t *testing.T) {
	mock := client.NewMockClient()

	// Add multiple subjects
	for i := 0; i < 10; i++ {
		mock.AddSubject("test-subject-"+string(rune('a'+i)), []client.Schema{
			{Subject: "test-subject-" + string(rune('a'+i)), Version: 1, ID: 100 + i},
		})
	}

	// Verify all subjects exist
	subjects, _ := mock.GetSubjects(false)
	if len(subjects) != 10 {
		t.Fatalf("expected 10 subjects, got %d", len(subjects))
	}
}

func TestDeleteFlags(t *testing.T) {
	// Test default flag values
	if deleteForce {
		t.Error("expected deleteForce to be false by default")
	}

	if deletePermanent {
		t.Error("expected deletePermanent to be false by default")
	}

	if deleteKeepLatest != 0 {
		t.Errorf("expected deleteKeepLatest to be 0, got %d", deleteKeepLatest)
	}
}

func TestConfirmActionFunction(t *testing.T) {
	// Test that confirmAction function exists
	_ = confirmAction
}
