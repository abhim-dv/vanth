package state

import (
	"path/filepath"
	"testing"
)

// fixturePath returns the Python-generated schema-v5 fixture. The generator is
// scripts/generate_go_fixture.py; the fixture is checked into testdata/.
func fixturePath(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "state", "jobs.sqlite")
	return path
}

func TestGoOpensPythonSchemaV5Fixture(t *testing.T) {
	db, err := OpenReadOnly(fixturePath(t))
	if err != nil {
		t.Fatalf("open python fixture: %v", err)
	}
	defer db.Close()

	version, err := SchemaVersion(db)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != LatestSchemaVersion {
		t.Fatalf("schema version = %d, want LatestSchemaVersion", version)
	}
}

func TestGoReadsPythonCreatedJobs(t *testing.T) {
	db, err := OpenReadOnly(fixturePath(t))
	if err != nil {
		t.Fatalf("open python fixture: %v", err)
	}
	defer db.Close()

	completed, err := Job(db, "job_completed")
	if err != nil {
		t.Fatalf("read completed job: %v", err)
	}
	if completed.Status != "completed" || completed.ExitCode.Int64 != 0 {
		t.Fatalf("completed job mismatch: %+v", completed)
	}
	if !completed.Name.Valid || completed.Name.String != "conformance complete" {
		t.Fatalf("completed name mismatch: %+v", completed.Name)
	}
	if completed.TagsJSON == "" {
		t.Fatal("tags_json should round-trip")
	}

	running, err := Job(db, "job_running")
	if err != nil {
		t.Fatalf("read running job: %v", err)
	}
	if running.Status != "running" || !running.Pid.Valid || running.Pid.Int64 != 2345 {
		t.Fatalf("running job mismatch: %+v", running)
	}
}

func TestGoOpenReadOnlyDoesNotWrite(t *testing.T) {
	db, err := OpenReadOnly(fixturePath(t))
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer db.Close()

	version, err := SchemaVersion(db)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != LatestSchemaVersion {
		t.Fatalf("schema version = %d, want LatestSchemaVersion", version)
	}
}
