package state

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGoWritesPythonReads writes rows with Go, then verifies Python's sqlite3
// can read them back. This is the reverse direction of the read-only fixture
// test and closes the cross-language round-trip (plan §15.2 case 2).
func TestGoWritesPythonReads(t *testing.T) {
	if os.Getenv("VANTH_SKIP_CROSS_LANG") != "" {
		t.Skip("VANTH_SKIP_CROSS_LANG set")
	}
	src := filepath.Join("..", "..", "testdata", "state", "jobs.sqlite")
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.sqlite")
	copyFile(t, src, path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := "2026-01-01T00:00:00Z"
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO jobs(job_id, name, command, status, created_at, updated_at,
			stdout_path, stderr_path, events_path)
		VALUES ('job_go_written', 'go wrote', 'echo ok', 'completed', ?, ?, 'o', 'e', 'ev')`,
		now, now); err != nil {
		tx.Rollback()
		t.Fatalf("insert job: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO events(event_id, job_id, seq, type, source, created_at)
		VALUES ('evt_go', 'job_go_written', 1, 'checkpoint', 'stdout', ?)`, now); err != nil {
		tx.Rollback()
		t.Fatalf("insert event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cmd := exec.Command("uv", "run", "python", filepath.Join("..", "..", "scripts", "verify_go_write.py"), path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python verification failed: %v\n%s", err, out)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
