package monitor

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestRefreshMergesRemoteShadows proves the Phase 3 loader is no longer dead
// code (review P1-7): a Refresh against a home whose remote.sqlite carries
// current-timeline shadows projects them into res.Jobs alongside local rows.
func TestRefreshMergesRemoteShadows(t *testing.T) {
	dir := t.TempDir()

	localDB := filepath.Join(dir, "jobs.sqlite")
	fixture := filepath.Join("..", "..", "testdata", "state", "jobs.sqlite")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(localDB, data, 0o644); err != nil {
		t.Fatalf("write local db: %v", err)
	}

	// Remote shadows db seeded exactly the way store.py writes it.
	remoteDB := filepath.Join(dir, "remote.sqlite")
	seedShadowRows(t, remoteDB)

	cfg := Config{
		Home:         dir,
		DBPath:       localDB,
		RemoteDBPath: remoteDB,
		JobLimit:     50,
	}
	q := NewQuerier(cfg)
	defer q.Close()
	res := q.Refresh(context.Background(), RefreshRequest{})

	found := false
	for _, j := range res.Jobs {
		if j.JobID == "job_live" {
			found = true
			if !j.IsRemote() || j.Location() != "rmt_a" {
				t.Fatalf("shadow projected without remote markers: %+v", j)
			}
			if j.Status != "running" {
				t.Fatalf("shadow status = %q, want running", j.Status)
			}
		}
	}
	if !found {
		t.Fatalf("remote shadow job_live missing from refreshed jobs (%d total)", len(res.Jobs))
	}
}

func seedShadowRows(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE remotes (
		   remote_id TEXT PRIMARY KEY, name TEXT, target TEXT NOT NULL, state TEXT NOT NULL,
		   state_epoch INTEGER NOT NULL DEFAULT 1, controller_id TEXT, credential_state TEXT,
		   pairing_state TEXT, key_path TEXT, known_hosts_path TEXT, installed_at TEXT,
		   snapshot_cursor_json TEXT, feed_cursor_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE remote_shadows (
		   shadow_id TEXT PRIMARY KEY, remote_id TEXT NOT NULL, remote_job_id TEXT NOT NULL,
		   status TEXT NOT NULL, payload_json TEXT, state_epoch INTEGER NOT NULL DEFAULT 1,
		   suppressed_at TEXT, superseded_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`INSERT INTO remotes(remote_id, target, state, state_epoch, created_at, updated_at)
		 VALUES ('rmt_a', 'user@host', 'paired', 2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		`INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, state_epoch, created_at, updated_at)
		 VALUES ('shd_x', 'rmt_a', 'job_live', 'running', '{"name":"train"}', 2, '2026-01-01T00:00:01Z', '2026-01-01T00:00:02Z')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %.40s: %v", s, err)
		}
	}
}
