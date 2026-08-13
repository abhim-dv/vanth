package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vanth/internal/state"
)

func fixtureCopy(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "..", "testdata", "state", "jobs.sqlite")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	dir := t.TempDir()
	dst := filepath.Join(dir, "jobs.sqlite")
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
	return dst
}

func sha256Of(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func walSize(path string) int64 {
	fi, err := os.Stat(path + "-wal")
	if err != nil {
		return -1
	}
	return fi.Size()
}

func testCfgFor(path string) Config {
	home := filepath.Dir(path)
	cfg := DefaultConfig(home)
	cfg.DBPath = path
	cfg.QueryTimeout = 200 * time.Millisecond
	cfg.DBBusyTimeoutMS = 50
	return cfg
}

func TestRefreshReadsPythonFixture(t *testing.T) {
	dbPath := fixtureCopy(t)
	cfg := testCfgFor(dbPath)
	q := NewQuerier(cfg)
	defer q.Close()

	req := RefreshRequest{
		Targets: []string{"job_completed", "job_running"},
		LastSeq: map[string]int64{},
	}
	res := q.Refresh(context.Background(), req)
	if res.Err != nil {
		t.Fatalf("refresh: %v", res.Err)
	}
	if !res.JobsOK {
		t.Fatal("jobs not loaded")
	}
	if len(res.Jobs) != 2 {
		t.Fatalf("jobs = %d, want 2", len(res.Jobs))
	}
	if !res.DBOpen || res.Fallback || res.Stale {
		t.Fatalf("unexpected flags: open=%v fallback=%v stale=%v", res.DBOpen, res.Fallback, res.Stale)
	}

	complete := res.Events["job_completed"]
	if len(complete) != 1 || complete[0].Type != "completed" {
		t.Fatalf("completed events = %+v", complete)
	}
	running := res.Events["job_running"]
	if len(running) != 1 || running[0].Type != "progress" {
		t.Fatalf("running events = %+v", running)
	}
	pts := res.Points[SeriesKey{JobID: "job_running", Metric: "progress.percent"}]
	if len(pts) != 1 || pts[0].Y != 33.33 {
		t.Fatalf("progress.percent = %+v", pts)
	}
	if res.LastSeq["job_completed"] != 1 || res.LastSeq["job_running"] != 1 {
		t.Errorf("lastSeq = %v", res.LastSeq)
	}

	// Incremental refresh with the cursor must return nothing new.
	req.LastSeq = res.LastSeq
	res2 := q.Refresh(context.Background(), req)
	if len(res2.Events) != 0 && len(res2.Points) != 0 {
		t.Errorf("incremental refresh returned data: events=%d points=%d", len(res2.Events), len(res2.Points))
	}
}

func TestReadOnlyGuarantee(t *testing.T) {
	dbPath := fixtureCopy(t)
	before, err := sha256Of(dbPath)
	if err != nil {
		t.Fatalf("sha before: %v", err)
	}
	cfg := testCfgFor(dbPath)
	q := NewQuerier(cfg)
	defer q.Close()

	req := RefreshRequest{Targets: []string{"job_completed", "job_running"}, LastSeq: map[string]int64{}}
	res := q.Refresh(context.Background(), req)
	if res.Err != nil {
		t.Fatalf("refresh: %v", res.Err)
	}
	// SQLite creates an empty -wal/-shm when opening a WAL-mode database even
	// read-only; the guarantee is that monitor activity never GROWS them.
	walBefore := walSize(dbPath)

	for i := 0; i < 3; i++ {
		res = q.Refresh(context.Background(), req)
		if res.Err != nil {
			t.Fatalf("refresh %d: %v", i, res.Err)
		}
		if walSize(dbPath) != walBefore {
			t.Errorf("refresh %d grew the WAL: %d -> %d", i, walBefore, walSize(dbPath))
		}
	}

	after, err := sha256Of(dbPath)
	if err != nil {
		t.Fatalf("sha after: %v", err)
	}
	if after != before {
		t.Error("monitor modified the database file")
	}
	if version := q.schemaVersion(); version != state.LatestSchemaVersion {
		t.Errorf("schema version = %d, want %d", version, state.LatestSchemaVersion)
	}
}

func (q *Querier) schemaVersion() int {
	db, err := q.open(context.Background())
	if err != nil || db == nil {
		return -1
	}
	version, _ := state.SchemaVersion(db)
	return version
}

func TestMissingDatabaseEmptyState(t *testing.T) {
	home := t.TempDir()
	cfg := DefaultConfig(home)
	cfg.DBPath = filepath.Join(home, "jobs.sqlite")
	cfg.EventsDir = filepath.Join(home, "events")
	q := NewQuerier(cfg)
	defer q.Close()

	res := q.Refresh(context.Background(), RefreshRequest{})
	if !res.Empty {
		t.Error("missing DB with no mirrors should be empty")
	}
	if !res.Fallback {
		t.Error("missing DB should use fallback mode")
	}
	if res.DBOpen {
		t.Error("DB should not report open")
	}
}

func TestJSONLFallbackReadsMirrors(t *testing.T) {
	home := t.TempDir()
	eventsDir := filepath.Join(home, "events")
	if err := os.MkdirAll(eventsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		`{"event_id":"e1","job_id":"job_a","seq":1,"type":"metric","level":"info","message":"m1","data":{"_step":10,"loss":0.42},"source":"stdout","created_at":"2026-01-01T00:00:00Z"}`,
		`{"event_id":"e2","job_id":"job_a","seq":2,"type":"progress","level":"info","message":null,"data":{"current":5,"total":10},"source":"stdout","created_at":"2026-01-01T00:00:00Z"}`,
		`not-json-at-all`,
		`{"event_id":"e3","job_id":"job_a","seq":3,"type":"metric","level":"info","message":"m3","data":{"loss":0.5},"source":"stdout","created_at":"2026-01-01T00:00:00Z"`,
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "job_a.jsonl"),
		[]byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig(home)
	cfg.EventsDir = eventsDir
	q := NewQuerier(cfg)
	defer q.Close()

	res := q.Refresh(context.Background(), RefreshRequest{Targets: []string{"job_a"}, LastSeq: map[string]int64{}})
	if !res.Fallback {
		t.Fatal("expected fallback mode")
	}
	if len(res.Jobs) != 1 || res.Jobs[0].JobID != "job_a" {
		t.Fatalf("discovered jobs = %+v", res.Jobs)
	}
	if res.Empty {
		t.Error("mirror present, should not be empty")
	}
	evs := res.Events["job_a"]
	if len(evs) != 2 {
		t.Fatalf("events = %d, want 2 (partial final line and malformed skipped)", len(evs))
	}
	if evs[0].Seq != 1 || evs[1].Seq != 2 {
		t.Errorf("event seqs = %d/%d", evs[0].Seq, evs[1].Seq)
	}
	pts := res.Points[SeriesKey{JobID: "job_a", Metric: "loss"}]
	if len(pts) != 1 || pts[0].X != 10 {
		t.Errorf("loss points = %+v (x should be 10 from _step)", pts)
	}
	if res.Malformed < 1 {
		t.Error("malformed line should be counted")
	}
}

func TestJSONLMalformedWarningsBounded(t *testing.T) {
	home := t.TempDir()
	eventsDir := filepath.Join(home, "events")
	if err := os.MkdirAll(eventsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for i := 0; i < 50; i++ {
		b.WriteString("garbage-line\n")
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "job_x.jsonl"), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(home)
	cfg.EventsDir = eventsDir
	q := NewQuerier(cfg)
	res := q.Refresh(context.Background(), RefreshRequest{Targets: []string{"job_x"}, LastSeq: map[string]int64{}})
	if res.Malformed > maxMalformedWarnings {
		t.Errorf("malformed warnings = %d, want bounded at %d", res.Malformed, maxMalformedWarnings)
	}
}

func TestSQLiteBusyIsStaleNotCrash(t *testing.T) {
	dbPath := fixtureCopy(t)
	cfg := testCfgFor(dbPath)
	q := NewQuerier(cfg)
	defer q.Close()

	// Switch the writer to rollback-journal mode so an EXCLUSIVE transaction
	// blocks readers (WAL readers are never blocked by writers).
	writer, err := state.Open(dbPath)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	if _, err := writer.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		t.Fatalf("journal_mode delete: %v", err)
	}
	if _, err := writer.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("begin exclusive: %v", err)
	}
	defer func() {
		_, _ = writer.Exec("ROLLBACK")
	}()

	res := q.Refresh(context.Background(), RefreshRequest{
		Targets: []string{"job_completed"},
		LastSeq: map[string]int64{},
	})
	if !res.Stale {
		t.Errorf("busy refresh should be stale, got flags: %+v", res)
	}
	// A busy refresh must not crash and must not fabricate an empty state.
	if res.Empty {
		t.Error("busy refresh should not report empty")
	}
}

func TestBusyTimeoutClassification(t *testing.T) {
	if !isBusy(errF("sqlite: database is locked")) {
		t.Error("locked should classify busy")
	}
	if !isBusy(errF("sqlite: database table is locked")) {
		t.Error("table locked should classify busy")
	}
	if !isBusy(errF("context deadline exceeded")) {
		t.Error("deadline should classify busy (treat as stale retry)")
	}
	if isBusy(errF("no such table: jobs")) {
		t.Error("schema errors must not be treated as busy")
	}
}

func TestReadLogTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job.log")
	lines := []string{"l1", "l2", "l3", "l4", "l5"}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tail := ReadLogTail(path, 1<<20, 3)
	if len(tail.Lines) != 3 || tail.Lines[0] != "l3" {
		t.Fatalf("tail = %v", tail.Lines)
	}
	if tail.Err != nil {
		t.Fatalf("tail err: %v", tail.Err)
	}

	// Byte-bounded tail drops a leading partial line.
	if err := os.WriteFile(path, []byte("start\n"+strings.Repeat("x", 512)+"\nEND"), 0o600); err != nil {
		t.Fatal(err)
	}
	tail = ReadLogTail(path, 64, 10)
	if len(tail.Lines) == 0 || tail.Lines[len(tail.Lines)-1] != "END" {
		t.Fatalf("byte-bounded tail = %v", tail.Lines)
	}
	for _, l := range tail.Lines {
		if strings.Contains(l, "start") {
			t.Error("partial leading line should be dropped")
		}
	}

	// Missing file -> empty, no error.
	if tail := ReadLogTail(filepath.Join(dir, "nope.log"), 64, 10); tail.Err != nil || len(tail.Lines) != 0 {
		t.Errorf("missing file tail = %+v", tail)
	}
}

func TestResolvePath(t *testing.T) {
	cfg := DefaultConfig(`C:\home\vanth`)
	if got := cfg.resolvePath("logs/job.log"); got != `C:\home\vanth\logs\job.log` {
		t.Errorf("relative resolve = %q", got)
	}
	if got := cfg.resolvePath(`C:\abs\job.log`); got != `C:\abs\job.log` {
		t.Errorf("absolute resolve = %q", got)
	}
	if got := cfg.resolvePath(""); got != "" {
		t.Errorf("empty resolve = %q", got)
	}
}

func TestCompactJSON(t *testing.T) {
	if CompactJSON(map[string]any{"a": 1.0, "b": "x"}) == "" {
		t.Error("compact json empty")
	}
	if CompactJSON(nil) != "" {
		t.Error("nil data should render empty")
	}
}

func errF(s string) error {
	return &testErr{s}
}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }
