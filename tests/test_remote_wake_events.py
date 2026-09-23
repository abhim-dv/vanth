import sys

import shellcmd

import vanth.daemon as daemon
from vanth.server import JobManager


def command(code="pass"):
    return shellcmd.join([sys.executable, "-c", code])


def binding(manager, events=("checkpoint",)):
    return manager.register_remote_wake_targets(
        "host-a", "job-1", [{"type": "local_command", "events": list(events), "command": command()}]
    )[0]


def test_remote_event_advances_cursor_and_deduplicates(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        target_id = binding(manager)
        event = {"seq": 1, "type": "checkpoint", "message": "saved", "data_json": '{"step": 2}'}
        assert manager.emit_remote_event("host-a", "job-1", event, 1)
        assert manager.remote_event_cursor("host-a", "job-1") == 1
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE target_id=?", (target_id,)).fetchone()[0] == 1
        assert manager.emit_remote_event("host-a", "job-1", event, 1) is None
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE target_id=?", (target_id,)).fetchone()[0] == 1
    finally:
        manager.close()


def test_remote_event_cursor_initialization_skips_history(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        target_id = binding(manager)
        manager.set_remote_event_cursor("host-a", "job-1", 4)
        assert manager.emit_remote_event(
            "host-a", "job-1", {"seq": 5, "type": "checkpoint", "data_json": "{}"}, 5
        )
        assert manager.remote_event_cursor("host-a", "job-1") == 5
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE target_id=?", (target_id,)).fetchone()[0] == 1
    finally:
        manager.close()


def test_terminal_removes_cursor(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        binding(manager)
        manager.set_remote_event_cursor("host-a", "job-1", 4)
        manager.emit_remote_terminal("host-a", "job-1", "completed")
        assert manager.remote_event_cursor("host-a", "job-1") is None
        assert not manager.db.execute("SELECT 1 FROM wake_targets WHERE job_id='remote:host-a:job-1'").fetchone()
    finally:
        manager.close()


def test_sync_initializes_without_replaying_history(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        binding(manager)

        class Control:
            def feed_sync(self, remote_id):
                pass

            def events(self, remote_id, cursors):
                return {"jobs": {"job-1": {"events": [], "next_seq": 9, "has_more": False}}}

        class Store:
            def get_shadow(self, remote_id, job_id):
                return {"status": "running"}

        daemon._remote_wake_sync_once(manager, Control(), Store())
        assert manager.remote_event_cursor("host-a", "job-1") == 9
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
    finally:
        manager.close()


def test_sync_emits_events_before_terminal(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        binding(manager, ("checkpoint", "completed"))
        manager.set_remote_event_cursor("host-a", "job-1", 0)

        class Control:
            def feed_sync(self, remote_id):
                pass

            def events(self, remote_id, cursors):
                return {"jobs": {"job-1": {
                    "events": [{"seq": 1, "type": "checkpoint", "message": "saved", "data_json": "{}"}],
                    "next_seq": 1, "has_more": False,
                }}}

        class Store:
            def get_shadow(self, remote_id, job_id):
                return {"status": "completed", "payload": {"exit_code": 0}}

        daemon._remote_wake_sync_once(manager, Control(), Store())
        rows = manager.db.execute("SELECT type FROM events WHERE job_id=? ORDER BY seq", ("remote:host-a:job-1",)).fetchall()
        assert [row["type"] for row in rows] == ["checkpoint", "completed"]
        assert manager.db.execute("SELECT COUNT(*) FROM events WHERE type='completed'").fetchone()[0] == 1
    finally:
        manager.close()


def test_terminal_waits_for_undrained_events(tmp_path):
    """A terminal must not overtake a backlog that did not drain within the
    per-tick page cap: the terminal deletes the binding, so an undrained
    checkpoint would be lost."""
    manager = JobManager(tmp_path / "state")
    try:
        binding(manager, ("checkpoint", "completed"))
        manager.set_remote_event_cursor("host-a", "job-1", 0)

        class Control:
            def feed_sync(self, remote_id):
                pass

            def events(self, remote_id, cursors):
                since = cursors.get("job-1") or 0
                seq = since + 1
                return {"jobs": {"job-1": {
                    "events": [{"seq": seq, "type": "checkpoint", "data_json": "{}"}],
                    "next_seq": seq, "has_more": True,
                }}}

        class Store:
            def get_shadow(self, remote_id, job_id):
                return {"status": "completed", "payload": {"exit_code": 0}}

        daemon._remote_wake_sync_once(manager, Control(), Store())
        assert manager.db.execute("SELECT COUNT(*) FROM events WHERE type='completed'").fetchone()[0] == 0
        assert manager.db.execute(
            "SELECT 1 FROM wake_targets WHERE job_id='remote:host-a:job-1'"
        ).fetchone() is not None
    finally:
        manager.close()


def test_event_cap_truncation_still_advances_cursor(tmp_path):
    """The per-job event cap drops events without running the mutate hook, so the
    cursor must be advanced explicitly — otherwise the backlog refetches the same
    page forever and the terminal wake is starved."""
    manager = JobManager(tmp_path / "state")
    try:
        manager.max_events_per_job = 1
        binding(manager)  # a checkpoint target
        assert manager.emit_remote_event("host-a", "job-1", {"seq": 1, "type": "checkpoint", "data_json": "{}"}, 1)
        # The second event exceeds the cap: dropped, but the cursor must move.
        assert manager.emit_remote_event("host-a", "job-1", {"seq": 2, "type": "checkpoint", "data_json": "{}"}, 2) is None
        assert manager.remote_event_cursor("host-a", "job-1") == 2
    finally:
        manager.close()


def test_drop_remote_wake_binding_settles_binding_and_cursor(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        binding(manager)
        manager.set_remote_event_cursor("host-a", "job-1", 3)
        assert manager.drop_remote_wake_binding("host-a", "job-1") is True
        assert manager.remote_event_cursor("host-a", "job-1") is None
        assert not manager.db.execute(
            "SELECT 1 FROM wake_targets WHERE job_id='remote:host-a:job-1'"
        ).fetchone()
        assert manager.drop_remote_wake_binding("host-a", "job-1") is False
    finally:
        manager.close()


def test_prune_removes_orphaned_cursor(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        manager.set_remote_event_cursor("host-a", "job-orphan", 2)
        manager._prune_remote_wake_rows(0)
        assert manager.remote_event_cursor("host-a", "job-orphan") is None
    finally:
        manager.close()
