"""Controller-side remote wake shadow synchronization."""

from __future__ import annotations

from vanth.daemon import _remote_wake_sync_once
from vanth.remote.protocol import VanthRemoteProtocolError


class FakeManager:
    def __init__(self, bindings):
        self.bindings = bindings
        self.emitted = []
        self.dropped = []

    def remote_wake_bindings(self):
        return self.bindings

    def remote_event_cursor(self, remote_id, job_id):
        return None

    def set_remote_event_cursor(self, remote_id, job_id, next_seq):
        pass

    def drop_remote_wake_binding(self, remote_id, job_id):
        self.dropped.append((remote_id, job_id))
        return True

    def emit_remote_event(self, remote_id, job_id, event, next_seq):
        self.emitted.append(("event", remote_id, job_id, event["type"]))
        return event

    def emit_remote_terminal(self, remote_id, job_id, status, *, exit_code=None):
        key = (remote_id, job_id)
        if any(item[:2] == key for item in self.emitted):
            return None
        event = {"remote_id": remote_id, "remote_job_id": job_id, "status": status, "exit_code": exit_code}
        self.emitted.append((remote_id, job_id, status, exit_code))
        return event


class FakeControl:
    def __init__(self, store, failure=None, events=None, events_failure=None):
        self.store = store
        self.failure = failure
        self.events_response = events
        self.events_failure = events_failure
        self.synced = []
        self.events_calls = 0

    def feed_sync(self, remote_id):
        self.synced.append(remote_id)
        if self.failure:
            raise self.failure

    def events(self, remote_id, cursors, *, limit=200):
        self.events_calls += 1
        if self.events_failure:
            raise self.events_failure
        return self.events_response or {"jobs": {}}


class FakeStore:
    def __init__(self, shadows):
        self.shadows = shadows

    def get_shadow(self, remote_id, job_id):
        return self.shadows[(remote_id, job_id)]


def binding(job_id="job-1", events=("completed",)):
    return {
        "remote_id": "host", "remote_job_id": job_id, "binding_id": "binding-1",
        "target_id": "target-1", "events": list(events),
    }


def test_terminal_shadow_emits_once_and_dedupes():
    manager = FakeManager([binding()])
    control = FakeControl(None)
    store = FakeStore({("host", "job-1"): {"status": "completed", "payload": {"exit_code": 0}}})

    _remote_wake_sync_once(manager, control, store)
    _remote_wake_sync_once(manager, control, store)

    assert control.synced == ["host", "host"]
    assert manager.emitted == [("host", "job-1", "completed", 0)]
    # A terminal-only binding never needs the event read.
    assert control.events_calls == 0


def test_non_terminal_shadow_does_not_emit():
    manager = FakeManager([binding()])
    control = FakeControl(None)
    store = FakeStore({("host", "job-1"): {"status": "running", "payload": {}}})

    _remote_wake_sync_once(manager, control, store)

    assert manager.emitted == []


def test_no_bindings_skips_feed_sync():
    manager = FakeManager([])
    control = FakeControl(None)

    _remote_wake_sync_once(manager, control, FakeStore({}))

    assert control.synced == []


def test_feed_failure_does_not_abort_shadow_inspection():
    manager = FakeManager([binding()])
    control = FakeControl(None, RuntimeError("temporary SSH failure"))
    store = FakeStore({("host", "job-1"): {"status": "failed", "payload": {"exit_code": 1}}})

    _remote_wake_sync_once(manager, control, store)

    assert manager.emitted == [("host", "job-1", "failed", 1)]


def test_event_read_failure_withholds_terminal_for_non_terminal_binding():
    """A checkpoint-target binding must not lose undrained events: a failed event
    read fails SAFE and the terminal waits for a later tick."""
    manager = FakeManager([binding(events=("checkpoint", "completed"))])
    control = FakeControl(None, events_failure=RuntimeError("transient"))
    store = FakeStore({("host", "job-1"): {"status": "completed", "payload": {"exit_code": 0}}})

    _remote_wake_sync_once(manager, control, store)

    assert manager.emitted == []


def test_unsupported_events_degrades_to_terminal_only():
    """An older host without `job.events` must not block the terminal wake."""
    manager = FakeManager([binding(events=("checkpoint", "completed"))])
    control = FakeControl(
        None, events_failure=VanthRemoteProtocolError("UNSUPPORTED_FEATURE", "no job.events")
    )
    store = FakeStore({("host", "job-1"): {"status": "completed", "payload": {"exit_code": 0}}})

    _remote_wake_sync_once(manager, control, store)

    assert manager.emitted == [("host", "job-1", "completed", 0)]


def test_undrained_events_withhold_terminal():
    manager = FakeManager([binding(events=("checkpoint", "completed"))])
    control = FakeControl(None, events={"jobs": {"job-1": {
        "events": [], "next_seq": 3, "has_more": True,
    }}})
    store = FakeStore({("host", "job-1"): {"status": "completed", "payload": {"exit_code": 0}}})

    _remote_wake_sync_once(manager, control, store)

    assert manager.emitted == []


def test_missing_shadow_settles_binding_without_traceback():
    """A deleted/forgotten remote job has no shadow; the binding must be settled
    rather than re-polled forever."""
    manager = FakeManager([binding()])

    class MissingStore:
        def get_shadow(self, remote_id, job_id):
            raise ValueError("no shadow")

    _remote_wake_sync_once(manager, FakeControl(None), MissingStore())

    assert manager.dropped == [("host", "job-1")]
    assert manager.emitted == []
