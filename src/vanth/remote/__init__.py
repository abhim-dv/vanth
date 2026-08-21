"""Remote execution contract and harness (Phases 0-2).

This package grows the SSH pairing, helper, and durable remote-execution
machinery described in ``docs/spec/remote-protocol-v1.md`` and the remote
execution plan. Phase 0 ships the wire protocol codec, canonical JSON
serializer, error registry, and the controller/remote-side sqlite stores.
Phase 1 adds secure SSH pairing (``pairing.py``, ``ssh.py``) and the
forced-command helper (``helper.py``). Phase 2 adds durable controller-side
request execution (``control.py``) and the remote daemon-side accepted
operations + dispatch loop (``remote.py``).
"""
