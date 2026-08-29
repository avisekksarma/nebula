# Quasar

A distributed, replicated key-value store. Building Raft in small steps.

## Current step: follower catch-up (shorter log only)

If a follower's log is a prefix of the leader's, the leader sends the missing suffix on heartbeat (and on write if a gap is detected). No conflict resolution yet.

After election, write some keys, kill a follower, write more, restart it, wait ~1s, then `GET /log` and `GET /kv/...` on that node should match the leader.

```bash
PEERS='A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003'

uv run --package quasar quasar --node-id A --port 8001 --peers "$PEERS"
uv run --package quasar quasar --node-id B --port 8002 --peers "$PEERS"
uv run --package quasar quasar --node-id C --port 8003 --peers "$PEERS"
```

Wait ~2s, find the leader via `/health`, PUT to that port, then check `/log` (`commit_index`) and `/kv/x` on all live nodes.
