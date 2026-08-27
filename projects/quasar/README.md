# Quasar

A distributed, replicated key-value store. Building Raft in small steps.

## Current step: become candidate

Leader is still `--leader A`. If a follower misses heartbeats for a random ~0.8–1.6s, it becomes a **candidate**. It does not win yet (no votes).

```bash
PEERS='A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003'

uv run --package quasar quasar --node-id A --port 8001 --peers "$PEERS" --leader A
uv run --package quasar quasar --node-id B --port 8002 --peers "$PEERS" --leader A
uv run --package quasar quasar --node-id C --port 8003 --peers "$PEERS" --leader A
```

```bash
curl http://127.0.0.1:8002/health
# Ctrl+C node A, wait ~1–2s
# B and C terminals: "candidate (leader A heartbeat lost)"
curl http://127.0.0.1:8002/health   # role=candidate, leader still A
# PUT to B still 409 — candidate is not leader
```
