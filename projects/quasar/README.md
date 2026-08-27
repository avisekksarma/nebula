# Quasar

A distributed, replicated key-value store. Building Raft in small steps.

## Current step: election complete (no Raft log yet)

No `--leader`. Every node starts as a follower; they elect one. A higher term always steps you down.

```bash
PEERS='A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003'

uv run --package quasar quasar --node-id A --port 8001 --peers "$PEERS"
uv run --package quasar quasar --node-id B --port 8002 --peers "$PEERS"
uv run --package quasar quasar --node-id C --port 8003 --peers "$PEERS"
```

Wait ~2s for the first election, then `curl .../health` on all three. Kill the leader, wait, confirm a new one. Restart the old node: it should come back as a **follower**, not a second leader.
