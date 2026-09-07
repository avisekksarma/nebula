# Quasar

Quasar is a replicated key-value store. A cluster of nodes elects a leader, replicates an ordered log, and applies committed entries to a local key-value map.

Each node is the same FastAPI binary. Cluster membership is passed on the command line (`--peers`). The Raft log is an append-only WAL (`data/<node-id>/log.jsonl`). Term and vote are in `data/<node-id>/raft.json`. Commit index and the KV map are still in memory.

## Why a log, not just the map

Copying `x=10` onto every node is enough while everyone is up. It is not enough to agree on **order** or on **which writes are durable** after a failure.

The **log** is the history of operations (`PUT` / `DELETE`) with a monotonic `index` and the **term** in which they were proposed. The **map** is derived from that history: only entries that have been **committed** are applied. A client `GET` reads the map, not the uncommitted tail of the log.

So a write is not “update the dict and hope replicas match.” It is:

1. Record the operation on the leader’s log.
2. Copy that log entry to the others.
3. When a majority of the cluster has the entry, mark it committed.
4. Apply committed entries, in index order, to each node’s map.

A node that was down can be sent the log suffix it is missing and then apply up to `commit_index`, instead of being handed an opaque snapshot of the map with no notion of sequence.

## Consensus in brief

The cluster has three voting members; **majority is two**. The leader counts itself plus each follower that acknowledged the entry. Leader + one live follower is enough to commit; one dead node does not stall the cluster. Two dead nodes cannot form a majority, so those writes stay uncommitted and are not applied.

**Terms** increase with each election. A node that hears a higher term steps down (leader or candidate becomes follower). That prevents a stale leader from overriding a newer one after it reconnects.

**Roles:** follower, candidate, leader. Everyone starts as a follower. The leader heartbeats every 250ms. If a follower hears nothing for a randomized 0.8–1.6s, it becomes a candidate: it increments its term, votes for itself, and asks the others. A node grants at most one vote per term, and only if the candidate’s log is at least as up-to-date. Two votes win.

Writes go only to the current leader. Followers answer `PUT`/`DELETE` with **409** and the leader’s URL.

## Replication and catch-up

New entries are pushed with `POST /internal/append`. Each request carries `prev_log_index` and `prev_log_term`. The follower accepts only if it has that exact `(index, term)` prefix. A missing index or a different **entry term** at that index is a reject.

The leader tracks `next_index` per follower (initialized to `last_log_index + 1`). On reject it decrements `next_index` by one and retries until the prefix matches. Then, if an incoming entry collides at the same index with a different term, the follower deletes that entry and everything after it and appends the leader’s suffix. Committed entries are never deleted. Empty `entries` is a heartbeat: same prefix check, but a matching prefix does not strip a longer uncommitted tail.

Followers set `commit_index = min(leader_commit, last log index)` and apply newly committed entries in order.

A process restart reloads the log, term, and vote from disk. Commit index and the KV map start empty. The leader’s `leader_commit` then applies only committed entries (the WAL tail is not replayed). `POST /snapshot` writes `data/<node-id>/snapshot.json` from the applied map, then drops WAL entries through `last_included_index`. Restart from snapshot is not implemented yet.

## Architecture

A client `PUT` is not applied to the map until a majority of the cluster has the log entry.

```mermaid
flowchart TB
    Client(["Client"]) -->|"PUT /kv/x = 10"| Leader

    Leader -->|"1. Append entry to own log"| LeaderLog["Leader Log"]

    LeaderLog -->|"2. Replicate entry"| A["A<br/>follower"]
    LeaderLog --> B["B<br/>follower"]
    LeaderLog --> C["C<br/>follower"]

    A --> Majority["3. Majority has entry"]
    B --> Majority
    C --> Majority

    Majority --> Commit["Leader advances<br/>commit_index"]
    Commit -->|"4. Apply committed"| KV["Leader KV map"]
    KV -->|"5. Tell followers<br/>leader_commit"| Applied["Followers apply<br/>committed entries"]
```

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

From the repository root:

```bash
uv sync --package quasar
```

## Run

Three processes, one terminal each, from the repository root:

```bash
PEERS='A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003'

uv run --package quasar quasar --node-id A --port 8001 --peers "$PEERS"
uv run --package quasar quasar --node-id B --port 8002 --peers "$PEERS"
uv run --package quasar quasar --node-id C --port 8003 --peers "$PEERS"
```

Wait about two seconds for election. Stdout shows `candidate` / `granted vote` / `leader`.

| Flag | Default | Description |
| --- | --- | --- |
| `--node-id` | `A` | This process’s id |
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `8000` | Bind port |
| `--peers` | (empty) | All nodes as `id=url`, comma-separated. Include this node; it is skipped locally. |
| `--data-dir` | `data/<node-id>` | This node’s WAL directory. A, B, and C must not share one. |

## Interactive lab

The lab is a website on top of the **same** three-node cluster. It does not implement another KV store or another election. `quasar-lab` starts the real `quasar` processes (A, B, C on 8001–8003) and a site on port 9000.

```bash
uv run --package quasar quasar-lab
```

Open [http://127.0.0.1:9000](http://127.0.0.1:9000). Do not also start the three `quasar` commands in the Run section — they bind the same ports.

What used to be curl and extra terminals is on the page:

| On the site | Same as |
| --- | --- |
| Three cards (role, term, log, KV map) | `curl /health` and `/log` on each port |
| PUT / GET / DELETE | `curl` to `/kv/{key}` (leader, or a node you pick — followers still return **409**) |
| Pause / Resume | Stop a process, then bring it back with its RAM still there |
| Restart | Kill and start again — empty log and map until catch-up |
| Event list | The `candidate` / `granted vote` / `leader` / `apply` lines in stdout |

Guided buttons chain those same actions:

1. **Watch an election** — all three start as followers; a candidate needs two votes to become leader.
2. **A write is a log entry** — PUT, then watch append → replicate → majority commit → apply to the map. GET reads the map, not the uncommitted tail of the log.
3. **Majority of two** — pause one follower (write still commits); pause two (entry stays on the log but is not applied).
4. **Kill the leader, then catch-up** — pause a follower, write, resume (it gets the missing log suffix); pause the leader and wait for a new election.

You can still curl `8001`–`8003` in another terminal. That is the same cluster; the site refreshes on its own. Writes still have to go to whoever `/health` reports as `leader`.

`--host` and `--port` default to `127.0.0.1` and `9000`. Local only; not a hosted multi-user service.

## API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | `role`, `term`, `leader`, `commit_index` |
| `GET` | `/log` | Log entries and `commit_index` |
| `PUT` | `/kv/{key}` | JSON body `{"value": "..."}` |
| `GET` | `/kv/{key}` | Read committed value |
| `DELETE` | `/kv/{key}` | Delete a committed key |
| `POST` | `/snapshot` | Write a local snapshot of the applied KV map, then drop that prefix from the WAL |

Cluster RPC: `POST /internal/heartbeat`, `/internal/vote`, `/internal/append`.

## Examples

Discover the leader:

```bash
for p in 8001 8002 8003; do echo -n "$p "; curl -s http://127.0.0.1:$p/health; echo; done
```

Write and read (leader on `8001`):

```bash
curl -s -X PUT http://127.0.0.1:8001/kv/x \
  -H 'Content-Type: application/json' \
  -d '{"value":"10"}'

curl -s http://127.0.0.1:8001/kv/x
curl -s http://127.0.0.1:8002/kv/x
curl -s http://127.0.0.1:8003/log
```

With one follower stopped, writes to the leader still commit. If the leader is stopped, wait for a new election and write to the node `/health` reports as `leader`.

After restarting a follower, wait one or two seconds, then compare `/log` and `/kv/...` with the leader.

## Layout

```
projects/quasar/
├── README.md
├── pyproject.toml
└── src/quasar/
    ├── __init__.py    # node CLI
    ├── __main__.py
    ├── app.py         # cluster / KV backend
    └── lab/           # interactive website (not part of the node)
        ├── __init__.py
        ├── server.py
        └── static/
```
