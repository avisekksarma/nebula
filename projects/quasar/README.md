# Quasar

A three-node replicated key-value store in Python, built from scratch as a way to learn Raft.

There is no etcd, no database, and no Raft library. Each node is the same FastAPI process with an in-memory log and an in-memory dictionary. We added one idea at a time: a local KV, then multiple processes, then RPC, then election, then a log, then majority commit, then catch-up for a follower whose log is only *shorter*.

**Not production software.** Logs and state live in RAM and vanish when the process dies. Log *conflicts* (diverged histories) are not handled yet.

## How it works

```
                    Client
                       |
              PUT /kv/x  {value: 10}
                       |
                       v
                 Leader (e.g. B)
                       |
            1. append to B's log
            2. POST /internal/append → A, C
            3. if ≥ 2 nodes have the entry
               → commit_index advances
            4. apply committed entries to B's dict
            5. tell followers the new commit_index
                       |
              /        |        \
             v         v         v
            A          B          C
         log + dict  log + dict  log + dict
```

- **Log** — ordered history: `{index, term, operation, key, value?}`. Index is 1-based.
- **Dict** — the state machine. It is updated only from *committed* log entries, not from a raw PUT.
- **Majority** — cluster size 3, so 2 nodes. Leader + one follower is enough to commit. One dead node does not block writes.
- **Leader election** — every node starts as a follower. Heartbeats every 250ms; a random timeout (~0.8–1.6s) with no heartbeat starts an election. One vote per term; majority wins. A higher term makes you step down.
- **Catch-up (prefix only)** — if a follower reports `last_log_index < leader_len`, the leader sends the missing suffix. A restarted follower with an empty log will fill in. Diverged logs are **not** repaired.

## What is in vs not in

| In | Not yet |
| --- | --- |
| Leader election (term, votes, heartbeats) | Persistence / WAL |
| Replicated in-memory log | Snapshots |
| Majority `commit_index` + apply | Log conflict / `nextIndex` rewind |
| Prefix catch-up for a shorter log | Membership changes |
| Client writes only on the current leader | Production RPC, auth, disk |

## Requirements

- Python **3.13+**
- [uv](https://docs.astral.sh/uv/) (this repo is a uv workspace)

From the **nebula repo root**:

```bash
uv sync --package quasar
```

## Run a three-node cluster

Three terminals, same working directory (repo root):

```bash
PEERS='A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003'

uv run --package quasar quasar --node-id A --port 8001 --peers "$PEERS"
uv run --package quasar quasar --node-id B --port 8002 --peers "$PEERS"
uv run --package quasar quasar --node-id C --port 8003 --peers "$PEERS"
```

There is no `--leader`. Wait about two seconds for the first election. Node processes print `candidate` / `granted vote` / `leader`.

Flags: `--node-id`, `--host` (default `127.0.0.1`), `--port`, `--peers` (`id=url` pairs, comma-separated). Include **all** nodes in `--peers`, including self; each process skips itself.

Uvicorn access logs are off so election and log lines stay readable.

## HTTP API

Client-facing (use the **leader** for writes):

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | `role`, `term`, `leader`, `commit_index` |
| `GET` | `/` | Same, plus key count |
| `GET` | `/log` | Full in-memory log + `commit_index` |
| `PUT` | `/kv/{key}` | Body `{"value":"..."}` — append, replicate, commit |
| `GET` | `/kv/{key}` | Read committed state |
| `DELETE` | `/kv/{key}` | Append a DELETE, then commit/apply |

Internal (nodes talking to each other): `/internal/heartbeat`, `/internal/vote`, `/internal/append`, plus leftover ping `/internal/message`.

A write to a follower returns **409** with `leader` and `leader_url`.

## Try it

Find the leader:

```bash
for p in 8001 8002 8003; do echo -n "$p "; curl -s http://127.0.0.1:$p/health; echo; done
```

If A is leader (`8001`):

```bash
curl -s -X PUT http://127.0.0.1:8001/kv/x -H 'Content-Type: application/json' -d '{"value":"10"}'
curl -s http://127.0.0.1:8001/kv/x
curl -s http://127.0.0.1:8002/kv/x
curl -s http://127.0.0.1:8003/log
```

All live nodes should show `x=10` and the same log / `commit_index`.

### Majority with one node down

Ctrl+C one **follower**. PUT another key on the leader. The remaining two should still commit (`2/3`).

Ctrl+C the **leader** instead. Wait ~2s for a new election, then PUT on whoever `/health` reports as leader.

### Catch-up after restart

1. Write a few keys on the leader.
2. Kill one follower.
3. Write more keys (they still commit).
4. Restart that follower with the same command.
5. Wait ~1–2 seconds, then `GET /log` and `GET /kv/...` on it — they should match the leader.

The leader process prints `catch-up ... sending indexes ...` when it fills a shorter log.

## Layout

```
projects/quasar/
├── README.md
├── pyproject.toml
├── skills.md                 # notes for the learning process
└── src/quasar/
    ├── __init__.py           # CLI → uvicorn
    ├── __main__.py
    └── app.py                # KV, election, log, commit, catch-up
```

Almost all behavior is in `app.py` on purpose: one file, easy to read in order.

## Status

This matches a teaching subset of Raft: election, log, commit/apply, prefix catch-up. Next natural gap is **conflicting logs** (a follower whose history is not a prefix of the leader), then persistence if you want a crash to survive.
