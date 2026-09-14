# Nebula

This repo is where I build distributed systems from scratch, one project at a time. The root is not an app. It is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): each project under `projects/` has its own package and dependencies, and they share one lockfile.

Python 3.13+ and [uv](https://docs.astral.sh/uv/). From this directory:

```bash
uv sync --package quasar
```

## Quasar

[Quasar](projects/quasar) is a three-node Raft-style key-value store. The nodes elect a leader, copy `PUT`/`DELETE` through an ordered log, and apply a write to the map only when a majority has it.

What it covers:

- **Elections and terms** — follower / candidate / leader; majority is two; a higher term forces an old leader to step down
- **Replication** — prefix checks, `next_index` catch-up, conflict repair on an uncommitted suffix
- **Persistence** — per-node WAL and term/vote on disk; restart reloads that state
- **Snapshots** — compact old log locally; a follower behind the snapshot gets the snapshot, then the leftover log
- **Linearizable reads** — `GET` is leader-only, and only after the leader confirms it still has a majority

Each node is the same process. Run three terminals, or `quasar-lab` (a local page on the same cluster). How to run it and the API are in the [Quasar README](projects/quasar/README.md).

## Layout

```
nebula/
├── pyproject.toml    # workspace only
├── uv.lock
└── projects/
    └── quasar/
```

A later project is another folder under `projects/` with its own `pyproject.toml`.
