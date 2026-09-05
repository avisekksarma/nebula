# Nebula

This repo is where I build distributed systems from scratch, one project at a time. The root is not an app. It is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): each project under `projects/` has its own package and dependencies, and they share one lockfile.

Python 3.13+ and [uv](https://docs.astral.sh/uv/). From this directory:

```bash
uv sync --package quasar
```

## Quasar

[Quasar](projects/quasar) is a three-node replicated key-value store. The nodes elect a leader, copy writes through an ordered log, and only apply a write to the map once a majority has it. State is in memory; a restart comes back empty until the leader sends what that node missed. If two logs diverge at the same index, the leader walks back until they share a prefix and the follower replaces the rest.

Each node is the same FastAPI process. You can run three terminals, or `quasar-lab` — a local page that talks to that same cluster.

How to run it, the API, and why the log exists are in the [Quasar README](projects/quasar/README.md).

## Layout

```
nebula/
├── pyproject.toml    # workspace only
├── uv.lock
└── projects/
    └── quasar/
```

A later project is another folder under `projects/` with its own `pyproject.toml`.
