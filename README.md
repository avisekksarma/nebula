# Nebula

A collection of distributed systems projects. Each project lives in its own folder under `projects/`, with its own `pyproject.toml` and dependencies. The repo root is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) so they share one lockfile and one virtualenv.

## Layout

```
nebula/
├── pyproject.toml          # workspace root (not an app)
├── uv.lock
└── projects/
    └── quasar/             # replicated KV store
```

Add a new project with:

```bash
uv init --package projects/<name>
```

Then put its code in `projects/<name>/src/<name>/`.

## Projects

| Project | What it is |
| --- | --- |
| [quasar](projects/quasar) | Distributed, replicated key-value store |
