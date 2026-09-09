"""Lab HTTP layer: launch real Quasar nodes and expose them to the browser."""

from __future__ import annotations

import asyncio
import json
import urllib.error
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from quasar.lab.cluster import (
    NODE_IDS,
    Cluster,
    http_json,
    node_url,
)

_STATIC = Path(__file__).resolve().parent / "static"
_POLL_S = 0.2
cluster = Cluster()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    await asyncio.to_thread(cluster.start_all)
    try:
        yield
    finally:
        await asyncio.to_thread(cluster.stop_all)


app = FastAPI(
    title="Quasar lab",
    description="Visual debugger over a live 3-node Quasar cluster.",
    version="0.2.0",
    lifespan=_lifespan,
)


class PutBody(BaseModel):
    value: str


def _require_node(node_id: str) -> None:
    if node_id not in NODE_IDS:
        raise HTTPException(status_code=404, detail=f"unknown node {node_id}")


def _target(node: str | None, write: bool) -> str:
    if node:
        _require_node(node)
        if cluster.is_paused(node):
            raise HTTPException(status_code=503, detail=f"{node} is paused")
        return node
    leader = cluster.find_leader()
    if leader:
        return leader
    if write:
        raise HTTPException(status_code=503, detail="no leader")
    for nid in NODE_IDS:
        if not cluster.is_paused(nid) and cluster.is_running(nid):
            return nid
    raise HTTPException(status_code=503, detail="no reachable node")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


@app.api_route("/p/{node_id}/{rest:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def peer_proxy(node_id: str, rest: str, request: Request) -> JSONResponse:
    """Fail-fast when a node is paused so peer RPC does not hang on SIGSTOP."""
    _require_node(node_id)
    if cluster.is_paused(node_id) or not cluster.is_running(node_id):
        raise HTTPException(status_code=502, detail=f"{node_id} unreachable")
    raw = await request.body()
    payload = json.loads(raw) if raw else None
    try:
        status, body = await asyncio.to_thread(
            http_json, request.method, f"{node_url(node_id)}/{rest}", payload, 1.0
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return JSONResponse(body, status_code=status)


@app.get("/cluster/snapshot")
def cluster_snapshot() -> dict[str, object]:
    return cluster.snapshot()


@app.get("/cluster/stream")
async def cluster_stream() -> StreamingResponse:
    async def gen():
        while True:
            snap = await asyncio.to_thread(cluster.snapshot)
            yield f"data: {json.dumps(snap)}\n\n"
            await asyncio.sleep(_POLL_S)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _kv_wrap(served_by: str, status: int, body: dict) -> JSONResponse:
    return JSONResponse({"served_by": served_by, "status": status, "body": body})


@app.put("/cluster/kv/{key}")
def put_kv(key: str, body: PutBody, node: str | None = None) -> JSONResponse:
    target = _target(node, write=True)
    try:
        status, payload = http_json(
            "PUT",
            f"{node_url(target)}/kv/{key}",
            {"value": body.value},
            timeout=5.0,
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _kv_wrap(target, status, payload)


@app.get("/cluster/kv/{key}")
def get_kv(key: str, node: str | None = None) -> JSONResponse:
    target = _target(node, write=True)
    try:
        status, payload = http_json(
            "GET", f"{node_url(target)}/kv/{key}", timeout=2.0
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _kv_wrap(target, status, payload)


@app.post("/cluster/nodes/{node_id}/pause")
def pause_node(node_id: str) -> dict[str, object]:
    _require_node(node_id)
    try:
        cluster.pause(node_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"node": node_id, "paused": True}


@app.post("/cluster/nodes/{node_id}/resume")
def resume_node(node_id: str) -> dict[str, object]:
    _require_node(node_id)
    try:
        cluster.resume(node_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"node": node_id, "paused": False}


@app.post("/cluster/nodes/{node_id}/restart")
def restart_node(node_id: str) -> dict[str, object]:
    _require_node(node_id)
    try:
        cluster.restart(node_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"node": node_id, "restarted": True}


@app.post("/cluster/reset")
def reset_cluster() -> dict[str, object]:
    try:
        cluster.reset()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"reset": True}


if _STATIC.is_dir():
    app.mount("/assets", StaticFiles(directory=_STATIC), name="assets")
