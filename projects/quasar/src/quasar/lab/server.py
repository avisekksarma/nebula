"""Run a live 3-node Quasar cluster and serve the lab website.

Talks to nodes only through the public KV API (`/health`, `/log`, `/kv/...`)
and the cluster RPC the nodes already use. Consensus stays in `quasar.app`.
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

_NODE_IDS = ("A", "B", "C")
_HOST = "127.0.0.1"
_BASE_PORT = 8001
_POLL_S = 0.2
_STATIC = Path(__file__).resolve().parent / "static"

_lock = threading.Lock()
_timeline: deque[dict[str, object]] = deque(maxlen=200)
_event_seq = 0


@dataclass
class Node:
    node_id: str
    port: int
    process: subprocess.Popen[str] | None = None
    paused: bool = False
    voted_for: str | None = None
    last_state: dict[str, object] | None = None
    last_error: str | None = None
    reader: threading.Thread | None = field(default=None, repr=False)


_nodes: dict[str, Node] = {}


def _lab_port() -> int:
    return int(os.environ.get("QUASAR_LAB_PORT", "9000"))


def _port(node_id: str) -> int:
    return _BASE_PORT + _NODE_IDS.index(node_id)


def _url(node_id: str) -> str:
    return f"http://{_HOST}:{_port(node_id)}"


def _peers_arg() -> str:
    port = _lab_port()
    return ",".join(f"{nid}=http://{_HOST}:{port}/p/{nid}" for nid in _NODE_IDS)


def _http(
    method: str,
    url: str,
    payload: dict | None = None,
    timeout: float = 2.0,
) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            body = json.loads(raw) if raw else {}
            return int(resp.status), body
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"detail": raw}
        return int(exc.code), body


def _record(kind: str, node: str, **data: object) -> None:
    global _event_seq
    with _lock:
        _event_seq += 1
        _timeline.append(
            {
                "seq": _event_seq,
                "t": time.time(),
                "node": node,
                "kind": kind,
                **data,
            }
        )
        n = _nodes.get(node)
        if n is None:
            return
        if kind == "vote_granted":
            cand = data.get("candidate_id")
            n.voted_for = str(cand) if cand is not None else n.voted_for
        elif kind == "candidate":
            n.voted_for = node
        elif kind == "step_down":
            n.voted_for = None


def _peer_from_url(url: str) -> str:
    m = re.search(r"/p/([A-C])\b", url)
    if m:
        return m.group(1)
    for nid in _NODE_IDS:
        if url.rstrip("/").endswith(str(_port(nid))):
            return nid
    return url


def _parse_entry(raw: str) -> dict[str, object]:
    try:
        value = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        return {"raw": raw}
    return value if isinstance(value, dict) else {"raw": raw}


def _parse_line(line: str, default_node: str) -> None:
    line = line.rstrip()
    if not line:
        return

    m = re.match(r"^([A-C]): apply (.+) \(commit_index=(\d+)\)$", line)
    if m:
        _record(
            "apply",
            m.group(1),
            entry=_parse_entry(m.group(2)),
            commit_index=int(m.group(3)),
        )
        return

    m = re.match(r"^([A-C]): log append (.+)$", line)
    if m:
        _record("log_append", m.group(1), entry=_parse_entry(m.group(2)))
        return

    m = re.match(
        r"^([A-C]): catch-up (\S+) sending indexes (\d+)-(\d+)$", line
    )
    if m:
        _record(
            "catch_up",
            m.group(1),
            peer=_peer_from_url(m.group(2)),
            from_index=int(m.group(3)),
            to_index=int(m.group(4)),
        )
        return

    m = re.match(r"^log replication to ([A-C]) failed: (.+)$", line)
    if m:
        _record("replicate_failed", default_node, peer=m.group(1))
        return

    m = re.match(
        r"^([A-C]): commit_index=(\d+) \(acks=(\d+)/(\d+)\)$", line
    )
    if m:
        _record(
            "commit",
            m.group(1),
            commit_index=int(m.group(2)),
            acks=int(m.group(3)),
            majority=int(m.group(4)),
        )
        return

    m = re.match(
        r"^([A-C]): stepping down (\w+) → follower \(term (\d+) → (\d+)\)$",
        line,
    )
    if m:
        _record(
            "step_down",
            m.group(1),
            from_role=m.group(2),
            term=int(m.group(3)),
            new_term=int(m.group(4)),
        )
        return

    m = re.match(r"^([A-C]): candidate term=(\d+) \(requesting votes\)$", line)
    if m:
        _record("candidate", m.group(1), term=int(m.group(2)))
        return

    m = re.match(r"^([A-C]): vote request to ([A-C]) failed: (.+)$", line)
    if m:
        _record("vote_request_failed", m.group(1), peer=m.group(2), term=None)
        return

    m = re.match(
        r"^([A-C]): got vote from ([A-C]) \((\d+)/(\d+)\)$", line
    )
    if m:
        _record(
            "vote_received",
            m.group(1),
            peer=m.group(2),
            votes=int(m.group(3)),
            majority=int(m.group(4)),
        )
        return

    m = re.match(r"^([A-C]): leader term=(\d+)$", line)
    if m:
        _record("leader", m.group(1), term=int(m.group(2)))
        return

    m = re.match(
        r"^([A-C]): skip gap index=(\d+) expected=(\d+)$", line
    )
    if m:
        _record(
            "skip_gap",
            m.group(1),
            index=int(m.group(2)),
            expected=int(m.group(3)),
        )
        return

    m = re.match(r"^([A-C]): granted vote to ([A-C]) term=(\d+)$", line)
    if m:
        _record(
            "vote_granted",
            m.group(1),
            candidate_id=m.group(2),
            term=int(m.group(3)),
        )
        return


def _read_output(node: Node) -> None:
    proc = node.process
    if proc is None or proc.stdout is None:
        return
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        _parse_line(line, node.node_id)


def _spawn(node: Node) -> None:
    cmd = [
        sys.executable,
        "-m",
        "quasar",
        "--node-id",
        node.node_id,
        "--host",
        _HOST,
        "--port",
        str(node.port),
        "--peers",
        _peers_arg(),
    ]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env.pop("QUASAR_DEBUG", None)
    node.process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    node.paused = False
    node.voted_for = None
    node.last_error = None
    node.reader = threading.Thread(target=_read_output, args=(node,), daemon=True)
    node.reader.start()


def _wait_ready(node: Node, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.process is not None and node.process.poll() is not None:
            node.last_error = f"exited {node.process.returncode}"
            return False
        try:
            status, body = _http("GET", f"{_url(node.node_id)}/health", timeout=0.3)
            if status == 200 and body.get("node") == node.node_id:
                return True
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(0.1)
    node.last_error = "did not become ready"
    return False


def _cont_if_paused(node: Node) -> None:
    if (
        node.paused
        and node.process is not None
        and node.process.poll() is None
        and node.process.pid is not None
    ):
        os.kill(node.process.pid, signal.SIGCONT)
        node.paused = False


def _stop_node(node: Node) -> None:
    proc = node.process
    if proc is None:
        return
    _cont_if_paused(node)
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=1)
    node.process = None
    node.paused = False


def _start_cluster() -> None:
    global _nodes, _event_seq
    with _lock:
        _timeline.clear()
        _event_seq = 0
        _nodes = {nid: Node(node_id=nid, port=_port(nid)) for nid in _NODE_IDS}
        to_start = list(_nodes.values())
    for node in to_start:
        _spawn(node)
    failed: list[str] = []
    for node in to_start:
        if not _wait_ready(node):
            failed.append(f"{node.node_id} ({node.last_error})")
    if failed:
        _stop_cluster()
        raise RuntimeError("nodes failed to start: " + ", ".join(failed))


def _stop_cluster() -> None:
    with _lock:
        nodes = list(_nodes.values())
    for node in nodes:
        _stop_node(node)


def _get_node(node_id: str) -> Node:
    node = _nodes.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"unknown node {node_id}")
    return node


def _store_from_log(entries: list, commit_index: int) -> dict[str, str]:
    store: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            idx = int(entry.get("index", 0))
        except (TypeError, ValueError):
            continue
        if idx > commit_index:
            continue
        op = str(entry.get("operation", ""))
        key = str(entry.get("key", ""))
        if op == "PUT":
            store[key] = str(entry.get("value", ""))
        elif op == "DELETE":
            store.pop(key, None)
    return store


def _poll_node(node: Node) -> dict[str, object]:
    with _lock:
        paused = node.paused
        dead = node.process is None or node.process.poll() is not None
        cached = node.last_state
        error = node.last_error
        voted_for = node.voted_for
    base = {
        "node": node.node_id,
        "port": node.port,
        "paused": paused,
        "reachable": False,
        "voted_for": voted_for,
    }
    if paused:
        out = dict(cached) if cached else {}
        out.update(base)
        out["reachable"] = False
        out["paused"] = True
        return out
    if dead:
        out = dict(cached) if cached else {}
        out.update(base)
        out["error"] = error or "not running"
        return out
    try:
        h_status, health = _http("GET", f"{_url(node.node_id)}/health", timeout=0.4)
        l_status, log_body = _http("GET", f"{_url(node.node_id)}/log", timeout=0.4)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        out = dict(cached) if cached else {}
        out.update(base)
        out["error"] = str(exc)
        return out
    if h_status != 200 or l_status != 200:
        out = dict(cached) if cached else {}
        out.update(base)
        out["error"] = f"http {h_status}/{l_status}"
        return out
    entries = list(log_body.get("entries") or [])
    commit_index = int(health.get("commit_index") or log_body.get("commit_index") or 0)
    merged: dict[str, object] = {
        **health,
        **base,
        "reachable": True,
        "paused": False,
        "log": entries,
        "commit_index": commit_index,
        "last_applied": commit_index,
        "store": _store_from_log(entries, commit_index),
        "voted_for": voted_for,
    }
    with _lock:
        node.last_state = merged
        node.last_error = None
    return merged


def _snapshot() -> dict[str, object]:
    with _lock:
        nodes = list(_nodes.values())
    nodes_out: dict[str, object] = {}
    for node in nodes:
        nodes_out[node.node_id] = _poll_node(node)
    with _lock:
        recent = list(_timeline)
    leader = None
    for nid in _NODE_IDS:
        state = nodes_out.get(nid)
        if (
            isinstance(state, dict)
            and state.get("reachable")
            and state.get("role") == "leader"
        ):
            leader = nid
            break
    return {"leader": leader, "nodes": nodes_out, "events": recent}


def _find_leader() -> str | None:
    with _lock:
        nodes = list(_nodes.values())
    for node in nodes:
        with _lock:
            if node.paused:
                continue
        try:
            status, body = _http("GET", f"{_url(node.node_id)}/health", timeout=0.4)
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
        if status == 200 and body.get("role") == "leader":
            return node.node_id
    return None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    await asyncio.to_thread(_start_cluster)
    try:
        yield
    finally:
        await asyncio.to_thread(_stop_cluster)


app = FastAPI(
    title="Quasar lab",
    description="Interactive control room for a live 3-node Quasar cluster.",
    version="0.1.0",
    lifespan=_lifespan,
)


class PutBody(BaseModel):
    value: str


def _target_node(node: str | None, write: bool) -> str:
    if node:
        if node not in _nodes:
            raise HTTPException(status_code=404, detail=f"unknown node {node}")
        with _lock:
            paused = _nodes[node].paused
        if paused:
            raise HTTPException(status_code=503, detail=f"{node} is paused")
        return node
    leader = _find_leader()
    if leader:
        return leader
    if write:
        raise HTTPException(status_code=503, detail="no leader")
    for nid in _NODE_IDS:
        with _lock:
            n = _nodes[nid]
            if not n.paused and n.process is not None and n.process.poll() is None:
                return nid
    raise HTTPException(status_code=503, detail="no reachable node")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


@app.get("/how")
def how() -> FileResponse:
    return FileResponse(_STATIC / "how.html")


@app.api_route("/p/{node_id}/{rest:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def peer_proxy(node_id: str, rest: str, request: Request) -> JSONResponse:
    """Fail-fast stand-in so a SIGSTOP'd peer does not hang cluster RPC."""
    if node_id not in _NODE_IDS:
        raise HTTPException(status_code=404, detail=f"unknown node {node_id}")
    with _lock:
        node = _nodes.get(node_id)
        paused = node.paused if node else True
        running = (
            node is not None
            and node.process is not None
            and node.process.poll() is None
        )
    if node is None or paused or not running:
        raise HTTPException(status_code=502, detail=f"{node_id} unreachable")
    raw = await request.body()
    payload = json.loads(raw) if raw else None
    try:
        status, body = await asyncio.to_thread(
            _http, request.method, f"{_url(node_id)}/{rest}", payload, 1.0
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return JSONResponse(body, status_code=status)


@app.get("/cluster/snapshot")
def cluster_snapshot() -> dict[str, object]:
    return _snapshot()


@app.get("/cluster/stream")
async def cluster_stream() -> StreamingResponse:
    async def gen():
        while True:
            snap = await asyncio.to_thread(_snapshot)
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


def _kv_response(served_by: str, status: int, body: dict) -> JSONResponse:
    return JSONResponse(
        {"served_by": served_by, "status": status, "body": body},
        status_code=200,
    )


@app.put("/cluster/kv/{key}")
def put_kv(key: str, body: PutBody, node: str | None = None) -> JSONResponse:
    target = _target_node(node, write=True)
    try:
        status, payload = _http(
            "PUT",
            f"{_url(target)}/kv/{key}",
            {"value": body.value},
            timeout=5.0,
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    log = payload.get("log") if isinstance(payload, dict) else None
    if (
        status == 200
        and isinstance(log, dict)
        and int(payload.get("commit_index") or 0) < int(log.get("index") or 0)
    ):
        failed = payload.get("log_replication_failed") or []
        acks = 1 + (len(_NODE_IDS) - 1) - len(failed)
        _record(
            "commit_blocked",
            target,
            index=int(log["index"]),
            acks=acks,
            majority=2,
        )
    return _kv_response(target, status, payload)


@app.get("/cluster/kv/{key}")
def get_kv(key: str, node: str | None = None) -> JSONResponse:
    target = _target_node(node, write=False)
    try:
        status, payload = _http("GET", f"{_url(target)}/kv/{key}", timeout=2.0)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _kv_response(target, status, payload)


@app.delete("/cluster/kv/{key}")
def delete_kv(key: str, node: str | None = None) -> JSONResponse:
    target = _target_node(node, write=True)
    try:
        status, payload = _http("DELETE", f"{_url(target)}/kv/{key}", timeout=5.0)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _kv_response(target, status, payload)


@app.post("/cluster/nodes/{node_id}/pause")
def pause_node(node_id: str) -> dict[str, object]:
    if not hasattr(signal, "SIGSTOP"):
        raise HTTPException(
            status_code=501, detail="pause requires SIGSTOP (macOS/Linux)"
        )
    with _lock:
        node = _get_node(node_id)
        if node.paused:
            return {"node": node_id, "paused": True}
        if node.process is None or node.process.poll() is not None:
            raise HTTPException(status_code=503, detail=f"{node_id} is not running")
        if node.process.pid is None:
            raise HTTPException(status_code=503, detail=f"{node_id} has no pid")
        os.kill(node.process.pid, signal.SIGSTOP)
        node.paused = True
    return {"node": node_id, "paused": True}


@app.post("/cluster/nodes/{node_id}/resume")
def resume_node(node_id: str) -> dict[str, object]:
    with _lock:
        node = _get_node(node_id)
        if not node.paused:
            return {"node": node_id, "paused": False}
        if node.process is None or node.process.poll() is not None:
            node.paused = False
            raise HTTPException(status_code=503, detail=f"{node_id} is not running")
        _cont_if_paused(node)
    return {"node": node_id, "paused": False}


@app.post("/cluster/nodes/{node_id}/restart")
def restart_node(node_id: str) -> dict[str, object]:
    with _lock:
        node = _get_node(node_id)
        _stop_node(node)
        node.last_state = None
        node.voted_for = None
    _spawn(node)
    ok = _wait_ready(node)
    if not ok:
        raise HTTPException(
            status_code=503,
            detail=f"{node_id} failed to restart ({node.last_error})",
        )
    return {"node": node_id, "restarted": True}


if _STATIC.is_dir():
    app.mount("/assets", StaticFiles(directory=_STATIC), name="assets")
