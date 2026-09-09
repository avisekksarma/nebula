import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

_HEARTBEAT_INTERVAL = 0.25
_ELECTION_TIMEOUT_MIN = 0.8
_ELECTION_TIMEOUT_MAX = 1.6

_store: dict[str, str] = {}
_log: list[dict[str, str | int]] = []
# Raft log indexes are 1, 2, 3, … (0 means none). They are not list offsets
# into _log: after a snapshot, _log[0] might be log index 101.
_commit_index = 0  # majority has this entry; safe to apply to the KV map
_last_applied = 0  # already copied into _store
_lock = threading.Lock()
_stop = threading.Event()
_role = "follower"
_term = 0
_voted_for: str | None = None
_leader: str | None = None
_last_heard = 0.0
_leader_alive = False
_election_timeout = 1.0
_next_index: dict[str, int] = {}  # leader only: next log index to send each follower
_snapshot_index = 0  # snapshot covers 1..here; those entries are gone from _log
_snapshot_term = 0  # term of the log entry at snapshot_index


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Load disk state, then run the election/heartbeat thread until shutdown."""
    _load_meta()  # term/vote before we talk to anyone
    _load_snapshot()  # KV + snapshot boundary; do not replay 1..last_included
    _load_wal()  # leftover WAL only (indexes after the snapshot)
    if _snapshot_index == 0:
        print(
            f"{_node_id()}: store empty until leader_commit (log={len(_log)})",
            flush=True,
        )
    _reset_election_timeout()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    yield
    _stop.set()


app = FastAPI(
    title="Quasar",
    description="A distributed, replicated key-value store.",
    version="0.1.0",
    lifespan=_lifespan,
)


def _node_id() -> str:
    """This process's cluster id from QUASAR_NODE_ID (default A)."""
    return os.environ.get("QUASAR_NODE_ID", "A")


def _not_leader_error() -> HTTPException:
    """409 plus whoever we currently believe is leader (may be None)."""
    with _lock:
        leader = _leader
    return HTTPException(
        status_code=409,
        detail={
            "error": "not the leader",
            "leader": leader,
            "leader_url": _peers().get(leader) if leader else None,
        },
    )


def _require_leader() -> None:
    """Reject client writes unless this node is the current leader.

    PUT/DELETE call this first. Followers get 409 plus the leader URL so
    the client can retry there.
    """
    with _lock:
        if _role == "leader":
            return
    raise _not_leader_error()


def _confirm_leader_majority() -> None:
    """Leader: ping followers before a linearizable GET.

    Self counts as 1. Majority (2 of 3) must acknowledge this term.
    HTTP is outside the lock. Higher term → step down, do not return a value.
    """
    with _lock:
        if _role != "leader":
            is_leader = False
        else:
            is_leader = True
            term = _term
            me = _node_id()
    if not is_leader:
        raise _not_leader_error()

    acks = 1
    tally = threading.Lock()

    def one(node_id: str, addr: str) -> None:
        nonlocal acks
        try:
            reply = _http_json(
                "POST",
                f"{addr}/internal/read_confirm",
                {"term": term, "leader_id": me},
                timeout=1.0,
            )
        except urllib.error.URLError as exc:
            print(f"{me}: read confirm to {node_id} failed: {exc}", flush=True)
            return
        with _lock:
            their_term = int(reply.get("term", 0))
            if their_term > _term:
                _become_follower(their_term)
                ok = False
            elif _role != "leader" or not reply.get("success"):
                ok = False
            else:
                ok = True
        if ok:
            with tally:
                acks += 1

    threads = [
        threading.Thread(target=one, args=(nid, addr), daemon=True)
        for nid, addr in _other_peers().items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with _lock:
        still_leader = _role == "leader"
        majority = _majority()
    if not still_leader:
        raise _not_leader_error()
    if acks < majority:
        print(
            f"{me}: linearizable read denied (acks={acks}/{majority})",
            flush=True,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error": "no majority for linearizable read",
                "acks": acks,
                "majority": majority,
            },
        )


def _peers() -> dict[str, str]:
    """Parse QUASAR_PEERS (`A=http://...,B=...`) into id → base URL."""
    raw = os.environ.get("QUASAR_PEERS", "")
    peers: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        node_id, addr = part.split("=", 1)
        peers[node_id.strip()] = addr.strip().rstrip("/")
    return peers


def _other_peers() -> dict[str, str]:
    """Peer map without this node — who we replicate to and vote with."""
    me = _node_id()
    return {nid: addr for nid, addr in _peers().items() if nid != me}


def _cluster_size() -> int:
    """Voting members: len(peers), plus this node if it was omitted from the list."""
    peers = _peers()
    n = len(peers)
    if _node_id() not in peers:
        n += 1
    return max(1, n)


def _majority() -> int:
    """Votes or acks needed to win an election or commit an entry (n//2 + 1)."""
    return _cluster_size() // 2 + 1


def _apply_entry(entry: dict[str, str | int]) -> None:
    """Apply one already-committed log entry to the KV map."""
    op = str(entry["operation"])
    key = str(entry["key"])
    if op == "PUT":
        _store[key] = str(entry["value"])
    elif op == "DELETE":
        _store.pop(key, None)


def _apply_committed() -> None:
    """Copy newly committed leftover-WAL entries into _store.

    Called after commit_index moves (local majority or leader_commit).
    Walks last_applied+1 .. commit_index. Snapshot-covered indexes are
    already in _store, so they are skipped, not replayed.
    """
    global _last_applied
    while _last_applied < _commit_index:
        nxt = _last_applied + 1
        if nxt <= _snapshot_index:
            _last_applied = nxt
            continue
        entry = _entry_at(nxt)
        if entry is None:
            break
        _last_applied = nxt
        _apply_entry(entry)
        print(f"{_node_id()}: apply {entry} (commit_index={_commit_index})", flush=True)


def _last_index() -> int:
    """Newest Raft log index this node still knows.

    That is _log[-1]['index'] when the leftover WAL is non-empty. If a
    snapshot compacted everything, _log is empty and this is
    _snapshot_index — not 0, and not len(_log).
    """
    if _log:
        return int(_log[-1]["index"])
    return _snapshot_index


def _last_log_meta() -> tuple[int, int]:
    """(last log index, last log term) for RequestVote up-to-date checks.

    Same source as _last_index: leftover WAL tail, or the snapshot
    boundary when the WAL is empty.
    """
    if _log:
        return int(_log[-1]["index"]), int(_log[-1]["term"])
    return _snapshot_index, _snapshot_term


def _log_pos(index: int) -> int | None:
    """List offset in _log for this Raft log index, or None if it is not there.

    After compaction _log[0] may be log index 101, so you cannot use
    _log[index - 1]. Offset is index minus the first leftover index.
    """
    if not _log:
        return None
    pos = index - int(_log[0]["index"])
    if pos < 0 or pos >= len(_log):
        return None
    return pos


def _entry_at(index: int) -> dict[str, str | int] | None:
    """Leftover WAL entry at this Raft log index, or None if compacted/missing."""
    pos = _log_pos(index)
    if pos is None:
        return None
    return _log[pos]


def _term_at(index: int) -> int:
    """Term of the log entry at this Raft index (for prev_log_term).

    Index 0 is term 0. If index is exactly _snapshot_index, the entry is
    gone from _log — use _snapshot_term instead.
    """
    if index <= 0:
        return 0
    if index == _snapshot_index:
        return _snapshot_term
    entry = _entry_at(index)
    return 0 if entry is None else int(entry["term"])


def _prefix_matches(prev_log_index: int, prev_log_term: int) -> bool:
    """True if we have the leader's prev_log_index with the same term.

    AppendEntries rejects unless this matches. prev_log_index 0 means
    'before the first entry'. At the snapshot boundary, compare against
    snapshot_term because that entry is no longer in _log.
    """
    if prev_log_index == 0:
        return True
    if prev_log_index == _snapshot_index:
        return prev_log_term == _snapshot_term
    entry = _entry_at(prev_log_index)
    if entry is None:
        return False
    return int(entry["term"]) == prev_log_term


def _advance_commit_from_leader(leader_commit: int) -> None:
    """Set commit_index from the leader's leader_commit, then apply.

    Followers never guess commit_index (it is not on disk). Cap at
    _last_index so we do not apply entries we do not have.
    """
    global _commit_index
    new = min(leader_commit, _last_index())  # leader tells us; we do not guess
    if new > _commit_index:
        _commit_index = new
    _apply_committed()


def _data_dir() -> str:
    """Per-node data directory (QUASAR_DATA_DIR, or data/<node-id>)."""
    return os.environ.get("QUASAR_DATA_DIR") or os.path.join("data", _node_id())


def _wal_path() -> str:
    """Path to this node's leftover log WAL (log.jsonl)."""
    return os.path.join(_data_dir(), "log.jsonl")


def _meta_path() -> str:
    """Path to raft.json (current_term, voted_for)."""
    return os.path.join(_data_dir(), "raft.json")


def _snapshot_path() -> str:
    """Path to snapshot.json (_snapshot_index/term + applied KV)."""
    return os.path.join(_data_dir(), "snapshot.json")


def _persist_snapshot_file(
    last_index: int, last_term: int, store: dict[str, str]
) -> dict[str, object]:
    """fsync snapshot.json (tmp + replace). Does not touch _log or in-memory indexes.

    Used by local POST /snapshot and by installing a leader snapshot. Caller
    updates _snapshot_index/_snapshot_term and compacts the WAL only after
    this returns.
    """
    payload: dict[str, object] = {
        "last_included_index": last_index,
        "last_included_term": last_term,
        "store": dict(store),
    }
    path = _snapshot_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(json.dumps(payload))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return payload


def _write_snapshot() -> dict[str, object] | None:
    """Snapshot the applied KV (through last_applied). Does not compact the WAL.

    POST /snapshot calls this, then compact. None if last_applied is 0.
    The snapshot covers last_applied, not _last_index (uncommitted tail
    is not in the snapshot).
    """
    if _last_applied == 0:
        return None
    last_index = _last_applied
    return _persist_snapshot_file(
        last_index, _term_at(last_index), dict(_store)
    )


def _load_snapshot() -> None:
    """Startup: restore _store, _snapshot_index/_snapshot_term, and last_applied.

    Do not replay 1.._snapshot_index. commit_index stays 0 until the leader
    sends leader_commit; then only the leftover WAL is applied.
    Missing or junk file: same as no snapshot.
    """
    global _snapshot_index, _snapshot_term, _last_applied
    path = _snapshot_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        last_index = int(data["last_included_index"])
        last_term = int(data["last_included_term"])
        raw_store = data.get("store")
        if last_index <= 0 or not isinstance(raw_store, dict):
            raise ValueError("invalid snapshot")
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        print(f"{_node_id()}: snapshot ignore malformed file", flush=True)
        return
    _snapshot_index = last_index
    _snapshot_term = last_term
    _store.clear()
    for key, value in raw_store.items():
        _store[str(key)] = str(value)
    _last_applied = last_index
    print(
        f"{_node_id()}: snapshot loaded last_included_index={_snapshot_index} "
        f"keys={len(_store)}",
        flush=True,
    )


def _compact_log(up_to: int) -> None:
    """Drop leftover-WAL entries with log index <= up_to.

    Call only after snapshot.json is fsynced. Rewrites log.jsonl with the
    suffix and replaces _log. Entries after up_to stay.
    """
    remaining = [e for e in _log if int(e["index"]) > up_to]
    _wal_rewrite(remaining)
    _log[:] = remaining


def _install_received_snapshot(
    last_index: int, last_term: int, store: dict[str, str]
) -> None:
    """Install a leader snapshot: replace KV, persist, compact, mark applied.

    Called from POST /internal/install_snapshot. fsync snapshot.json, then
    compact. last_applied and commit_index move to last_index so 1..N
    are not replayed. Leftover entries after that index stay in _log.
    """
    global _snapshot_index, _snapshot_term, _last_applied, _commit_index
    _persist_snapshot_file(last_index, last_term, store)
    _snapshot_index = last_index
    _snapshot_term = last_term
    _store.clear()
    for key, value in store.items():
        _store[str(key)] = str(value)
    _last_applied = last_index
    if _commit_index < last_index:
        _commit_index = last_index
    _compact_log(last_index)
    print(
        f"{_node_id()}: installed snapshot last_included_index={_snapshot_index} "
        f"last_included_term={_snapshot_term} keys={len(_store)}; "
        f"wal remaining={len(_log)}",
        flush=True,
    )


def _persist_meta(term: int, voted_for: str | None) -> None:
    """fsync current_term and voted_for to raft.json.

    Must finish before we grant a vote or start an election, so a crash
    cannot make us vote twice in the same term.
    """
    path = _meta_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(json.dumps({"current_term": term, "voted_for": voted_for}))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _load_meta() -> None:
    """Startup: restore term and voted_for from raft.json.

    Missing or junk file leaves defaults (term 0, voted_for None).
    """
    global _term, _voted_for
    path = _meta_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        _term = int(data["current_term"])
        voted = data.get("voted_for")
        _voted_for = None if voted is None else str(voted)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        print(f"{_node_id()}: meta ignore malformed file", flush=True)
        return
    print(
        f"{_node_id()}: meta loaded term={_term} voted_for={_voted_for}",
        flush=True,
    )


def _wal_write_lines(fh, entries: list[dict[str, str | int]]) -> None:
    """Write entries as JSONL and fsync. Shared by append and rewrite."""
    for entry in entries:
        fh.write(json.dumps(entry, separators=(",", ":")) + "\n")
    fh.flush()
    os.fsync(fh.fileno())  # durable before we touch _log


def _wal_append(entry: dict[str, str | int]) -> None:
    """Append one log entry to log.jsonl and fsync before _log.append."""
    path = _wal_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a") as fh:
        _wal_write_lines(fh, [entry])


def _wal_rewrite(entries: list[dict[str, str | int]]) -> None:
    """Replace log.jsonl with exactly these entries (tmp, fsync, rename).

    Used after snapshot compaction or when a conflict truncates the
    uncommitted tail. Cannot edit one JSONL line in the middle.
    """
    path = _wal_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        _wal_write_lines(fh, entries)
    os.replace(tmp, path)


def _load_wal() -> None:
    """Startup: rebuild _log from log.jsonl (suffix only, not applied yet).

    Skip entries at or below _snapshot_index. Drop a trailing incomplete
    JSON line from a crash. Do not apply; wait for leader_commit.
    """
    path = _wal_path()
    if not os.path.isfile(path):
        return
    loaded: list[dict[str, str | int]] = []
    skipped = False
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                skipped = True
                print(
                    f"{_node_id()}: wal skip trailing incomplete record",
                    flush=True,
                )
                break
            if int(entry.get("index", 0)) <= _snapshot_index:
                continue
            loaded.append(entry)
    _log.extend(loaded)
    if skipped:
        _wal_rewrite(loaded)
    print(f"{_node_id()}: wal loaded {len(_log)} entries from {path}", flush=True)


def _append_log(operation: str, key: str, value: str | None = None) -> dict[str, str | int]:
    """Leader: persist a new log entry at _last_index()+1, then append to _log.

    PUT/DELETE call this before replicating. Disk first, then memory.
    """
    with _lock:
        entry: dict[str, str | int] = {
            "index": _last_index() + 1,
            "term": _term,
            "operation": operation,
            "key": key,
        }
        if value is not None:
            entry["value"] = value
        _wal_append(entry)
        _log.append(entry)
        print(f"{_node_id()}: log append {entry}", flush=True)
        return entry


def _send_append_entries(node_id: str, addr: str) -> bool:
    """Leader: replicate to one follower (AppendEntries, or snapshot if too far behind).

    Heartbeats, PUT/DELETE, and commit push all go through here. On prefix
    reject, walk next_index back — unless the follower's last_log_index is
    below _snapshot_index, in which case send the snapshot instead.
    HTTP is outside the lock.
    """
    while True:
        send_snapshot = False
        with _lock:
            if _role != "leader":
                return False
            last = _last_index()
            ni = min(_next_index.get(node_id, last + 1), last + 1)
            if ni <= _snapshot_index:
                ni = _snapshot_index + 1
            _next_index[node_id] = ni
            prev_index = ni - 1
            prev_term = _term_at(prev_index)
            start = _log_pos(ni)
            entries = [] if start is None else [dict(e) for e in _log[start:]]
            payload = {
                "term": _term,
                "leader_id": _node_id(),
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": _commit_index,
            }
        try:
            reply = _http_json(
                "POST", f"{addr}/internal/append", payload, timeout=1.0
            )
        except urllib.error.URLError as exc:
            print(f"log replication to {node_id} failed: {exc}", flush=True)
            return False
        with _lock:
            their_term = int(reply.get("term", 0))
            if their_term > _term:
                _become_follower(their_term)
                return False
            if _role != "leader":
                return False
            if reply.get("success"):
                _next_index[node_id] = ni + len(entries)
                return True
            their_last = int(reply.get("last_log_index", 0))
            if _snapshot_index > 0 and their_last < _snapshot_index:
                print(
                    f"{_node_id()}: append to {node_id} cannot catch up "
                    f"(follower last_log_index={their_last}, "
                    f"leader snapshot={_snapshot_index}); sending snapshot",
                    flush=True,
                )
                send_snapshot = True
            elif ni <= 1:
                return False
            else:
                _next_index[node_id] = ni - 1
                print(
                    f"{_node_id()}: append to {node_id} rejected, next_index={ni - 1}",
                    flush=True,
                )
        if send_snapshot:
            if not _send_install_snapshot(node_id, addr):
                return False
            continue  # AppendEntries from next_index = snapshot + 1


def _send_install_snapshot(node_id: str, addr: str) -> bool:
    """Leader: POST snapshot.json to a far-behind follower, then set next_index.

    Called from _send_append_entries when follower last_log_index <
    _snapshot_index. Sends the file (not live _store). On success,
    next_index becomes _snapshot_index+1 so AppendEntries can resume.
    HTTP is outside the lock.
    """
    with _lock:
        if _role != "leader" or _snapshot_index == 0:
            return False
        path = _snapshot_path()
        term = _term
        me = _node_id()
        try:
            with open(path) as fh:
                snap = json.load(fh)
            payload = {
                "term": term,
                "leader_id": me,
                "last_included_index": int(snap["last_included_index"]),
                "last_included_term": int(snap["last_included_term"]),
                "store": {str(k): str(v) for k, v in dict(snap["store"]).items()},
            }
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            print(f"{_node_id()}: snapshot send to {node_id} failed: {exc}", flush=True)
            return False
        last_index = int(payload["last_included_index"])
    try:
        reply = _http_json(
            "POST", f"{addr}/internal/install_snapshot", payload, timeout=2.0
        )
    except urllib.error.URLError as exc:
        print(f"{_node_id()}: snapshot send to {node_id} failed: {exc}", flush=True)
        return False
    with _lock:
        their_term = int(reply.get("term", 0))
        if their_term > _term:
            _become_follower(their_term)
            return False
        if not reply.get("success"):
            return False
        _next_index[node_id] = last_index + 1
        print(
            f"{_node_id()}: installed snapshot on {node_id} through {last_index}",
            flush=True,
        )
        return True


def _replicate_log() -> list[str]:
    """Leader: AppendEntries to every other peer in parallel. Returns ids that failed.

    Parallel so one unreachable follower cannot delay the live one past the
    election timeout (a 1s HTTP timeout is enough to make the other node
    start an election).
    """
    return _send_append_to_all(wait=True)


def _push_commit_index() -> None:
    """Leader: send AppendEntries again so followers learn the new commit_index."""
    _send_append_to_all(wait=True)


def _send_append_to_all(*, wait: bool = True) -> list[str]:
    """AppendEntries every other peer at once.

    wait=True (writes): join so we know who acked. wait=False (heartbeats):
    do not join, or a paused peer's 1s timeout would stall heartbeats to
    the live majority and trigger a needless election.
    """
    failed: list[str] = []
    lock = threading.Lock()
    threads: list[threading.Thread] = []

    def one(node_id: str, addr: str) -> None:
        if not _send_append_entries(node_id, addr):
            with lock:
                failed.append(node_id)

    for node_id, addr in _other_peers().items():
        t = threading.Thread(target=one, args=(node_id, addr), daemon=True)
        threads.append(t)
        t.start()
    if wait:
        for t in threads:
            t.join()
        return failed
    return []


def _maybe_commit(index: int, acks: int) -> None:
    """If a majority has this log index, raise commit_index and apply, then notify followers.

    PUT/DELETE call this after replication. acks includes the leader itself.
    """
    global _commit_index
    if acks < _majority():
        return
    with _lock:
        if index > _commit_index:
            _commit_index = index
            print(
                f"{_node_id()}: commit_index={_commit_index} (acks={acks}/{_majority()})",
                flush=True,
            )
        _apply_committed()
    _push_commit_index()


def _http_json(
    method: str, url: str, payload: dict | None = None, timeout: float = 5.0
) -> dict:
    """POST/GET JSON to a peer. Caller must not hold _lock.

    Timeouts and dropped connections become URLError so replication/votes
    treat an unreachable peer as a failed request, not a crash.
    """
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except (TimeoutError, ConnectionError) as exc:
        raise urllib.error.URLError(exc) from exc


def _reset_election_timeout() -> None:
    """Pick a new randomized election timeout and mark 'heard from leader' now."""
    global _election_timeout, _last_heard
    _election_timeout = random.uniform(_ELECTION_TIMEOUT_MIN, _ELECTION_TIMEOUT_MAX)
    _last_heard = time.monotonic()


def _become_follower(new_term: int | None = None) -> None:
    """Step down to follower. If new_term is higher, persist it and clear voted_for.

    RPC handlers call this on a newer term. Persists raft.json before we
    act in that term.
    """
    global _role, _term, _voted_for, _leader, _leader_alive
    if new_term is not None and new_term > _term:
        if _role in ("leader", "candidate"):
            print(
                f"{_node_id()}: stepping down {_role} → follower (term {_term} → {new_term})",
                flush=True,
            )
        _term = new_term
        _voted_for = None
        _persist_meta(_term, _voted_for)  # durable before we act in the new term
        _leader = None
        _leader_alive = False
    _role = "follower"
    _reset_election_timeout()


def _send_heartbeats() -> None:
    """Leader: AppendEntries to each peer in parallel (empty entries if nothing new)."""
    with _lock:
        if _role != "leader":
            return
    _send_append_to_all(wait=False)


def _start_election() -> None:
    """Become candidate, persist vote for self, RequestVote the others.

    Heartbeat loop calls this on election timeout. Majority votes → leader,
    next_index initialized to last_log_index+1, then immediate heartbeats.
    """
    global _role, _term, _voted_for, _leader, _leader_alive, _next_index
    with _lock:
        if _role == "leader":
            return
        _role = "candidate"
        _term += 1
        me = _node_id()
        _voted_for = me
        _persist_meta(_term, _voted_for)  # before RequestVote leaves this node
        _leader = None
        _leader_alive = False
        term = _term
        last_log_index, last_log_term = _last_log_meta()
        _reset_election_timeout()
        print(f"{me}: candidate term={term} (requesting votes)", flush=True)

    votes = 1
    majority = _majority()
    for node_id, addr in _other_peers().items():
        try:
            reply = _http_json(
                "POST",
                f"{addr}/internal/vote",
                {
                    "term": term,
                    "candidate_id": me,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                },
                timeout=1.0,
            )
        except urllib.error.URLError as exc:
            print(f"{me}: vote request to {node_id} failed: {exc}", flush=True)
            continue
        with _lock:
            their_term = int(reply.get("term", 0))
            if their_term > _term:
                _become_follower(their_term)
                return
            if _role != "candidate" or _term != term:
                return
            if reply.get("vote_granted"):
                votes += 1
                print(f"{me}: got vote from {node_id} ({votes}/{majority})", flush=True)
                if votes >= majority:
                    _role = "leader"
                    _leader = me
                    _leader_alive = True
                    _next_index = {nid: _last_index() + 1 for nid in _other_peers()}
                    print(f"{me}: leader term={term}", flush=True)
                    break

    with _lock:
        won = _role == "leader"
    if won:
        _send_heartbeats()


def _heartbeat_loop() -> None:
    """Background: leader sends heartbeats; follower starts an election on timeout."""
    while not _stop.wait(_HEARTBEAT_INTERVAL):
        with _lock:
            role = _role
            timed_out = (time.monotonic() - _last_heard) >= _election_timeout
        if role == "leader":
            _send_heartbeats()
        elif timed_out:
            _start_election()


class PutBody(BaseModel):
    value: str


class HeartbeatBody(BaseModel):
    term: int
    leader_id: str
    leader_commit: int = 0


class VoteBody(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int = 0
    last_log_term: int = 0


class AppendBody(BaseModel):
    term: int = 0
    leader_id: str = ""
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: list[dict[str, str | int]] = Field(default_factory=list)
    leader_commit: int = 0


class InstallSnapshotBody(BaseModel):
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    store: dict[str, str] = Field(default_factory=dict)


class ReadConfirmBody(BaseModel):
    term: int
    leader_id: str


@app.get("/health")
def health() -> dict[str, str | int | bool | None]:
    """Role, term, leader, and commit_index for this node."""
    with _lock:
        return {
            "status": "ok",
            "service": "quasar",
            "node": _node_id(),
            "role": _role,
            "term": _term,
            "voted_for": _voted_for,
            "leader": _leader,
            "leader_alive": True if _role == "leader" else _leader_alive,
            "commit_index": _commit_index,
        }


@app.post("/snapshot")
def create_snapshot() -> dict[str, object]:
    """Write a local snapshot of the applied KV, then drop that prefix from the WAL.

    Any node can call this. Does not send the snapshot to peers. Covers
    last_applied, not _last_index (uncommitted tail stays in the WAL).
    """
    global _snapshot_index, _snapshot_term
    with _lock:
        snap = _write_snapshot()
        if snap is None:
            raise HTTPException(status_code=400, detail="no committed entries to snapshot")
        _snapshot_index = int(snap["last_included_index"])
        _snapshot_term = int(snap["last_included_term"])
        _compact_log(_snapshot_index)
        print(
            f"{_node_id()}: snapshot last_included_index={_snapshot_index} "
            f"last_included_term={_snapshot_term}; wal remaining={len(_log)}",
            flush=True,
        )
        return {"ok": True, "path": _snapshot_path(), **snap}


@app.get("/log")
def get_log() -> dict[str, object]:
    """Leftover WAL plus commit_index, snapshot_index, and last_applied."""
    with _lock:
        return {
            "node": _node_id(),
            "role": _role,
            "term": _term,
            "commit_index": _commit_index,
            "snapshot_index": _snapshot_index,
            "last_applied": _last_applied,
            "entries": list(_log),
        }


@app.put("/kv/{key}")
def put(key: str, body: PutBody) -> dict[str, object]:
    """Leader: append PUT, replicate, commit if a majority acked."""
    _require_leader()
    entry = _append_log("PUT", key, body.value)
    log_failed = _replicate_log()
    acks = 1 + len(_other_peers()) - len(log_failed)
    _maybe_commit(int(entry["index"]), acks)
    with _lock:
        commit_index = _commit_index
    return {
        "key": key,
        "value": body.value,
        "log": entry,
        "commit_index": commit_index,
        "log_replication_failed": log_failed,
    }


@app.get("/kv/{key}")
def get(key: str) -> dict[str, str]:
    """Linearizable read of a committed key.

    Followers 409. The leader confirms a majority still agrees on this term
    (no log entry, no KV change), then reads _store only.
    """
    _require_leader()
    _confirm_leader_majority()
    with _lock:
        if _role != "leader":
            raise _not_leader_error()
        if key not in _store:
            raise HTTPException(status_code=404, detail="key not found")
        return {"key": key, "value": _store[key]}


@app.delete("/kv/{key}")
def delete(key: str) -> dict[str, object]:
    """Leader: append DELETE, replicate, commit if a majority acked."""
    _require_leader()
    with _lock:
        if key not in _store:
            raise HTTPException(status_code=404, detail="key not found")
    entry = _append_log("DELETE", key)
    log_failed = _replicate_log()
    acks = 1 + len(_other_peers()) - len(log_failed)
    _maybe_commit(int(entry["index"]), acks)
    with _lock:
        commit_index = _commit_index
    return {
        "deleted": key,
        "log": entry,
        "commit_index": commit_index,
        "log_replication_failed": log_failed,
    }


@app.post("/internal/append")
def receive_append(body: AppendBody) -> dict[str, object]:
    """Follower handler for AppendEntries.

    Leader calls this from _send_append_entries. Stale term or prefix
    mismatch → reject and return last_log_index. Else append or replace
    the uncommitted suffix, then apply up to leader_commit.
    """
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {
                "term": _term,
                "success": False,
                "last_log_index": _last_index(),
            }
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()

        if not _prefix_matches(body.prev_log_index, body.prev_log_term):
            print(
                f"{_node_id()}: append reject prev_index={body.prev_log_index} "
                f"prev_term={body.prev_log_term}",
                flush=True,
            )
            return {
                "term": _term,
                "success": False,
                "last_log_index": _last_index(),
            }

        for entry in body.entries:
            idx = int(entry["index"])
            existing = _entry_at(idx)
            if existing is not None:
                existing_term = int(existing["term"])
                incoming_term = int(entry["term"])
                if existing_term == incoming_term:
                    continue
                if idx <= _commit_index:
                    print(
                        f"{_node_id()}: refuse truncate committed index={idx}",
                        flush=True,
                    )
                    return {
                        "term": _term,
                        "success": False,
                        "last_log_index": _last_index(),
                    }
                print(
                    f"{_node_id()}: conflict at index={idx} "
                    f"(had term {existing_term}, got {incoming_term}); truncated",
                    flush=True,
                )
                replaced = [e for e in _log if int(e["index"]) < idx] + [dict(entry)]
                _wal_rewrite(replaced)
                _log[:] = replaced
                print(f"{_node_id()}: log append {entry}", flush=True)
            elif idx == _last_index() + 1:
                incoming = dict(entry)
                _wal_append(incoming)
                _log.append(incoming)
                print(f"{_node_id()}: log append {entry}", flush=True)
            else:
                print(
                    f"{_node_id()}: skip gap index={idx} expected={_last_index() + 1}",
                    flush=True,
                )
                return {
                    "term": _term,
                    "success": False,
                    "last_log_index": _last_index(),
                }

        _advance_commit_from_leader(body.leader_commit)
        return {
            "term": _term,
            "success": True,
            "last_log_index": _last_index(),
        }


@app.post("/internal/install_snapshot")
def receive_install_snapshot(body: InstallSnapshotBody) -> dict[str, object]:
    """Follower handler for InstallSnapshot.

    Leader calls this when our last log is behind _snapshot_index.
    Same term rules as append. Older/equal snapshots are ignored.
    """
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {"term": _term, "success": False}
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()
        if body.last_included_index <= _snapshot_index:
            return {"term": _term, "success": True}
        _install_received_snapshot(
            body.last_included_index,
            body.last_included_term,
            dict(body.store),
        )
        return {"term": _term, "success": True}


@app.post("/internal/read_confirm")
def receive_read_confirm(body: ReadConfirmBody) -> dict[str, object]:
    """Follower: acknowledge a leader's linearizable-read check.

    Same term rules as heartbeat/append. Does not touch the log or KV.
    Stale term → success false so the caller cannot count us in the majority.
    """
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {"term": _term, "success": False}
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()
        return {"term": _term, "success": True}


@app.post("/internal/heartbeat")
def receive_heartbeat(body: HeartbeatBody) -> dict[str, object]:
    """Follower handler for the leftover heartbeat RPC (term + leader_commit).

    New replication uses AppendEntries; this path still applies leader_commit.
    """
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {
                "term": _term,
                "success": False,
                "last_log_index": _last_index(),
            }
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()
        _advance_commit_from_leader(body.leader_commit)
        return {
            "term": _term,
            "success": True,
            "last_log_index": _last_index(),
        }


@app.post("/internal/vote")
def receive_vote(body: VoteBody) -> dict[str, object]:
    """Follower handler for RequestVote.

    Grant at most one vote per term, and only if the candidate's
    (last_log_index, last_log_term) is at least as up-to-date as ours.
    Persist voted_for before the response.
    """
    global _role, _term, _voted_for
    with _lock:
        if body.term < _term:
            return {"term": _term, "vote_granted": False}
        if body.term > _term:
            _become_follower(body.term)
        my_index, my_term = _last_log_meta()
        if body.last_log_term != my_term:
            log_ok = body.last_log_term > my_term
        else:
            log_ok = body.last_log_index >= my_index
        granted = (_voted_for is None or _voted_for == body.candidate_id) and log_ok
        if granted:
            _voted_for = body.candidate_id
            _persist_meta(_term, _voted_for)  # before the vote response
            _reset_election_timeout()
            print(
                f"{_node_id()}: granted vote to {body.candidate_id} term={_term}",
                flush=True,
            )
        return {"term": _term, "vote_granted": granted}
