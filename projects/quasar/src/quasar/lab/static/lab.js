const NODE_IDS = ["A", "B", "C"];

let snap = { leader: null, nodes: {}, events: [] };
let busy = false;

const $ = (id) => document.getElementById(id);

function narrate(text) {
  $("narration").textContent = text;
}

function qs(node) {
  return node ? `?node=${encodeURIComponent(node)}` : "";
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { detail: text };
  }
  if (!res.ok) {
    const detail = data.detail;
    const msg =
      typeof detail === "string"
        ? detail
        : detail
          ? JSON.stringify(detail)
          : res.statusText;
    throw new Error(msg);
  }
  return data;
}

async function kvOp(op) {
  const key = $("kv-key").value.trim();
  const value = $("kv-value").value;
  const node = $("kv-node").value;
  if (!key) {
    $("kv-result").textContent = "Key is required.";
    return;
  }
  let data;
  if (op === "put") {
    data = await api("PUT", `/cluster/kv/${encodeURIComponent(key)}${qs(node)}`, {
      value,
    });
  } else if (op === "get") {
    data = await api("GET", `/cluster/kv/${encodeURIComponent(key)}${qs(node)}`);
  } else {
    data = await api(
      "DELETE",
      `/cluster/kv/${encodeURIComponent(key)}${qs(node)}`,
    );
  }
  $("kv-result").textContent = JSON.stringify(data, null, 2);
  return data;
}

function nodeState(id) {
  return snap.nodes?.[id] || { node: id, reachable: false };
}

function fmtEntry(entry, commitIndex) {
  const idx = Number(entry.index);
  const committed = idx <= Number(commitIndex || 0);
  const val = entry.value !== undefined ? `=${entry.value}` : "";
  const cls = committed ? "committed" : "pending";
  const tag = committed ? "committed" : "uncommitted";
  return `<li class="${cls}">#${idx} t${entry.term} ${entry.operation} ${entry.key}${val} <span>(${tag})</span></li>`;
}

function fmtEvent(ev) {
  const n = ev.node || "?";
  switch (ev.kind) {
    case "candidate":
      return `${n} became candidate (term ${ev.term})`;
    case "vote_granted":
      return `${n} granted vote to ${ev.candidate_id} (term ${ev.term})`;
    case "vote_denied":
      return `${n} denied vote to ${ev.candidate_id} (already ${ev.voted_for})`;
    case "vote_received":
      return `${n} got vote from ${ev.peer} (${ev.votes}/${ev.majority})`;
    case "vote_request_failed":
      return `${n} vote request to ${ev.peer} failed`;
    case "leader":
      return `${n} won election, now leader (term ${ev.term})`;
    case "step_down":
      return `${n} stepped down ${ev.from_role} → follower (term ${ev.term} → ${ev.new_term})`;
    case "log_append": {
      const e = ev.entry || {};
      const val = e.value !== undefined ? `=${e.value}` : "";
      return `${n} log append #${e.index} ${e.operation} ${e.key}${val}`;
    }
    case "commit":
      return ev.source === "leader"
        ? `${n} learned commit_index=${ev.commit_index}`
        : `${n} commit_index=${ev.commit_index} (acks=${ev.acks}/${ev.majority})`;
    case "commit_blocked":
      return `${n} cannot commit #${ev.index}: acks ${ev.acks} < majority ${ev.majority}`;
    case "apply": {
      const e = ev.entry || {};
      return `${n} apply #${e.index} ${e.operation} ${e.key} → map`;
    }
    case "catch_up":
      return `${n} catch-up → ${ev.peer} indexes ${ev.from_index}–${ev.to_index}`;
    case "skip_gap":
      return `${n} skipped gap index=${ev.index} (expected ${ev.expected})`;
    case "replicate_failed":
      return `${n} replication to ${ev.peer} failed (index ${ev.index})`;
    default:
      return `${n} ${ev.kind}`;
  }
}

function renderNodes() {
  $("nodes").innerHTML = NODE_IDS.map((id) => {
    const n = nodeState(id);
    const paused = Boolean(n.paused);
    const role = paused ? "paused" : n.reachable ? n.role || "unknown" : "down";
    const commit = Number(n.commit_index || 0);
    const log = Array.isArray(n.log) ? n.log : [];
    const store = n.store && typeof n.store === "object" ? n.store : {};
    const storeKeys = Object.keys(store);
    const cardClass = [
      "card",
      role === "leader" ? "is-leader" : "",
      role === "candidate" ? "is-candidate" : "",
      paused ? "is-paused" : "",
    ]
      .filter(Boolean)
      .join(" ");
    const logHtml =
      log.length === 0
        ? `<li class="empty">empty log</li>`
        : log.map((e) => fmtEntry(e, commit)).join("");
    const storeHtml =
      storeKeys.length === 0
        ? `<li class="empty">empty map</li>`
        : storeKeys
            .map((k) => `<li>${k} = ${store[k]}</li>`)
            .join("");
    return `
      <article class="${cardClass}" data-node="${id}">
        <div class="card-head">
          <h3>${id}</h3>
          <span class="badge ${role}">${role}</span>
        </div>
        <div class="meta">
          <span>term</span><b>${n.term ?? "—"}</b>
          <span>leader</span><b>${n.leader ?? "—"}</b>
          <span>voted for</span><b>${n.voted_for ?? "—"}</b>
          <span>commit</span><b>${n.commit_index ?? 0}</b>
          <span>applied</span><b>${n.last_applied ?? 0}</b>
          <span>log len</span><b>${log.length}</b>
        </div>
        <div class="card-actions">
          <button type="button" data-act="pause" data-node="${id}" ${paused ? "disabled" : ""}>Pause</button>
          <button type="button" data-act="resume" data-node="${id}" ${paused ? "" : "disabled"}>Resume</button>
          <button type="button" data-act="restart" data-node="${id}">Restart</button>
        </div>
        <p class="block-title">Log</p>
        <ul class="log">${logHtml}</ul>
        <p class="block-title">KV map (committed only)</p>
        <ul class="store">${storeHtml}</ul>
      </article>
    `;
  }).join("");
}

function renderPulse() {
  const el = $("pulse");
  const leader = snap.leader;
  if (!leader) {
    el.classList.remove("has-leader");
    el.textContent = "no leader — waiting on election";
    return;
  }
  const others = NODE_IDS.filter((id) => {
    const n = nodeState(id);
    return id !== leader && !n.paused;
  });
  el.classList.add("has-leader");
  el.textContent =
    others.length > 0
      ? `${leader} is leader · heartbeat 250ms → ${others.join(", ")}`
      : `${leader} is leader · no live followers`;
}

function renderTimeline() {
  const events = Array.isArray(snap.events) ? snap.events : [];
  const last = events.slice(-40).reverse();
  $("timeline").innerHTML =
    last.length === 0
      ? `<li><span></span><span>No events yet.</span></li>`
      : last
          .map(
            (ev) =>
              `<li><span class="nid">${ev.node || "?"}</span><span>${fmtEvent(ev)}</span></li>`,
          )
          .join("");
}

function render() {
  renderNodes();
  renderPulse();
  renderTimeline();
}

function waitFor(predicate, timeoutMs, label) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error(label || "timed out"));
        return;
      }
      setTimeout(tick, 150);
    };
    tick();
  });
}

function liveNodes() {
  return NODE_IDS.map(nodeState).filter((n) => n.reachable && !n.paused);
}

function followerIds() {
  return liveNodes()
    .filter((n) => n.role === "follower")
    .map((n) => n.node);
}

async function resumeAll() {
  for (const id of NODE_IDS) {
    const n = nodeState(id);
    if (n.paused) {
      await api("POST", `/cluster/nodes/${id}/resume`);
    }
  }
}

async function withScenario(fn) {
  if (busy) return;
  busy = true;
  document.querySelectorAll("[data-scenario]").forEach((b) => {
    b.disabled = true;
  });
  try {
    await fn();
  } catch (err) {
    narrate(String(err.message || err));
    $("kv-result").textContent = String(err.message || err);
  } finally {
    busy = false;
    document.querySelectorAll("[data-scenario]").forEach((b) => {
      b.disabled = false;
    });
  }
}

const scenarios = {
  async election() {
    narrate(
      "Restarting all three nodes. RAM is wiped; everyone starts as a follower.",
    );
    for (const id of NODE_IDS) {
      await api("POST", `/cluster/nodes/${id}/restart`);
    }
    narrate(
      "Election timeout is 0.8–1.6s. A candidate increments its term, votes for itself, and needs one more vote. Majority is two.",
    );
    await waitFor(() => Boolean(snap.leader), 8000, "no leader after election");
    const leader = snap.leader;
    const term = nodeState(leader).term;
    narrate(
      `${leader} is leader in term ${term}. One vote per term; two votes win. Heartbeats every 250ms keep followers from starting another election.`,
    );
  },

  async write() {
    await resumeAll();
    await waitFor(() => Boolean(snap.leader), 5000, "no leader");
    const key = $("kv-key").value.trim() || "x";
    const value = $("kv-value").value || "10";
    narrate(
      `PUT ${key}=${value} on the leader. The operation is appended to the log first — not written straight into the map.`,
    );
    const data = await api("PUT", `/cluster/kv/${encodeURIComponent(key)}`, {
      value,
    });
    $("kv-result").textContent = JSON.stringify(data, null, 2);
    await new Promise((r) => setTimeout(r, 600));
    const body = data.body || {};
    const committed = body.commit_index >= (body.log && body.log.index);
    narrate(
      committed
        ? `Entry #${body.log?.index} replicated. Majority (2 of 3) acknowledged, so commit_index advanced and each node applied the entry to its map. GET reads the map, not the uncommitted tail.`
        : `Entry is on the leader log but commit_index did not advance (not enough acks). The map stays unchanged until a majority has the entry.`,
    );
    try {
      const got = await api("GET", `/cluster/kv/${encodeURIComponent(key)}`);
      $("kv-result").textContent = JSON.stringify(
        { put: data, get: got },
        null,
        2,
      );
    } catch (err) {
      $("kv-result").textContent = JSON.stringify(
        { put: data, get_error: String(err.message || err) },
        null,
        2,
      );
    }
  },

  async majority() {
    await resumeAll();
    await waitFor(() => Boolean(snap.leader), 5000, "no leader");
    await waitFor(() => followerIds().length >= 2, 4000, "need two followers");
    const [f1, f2] = followerIds();
    const key = `m${Date.now() % 100000}`;
    narrate(
      `Pausing follower ${f1}. Leader + one live follower is still a majority.`,
    );
    await api("POST", `/cluster/nodes/${f1}/pause`);
    await new Promise((r) => setTimeout(r, 300));
    const okPut = await api("PUT", `/cluster/kv/${encodeURIComponent(key)}`, {
      value: "ok",
    });
    $("kv-result").textContent = JSON.stringify(okPut, null, 2);
    const okCommit = (okPut.body || {}).commit_index;
    narrate(
      `Write committed at index ${okCommit}. One dead node does not stall the cluster. Pausing ${f2} next — now only the leader is live.`,
    );
    await new Promise((r) => setTimeout(r, 1200));
    await api("POST", `/cluster/nodes/${f2}/pause`);
    await new Promise((r) => setTimeout(r, 300));
    const stuckKey = `${key}b`;
    const stuck = await api("PUT", `/cluster/kv/${encodeURIComponent(stuckKey)}`, {
      value: "stuck",
    });
    $("kv-result").textContent = JSON.stringify(
      { with_majority: okPut, without_majority: stuck },
      null,
      2,
    );
    const logIdx = stuck.body?.log?.index;
    const commitIdx = stuck.body?.commit_index;
    narrate(
      `Second write is on the leader log (#${logIdx}) but acks=1 < 2, so commit_index stays ${commitIdx}. The map does not gain ${stuckKey}. Resume the followers when you want them to catch up.`,
    );
  },

  async catchup() {
    await resumeAll();
    await waitFor(() => Boolean(snap.leader), 5000, "no leader");
    await waitFor(() => followerIds().length >= 1, 4000, "need a follower");
    const follower = followerIds()[0];
    const prefix = `c${Date.now() % 100000}`;
    narrate(
      `Pausing follower ${follower}, then writing twice. Its log and map freeze (process is SIGSTOP’d).`,
    );
    await api("POST", `/cluster/nodes/${follower}/pause`);
    await api("PUT", `/cluster/kv/${encodeURIComponent(prefix + "1")}`, {
      value: "1",
    });
    await api("PUT", `/cluster/kv/${encodeURIComponent(prefix + "2")}`, {
      value: "2",
    });
    narrate(
      `Resuming ${follower}. The leader reads last_log_index from heartbeats and sends the missing suffix. Watch catch-up in the event log, then the log and map fill in.`,
    );
    await api("POST", `/cluster/nodes/${follower}/resume`);
    await new Promise((r) => setTimeout(r, 2000));
    const leader = snap.leader;
    narrate(
      `Pausing leader ${leader}. Followers stop hearing heartbeats. After 0.8–1.6s a candidate runs; two votes elect a new leader.`,
    );
    await api("POST", `/cluster/nodes/${leader}/pause`);
    try {
      await waitFor(
        () => snap.leader && snap.leader !== leader,
        6000,
        "no new leader",
      );
      narrate(
        `${snap.leader} is the new leader. A write now goes there. The old leader is frozen until you resume it; a higher term will make it step down.`,
      );
    } catch {
      narrate(
        "No new leader yet — if two nodes are down there is no majority. Resume a paused follower and wait for the election timeout.",
      );
    }
  },
};

$("kv-form").addEventListener("submit", (e) => {
  e.preventDefault();
  kvOp("put").catch((err) => {
    $("kv-result").textContent = String(err.message || err);
  });
});

$("kv-form").addEventListener("click", (e) => {
  const op = e.target.closest("button")?.dataset.op;
  if (!op || op === "put") return;
  kvOp(op).catch((err) => {
    $("kv-result").textContent = String(err.message || err);
  });
});

$("nodes").addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const id = btn.dataset.node;
  const act = btn.dataset.act;
  api("POST", `/cluster/nodes/${id}/${act}`).catch((err) => {
    narrate(String(err.message || err));
  });
});

document.querySelector(".scenarios").addEventListener("click", (e) => {
  const name = e.target.closest("button")?.dataset.scenario;
  if (!name || !scenarios[name]) return;
  withScenario(scenarios[name]);
});

$("resume-all").addEventListener("click", () => {
  resumeAll().catch((err) => narrate(String(err.message || err)));
});

function applySnap(next) {
  snap = next;
  render();
}

fetch("/cluster/snapshot")
  .then((r) => r.json())
  .then(applySnap)
  .catch(() => {});

const src = new EventSource("/cluster/stream");
src.onmessage = (e) => {
  try {
    applySnap(JSON.parse(e.data));
  } catch {
    /* ignore truncated frames */
  }
};
src.onerror = () => {
  $("pulse").classList.remove("has-leader");
  $("pulse").textContent = "lost lab stream; retrying…";
};
