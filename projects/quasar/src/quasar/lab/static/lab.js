const NODE_IDS = ["A", "B", "C"];
const DONE_KEY = "quasar-lab-done";
const HOW_KEY = "quasar-lab-seen-how";

let snap = { leader: null, nodes: {} };
let session = null;
let runGen = 0;
let stepping = false;
let flashes = new Map();

const $ = (id) => document.getElementById(id);

function doneSet() {
  try {
    return new Set(JSON.parse(localStorage.getItem(DONE_KEY) || "[]"));
  } catch {
    return new Set();
  }
}

function markDone(id) {
  const s = doneSet();
  s.add(id);
  localStorage.setItem(DONE_KEY, JSON.stringify([...s]));
}

function asHot(v) {
  if (!v) return [];
  return (Array.isArray(v) ? v : [v]).filter(Boolean);
}

function currentCase() {
  return CASES.find((c) => c.id === session?.caseId) || null;
}

function isFlash(key) {
  const until = flashes.get(key);
  return Boolean(until && until > Date.now());
}

function noteFlash(key) {
  flashes.set(key, Date.now() + 1600);
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
    throw new Error(
      typeof detail === "string" ? detail : JSON.stringify(detail || res.statusText),
    );
  }
  return data;
}

function nodeState(id) {
  return snap.nodes?.[id] || { node: id, status: "down", reachable: false };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function waitFor(pred, timeoutMs, label) {
  const mine = runGen;
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      if (runGen !== mine) {
        reject(new Error("stopped"));
        return;
      }
      if (pred()) {
        resolve();
        return;
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error(label || "timed out"));
        return;
      }
      setTimeout(tick, 120);
    };
    tick();
  });
}

function followers() {
  return NODE_IDS.map(nodeState).filter(
    (n) => n.reachable && !n.paused && n.role === "follower",
  );
}

async function ensureLive() {
  for (const id of NODE_IDS) {
    if (nodeState(id).paused) {
      try {
        await api("POST", `/cluster/nodes/${id}/resume`);
      } catch {
        /* ignore */
      }
    }
  }
  try {
    await waitFor(
      () => Boolean(snap.leader) && followers().length >= 2,
      4500,
    );
  } catch {
    await api("POST", "/cluster/reset");
    await waitFor(
      () => Boolean(snap.leader) && followers().length >= 2,
      8000,
      "cluster not ready",
    );
  }
}

function setFocus(text) {
  $("focus").textContent = text;
}

function setLive(text) {
  $("dbg-live").textContent = text || "";
}

const CASES = [
  {
    id: "election",
    n: "01",
    title: "Election",
    blurb: "Fresh cluster. Three followers, random timeouts. Step once to wipe, then guess who wins — then watch.",
    lesson:
      "You cannot name the leader in advance. Election timeout is random (~0.8–1.6s). A candidate needs 2 of 3 votes. Guessing this run's winner is luck.",
    steps: [
      {
        label: "Reset",
        say: "Resetting. Watch every node come up as a follower — no leader yet.",
        hot: () => NODE_IDS,
        async act() {
          await api("POST", "/cluster/reset");
          await sleep(250);
        },
        note: () =>
          "Empty logs, maps empty, term will tick on the first timeout.",
      },
      {
        label: "Elect",
        say: "Waiting on election timeout. A follower becomes candidate, then someone needs a majority.",
        hot: () => NODE_IDS,
        predict: {
          ask: "Which node becomes leader?",
          options: [
            { id: "A", label: "A" },
            { id: "B", label: "B" },
            { id: "C", label: "C" },
            { id: "random", label: "Can't know — whoever times out first" },
          ],
          ok: (choice, ctx) => choice === "random" || choice === ctx.leader,
        },
        async act(ctx) {
          await waitFor(() => Boolean(snap.leader), 8000, "no leader");
          ctx.leader = snap.leader;
          ctx.term = nodeState(snap.leader).term;
        },
        note: (ctx) => {
          const lucky =
            session.pick &&
            session.pick === ctx.leader &&
            session.pick !== "random";
          return `${ctx.leader} won term ${ctx.term}.${
            lucky ? " You named them — luck. The timeout is random." : ""
          }`;
        },
      },
    ],
  },
  {
    id: "write",
    n: "02",
    title: "Write path",
    blurb: "A client PUT is not “update every dict.” Step through append → replicate → majority commit → apply.",
    lesson:
      "GET reads the map, not the uncommitted tail. The map moves only after a majority has the log entry.",
    steps: [
      {
        label: "Ready",
        say: "Need a live leader and two followers.",
        hot: () => (snap.leader ? [snap.leader] : NODE_IDS),
        async act() {
          await ensureLive();
        },
        note: () =>
          `${snap.leader} is leader. Look at commit vs applied vs the map — they should match before the write.`,
      },
      {
        label: "PUT",
        say: "PUT x=10 to the leader. Watch the log chip appear, then commit, then the map.",
        hot: () => (snap.leader ? [snap.leader] : []),
        predict: {
          ask: "What happens to the log and the map?",
          options: [
            { id: "map", label: "Every map gets x=10 immediately" },
            { id: "log-then-map", label: "Log first, majority commits, then apply" },
            { id: "leader-only", label: "Only the leader map changes" },
          ],
          ok: (choice) => choice === "log-then-map",
        },
        async act(ctx) {
          ctx.leader = snap.leader;
          const put = await api("PUT", "/cluster/kv/x", { value: "10" });
          ctx.body = put.body || {};
          ctx.idx = ctx.body.log && ctx.body.log.index;
          await waitFor(
            () => NODE_IDS.some((id) => nodeState(id).store?.x === "10"),
            2500,
          ).catch(() => {});
        },
        note: (ctx) => {
          const maps = NODE_IDS.filter((id) => nodeState(id).store?.x === "10");
          const committed = ctx.body.commit_index >= ctx.idx;
          return committed
            ? `Entry #${ctx.idx} committed. Maps with x=10: ${maps.join(", ") || "none yet"}.`
            : `On the log (#${ctx.idx}) but commit_index is ${ctx.body.commit_index}.`;
        },
      },
    ],
  },
  {
    id: "follower-crash",
    n: "03",
    title: "Majority",
    blurb: "Kill one follower, then write. Majority of 3 is 2 — see if the cluster still commits.",
    lesson:
      "Majority is 2 of 3. Leader + one live follower is enough. One dead node does not stall writes.",
    steps: [
      {
        label: "Ready",
        say: "Bring the cluster to leader + two followers.",
        async act() {
          await ensureLive();
        },
        note: () => `${snap.leader} leading. Two followers up.`,
      },
      {
        label: "Pause",
        say: "Pausing one follower (SIGSTOP). Heartbeats to it will fail fast.",
        hot: (ctx) => [ctx.frozen],
        async act(ctx) {
          if (!followers().length) throw new Error("need a follower");
          ctx.frozen = followers()[0].node;
          await api("POST", `/cluster/nodes/${ctx.frozen}/pause`);
          await waitFor(() => nodeState(ctx.frozen).paused, 2000);
        },
        note: (ctx) =>
          `${ctx.frozen} is frozen. ${snap.leader} + the other follower are still a majority.`,
      },
      {
        label: "PUT",
        say: "Write a new key while one replica is dead.",
        hot: (ctx) => [snap.leader, ctx.frozen].filter(Boolean),
        predict: {
          ask: "Can the leader still commit this write?",
          options: [
            { id: "yes", label: "Yes — 2 of 3 is enough" },
            { id: "no", label: "No — all three must ack" },
            { id: "reject", label: "The leader rejects the PUT" },
          ],
          ok: (choice) => choice === "yes",
        },
        async act(ctx) {
          ctx.key = `f${Date.now() % 100000}`;
          const put = await api("PUT", `/cluster/kv/${ctx.key}`, { value: "ok" });
          ctx.body = put.body || {};
          ctx.idx = ctx.body.log && ctx.body.log.index;
          await sleep(400);
        },
        note: (ctx) => {
          const committed = ctx.body.commit_index >= ctx.idx;
          const failed = (ctx.body.log_replication_failed || []).join(", ");
          return committed
            ? `Committed #${ctx.idx}. Failed replicas: ${failed || "none"}. ${ctx.frozen} never needed to ack.`
            : `Did not commit (commit_index=${ctx.body.commit_index}, log #${ctx.idx}).`;
        },
      },
    ],
  },
  {
    id: "leader-crash",
    n: "04",
    title: "Leader down",
    blurb: "Pause the leader. Followers stop hearing heartbeats. Then wait — don't skip the wait.",
    lesson:
      "No heartbeat → election timeout → candidate increments term, votes for itself, needs one more vote. Nobody is appointed without a majority.",
    steps: [
      {
        label: "Ready",
        say: "Need a leader to kill.",
        async act() {
          await ensureLive();
        },
        note: () => `${snap.leader} is leader. Next step freezes them.`,
      },
      {
        label: "Pause",
        say: "Pausing the leader. Heartbeats stop. Watch followers' clocks (term stays until timeout).",
        hot: (ctx) => [ctx.old],
        async act(ctx) {
          ctx.old = snap.leader;
          await api("POST", `/cluster/nodes/${ctx.old}/pause`);
          await waitFor(() => nodeState(ctx.old).paused, 2000);
        },
        note: (ctx) =>
          `${ctx.old} frozen. The others will time out — this is not instant.`,
      },
      {
        label: "Elect",
        say: "Waiting for a new leader. Look for a candidate, then a new id in the header.",
        hot: (ctx) => NODE_IDS.filter((id) => id !== ctx.old),
        predict: {
          ask: "What happens next?",
          options: [
            { id: "stuck", label: "The cluster stays leaderless" },
            { id: "elect", label: "A follower times out and wins a vote" },
            { id: "promote", label: "A follower becomes leader with no vote" },
          ],
          ok: (choice) => choice === "elect",
        },
        async act(ctx) {
          await waitFor(
            () => snap.leader && snap.leader !== ctx.old,
            7000,
            "no new leader",
          );
          ctx.neu = snap.leader;
          ctx.term = nodeState(snap.leader).term;
        },
        note: (ctx) =>
          `${ctx.neu} is leader (term ${ctx.term}). ${ctx.old} is still frozen.`,
      },
    ],
  },
  {
    id: "stale-leader",
    n: "05",
    title: "Stale leader",
    blurb: "Elect a new leader, then wake the old one. Two leaders is the bug this step exists to kill.",
    lesson:
      "A higher term on a heartbeat or append makes the stale leader step down. Resume is not a RAM wipe — restart would be.",
    steps: [
      {
        label: "Ready",
        say: "Need a live leader before we can make them stale.",
        async act() {
          await ensureLive();
        },
        note: () => `${snap.leader} leading.`,
      },
      {
        label: "Pause",
        say: "Pause the current leader so the others elect.",
        hot: (ctx) => [ctx.old],
        async act(ctx) {
          ctx.old = snap.leader;
          await api("POST", `/cluster/nodes/${ctx.old}/pause`);
          await waitFor(
            () => snap.leader && snap.leader !== ctx.old,
            7000,
            "no new leader",
          );
          ctx.neu = snap.leader;
        },
        note: (ctx) =>
          `${ctx.neu} won. ${ctx.old} still thinks it is leader — it just cannot talk.`,
      },
      {
        label: "Resume",
        say: "Waking the old leader. Watch its role badge, not the log.",
        hot: (ctx) => [ctx.old, ctx.neu],
        predict: {
          ask: "What happens to the old leader?",
          options: [
            { id: "dual", label: "Two leaders stay active" },
            { id: "step", label: "It sees a higher term and steps down" },
            { id: "wipe", label: "It wipes its log and starts empty" },
          ],
          ok: (choice) => choice === "step",
        },
        async act(ctx) {
          await api("POST", `/cluster/nodes/${ctx.old}/resume`);
          await sleep(1500);
          ctx.role = nodeState(ctx.old).role;
        },
        note: (ctx) =>
          `${ctx.old} is now ${ctx.role}. Current leader is ${snap.leader || ctx.neu}.`,
      },
    ],
  },
  {
    id: "uncommitted",
    n: "06",
    title: "Uncommitted",
    blurb: "Pause both followers, then PUT. The interesting part is what does not move: commit and the map.",
    lesson:
      "Appended ≠ committed ≠ applied. One ack of two required leaves the entry on the leader log. GET still misses it.",
    steps: [
      {
        label: "Ready",
        say: "Need a leader and two followers, then we freeze the majority away.",
        async act() {
          await ensureLive();
        },
        note: () => `${snap.leader} leading, both followers live.`,
      },
      {
        label: "Pause",
        say: "Freezing both followers. Only the leader can append locally.",
        hot: (ctx) => ctx.frozen || [],
        async act(ctx) {
          if (followers().length < 2) throw new Error("need two followers");
          ctx.frozen = followers().map((n) => n.node);
          await api("POST", `/cluster/nodes/${ctx.frozen[0]}/pause`);
          await api("POST", `/cluster/nodes/${ctx.frozen[1]}/pause`);
          await waitFor(
            () => ctx.frozen.every((id) => nodeState(id).paused),
            2000,
          );
        },
        note: (ctx) =>
          `${ctx.frozen.join(" and ")} frozen. Majority is gone.`,
      },
      {
        label: "PUT",
        say: "PUT while alone. Look at the leader log vs commit vs map.",
        hot: () => (snap.leader ? [snap.leader] : []),
        predict: {
          ask: "Is that entry committed?",
          options: [
            { id: "yes", label: "Yes — the leader already has it" },
            { id: "no", label: "On the leader log, not committed or applied" },
            { id: "drop", label: "The PUT is rejected" },
          ],
          ok: (choice) => choice === "no",
        },
        async act(ctx) {
          ctx.key = `u${Date.now() % 100000}`;
          const put = await api("PUT", `/cluster/kv/${ctx.key}`, {
            value: "stuck",
          });
          ctx.body = put.body || {};
          ctx.idx = ctx.body.log && ctx.body.log.index;
          await sleep(450);
          ctx.committed = ctx.body.commit_index >= ctx.idx;
          ctx.applied = NODE_IDS.some(
            (id) => nodeState(id).store?.[ctx.key] === "stuck",
          );
        },
        note: (ctx) =>
          `Log #${ctx.idx} exists. Committed: ${ctx.committed ? "yes" : "no"}. In a map: ${
            ctx.applied ? "yes" : "no"
          }.`,
      },
    ],
  },
];

function diffSnaps(prev, next) {
  if (!prev?.nodes || !next?.nodes) return;
  for (const id of NODE_IDS) {
    const a = prev.nodes[id];
    const b = next.nodes[id];
    if (!a || !b) continue;
    if (a.role !== b.role) noteFlash(`${id}:role`);
    if (a.term !== b.term) noteFlash(`${id}:term`);
    if (a.commit_index !== b.commit_index) noteFlash(`${id}:commit`);
    if (a.last_applied !== b.last_applied) noteFlash(`${id}:applied`);
    if (a.snapshot_index !== b.snapshot_index) noteFlash(`${id}:snap`);
    const oldIdx = new Set(
      (Array.isArray(a.log) ? a.log : []).map((e) => Number(e.index)),
    );
    for (const e of Array.isArray(b.log) ? b.log : []) {
      if (!oldIdx.has(Number(e.index))) noteFlash(`${id}:log:${e.index}`);
    }
    const oldStore = a.store && typeof a.store === "object" ? a.store : {};
    const newStore = b.store && typeof b.store === "object" ? b.store : {};
    for (const k of Object.keys(newStore)) {
      if (oldStore[k] !== newStore[k]) noteFlash(`${id}:kv:${k}`);
    }
  }
}

function logChips(id, n) {
  const snapIdx = Number(n.snapshot_index || 0);
  const commit = Number(n.commit_index || 0);
  const applied = Number(n.last_applied || 0);
  const entries = Array.isArray(n.log) ? n.log : [];
  const chips = [];
  if (snapIdx > 0) chips.push(`<span class="chip snap">snap≤${snapIdx}</span>`);
  if (entries.length === 0 && snapIdx === 0) {
    chips.push(`<span class="chip">empty</span>`);
    return chips.join("");
  }
  for (const e of entries) {
    const idx = Number(e.index);
    let cls = "uncommitted";
    if (idx <= applied) cls = "applied";
    else if (idx <= commit) cls = "committed";
    const flash = isFlash(`${id}:log:${idx}`) ? " flash" : "";
    chips.push(
      `<span class="chip ${cls}${flash}">${idx}:t${e.term} ${e.operation} ${e.key}</span>`,
    );
  }
  return chips.join("");
}

function storeChips(id, n) {
  const store = n.store && typeof n.store === "object" ? n.store : {};
  const keys = Object.keys(store);
  if (keys.length === 0) return `<span class="chip">empty</span>`;
  return keys
    .map((k) => {
      const flash = isFlash(`${id}:kv:${k}`) ? " flash" : "";
      return `<span class="chip applied${flash}">${k}=${store[k]}</span>`;
    })
    .join("");
}

function roleOf(n) {
  const paused = Boolean(n.paused);
  const down = n.status === "down" || (!n.reachable && !paused);
  if (paused) return "paused";
  if (down) return "down";
  return n.role || "unknown";
}

function ensureCards() {
  const root = $("cluster");
  if (root.children.length === NODE_IDS.length) return;
  root.innerHTML = NODE_IDS.map(
    (id) => `
    <article class="node" data-node="${id}">
      <div class="head">
        <h2>${id}<span class="port" data-f="port"></span></h2>
        <span class="badge" data-f="role"></span>
      </div>
      <div class="stats">
        <div><span>term</span><b data-f="term"></b></div>
        <div><span>commit</span><b data-f="commit"></b></div>
        <div><span>applied</span><b data-f="applied"></b></div>
        <div><span>snap</span><b data-f="snap"></b></div>
      </div>
      <div class="actions">
        <button type="button" data-act="pause" data-node="${id}">Pause</button>
        <button type="button" data-act="resume" data-node="${id}">Resume</button>
        <button type="button" data-act="restart" data-node="${id}">Restart</button>
      </div>
      <p class="strip-label">Log</p>
      <div class="strip" data-f="log"></div>
      <p class="strip-label">Map</p>
      <div class="strip map" data-f="store"></div>
    </article>`,
  ).join("");
}

function updateNode(el, id) {
  const n = nodeState(id);
  const role = roleOf(n);
  const paused = role === "paused";
  const down = role === "down";
  const hot = asHot(session?.hot);
  const dim = hot.length > 0 && !hot.includes(id);
  el.className = [
    "node",
    role === "leader" ? "is-leader" : "",
    role === "candidate" ? "is-candidate" : "",
    paused ? "is-paused" : "",
    down ? "is-down" : "",
    hot.includes(id) ? "is-hot" : "",
    dim ? "is-dim" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const badge = el.querySelector("[data-f=role]");
  badge.textContent = role;
  badge.className = `badge ${role}${isFlash(`${id}:role`) ? " flash" : ""}`;

  el.querySelector("[data-f=port]").textContent = n.port ? `:${n.port}` : "";

  const setStat = (field, key, value) => {
    const b = el.querySelector(`[data-f=${field}]`);
    b.textContent = value;
    b.classList.toggle("flash", isFlash(`${id}:${key}`));
  };
  setStat("term", "term", n.term ?? "—");
  setStat("commit", "commit", n.commit_index ?? 0);
  setStat("applied", "applied", n.last_applied ?? 0);
  setStat("snap", "snap", n.snapshot_index || "—");

  const locked = Boolean(session) && session.phase !== "done";
  const pauseBtn = el.querySelector("[data-act=pause]");
  const resumeBtn = el.querySelector("[data-act=resume]");
  pauseBtn.disabled = locked || paused || down;
  resumeBtn.disabled = locked || !paused;
  el.querySelector("[data-act=restart]").disabled = locked;

  const logEl = el.querySelector("[data-f=log]");
  const logHtml = logChips(id, n);
  if (logEl.dataset.sig !== logHtml) {
    logEl.dataset.sig = logHtml;
    logEl.innerHTML = logHtml;
  }

  const storeEl = el.querySelector("[data-f=store]");
  const storeHtml = storeChips(id, n);
  if (storeEl.dataset.sig !== storeHtml) {
    storeEl.dataset.sig = storeHtml;
    storeEl.innerHTML = storeHtml;
  }
}

function renderCluster() {
  ensureCards();
  for (const id of NODE_IDS) {
    updateNode($("cluster").querySelector(`[data-node="${id}"]`), id);
  }
  const el = $("status");
  if (!snap.leader) {
    el.classList.remove("has-leader");
    el.textContent = "no leader";
  } else {
    el.classList.add("has-leader");
    el.textContent = `${snap.leader} is leader`;
  }
}

function renderMissions() {
  const done = doneSet();
  $("cleared").textContent = `${done.size}/${CASES.length}`;
  $("cleared").classList.toggle("ready", done.size === CASES.length);
  const hint = !session
    ? "Start with 01"
    : session.phase === "intro"
      ? "Then press Start below"
      : session.phase === "done"
        ? "Next case, or Again"
        : "Follow “Do this now” below";
  $("missions").innerHTML =
    `<h2>Cases</h2><p class="rail-hint">${hint}</p>` +
    CASES.map((c) => {
      const cls = [
        "mission",
        done.has(c.id) ? "done" : "",
        session?.caseId === c.id ? "active" : "",
      ]
        .filter(Boolean)
        .join(" ");
      return `<button type="button" class="${cls}" data-case="${c.id}" ${
        session && session.phase !== "done" && session.phase !== "intro"
          ? "disabled"
          : ""
      } title="${c.blurb}"><span class="n">${c.n}</span><span class="name">${
        c.title
      }</span><span class="mark">${done.has(c.id) ? "✓" : ""}</span></button>`;
    }).join("");
}

function stepLabel() {
  if (!session) return "Start";
  if (session.phase === "intro") return "Start";
  if (session.phase === "done") return "Again";
  return "Step";
}

function howOpen() {
  return !$("how").classList.contains("hidden");
}

function showHow() {
  $("how").classList.remove("hidden");
  localStorage.setItem(HOW_KEY, "1");
}

function hideHow() {
  $("how").classList.add("hidden");
}

function paintCoach() {
  document.querySelectorAll(".pulse").forEach((el) => el.classList.remove("pulse"));
  if (!session) {
    $("missions").querySelector("[data-case]")?.classList.add("pulse");
    setFocus("Click 01 Election on the left.");
    return;
  }
  if (session.phase === "intro") {
    $("dbg-step").classList.add("pulse");
    setFocus("Press Start (gold button under the nodes).");
    return;
  }
  if (session.phase === "wait-predict" && !session.pick && !session.skipThis) {
    setFocus("Pick an answer in the bar below — or Skip guess.");
    return;
  }
  if (session.phase === "running") {
    setFocus("Watch the highlighted node. Do not click yet.");
    return;
  }
  if (session.phase === "inspect") {
    $("dbg-step").classList.add("pulse");
    setFocus("Look at A / B / C, then press Step.");
    return;
  }
  if (session.phase === "wait-predict" && session.pick) {
    $("dbg-step").classList.add("pulse");
    setFocus("Press Step to run that guess on the cluster.");
    return;
  }
  if (session.phase === "done") {
    const nxt = CASES.find((x) => !doneSet().has(x.id));
    if (nxt) {
      $("missions")
        .querySelector(`[data-case="${nxt.id}"]`)
        ?.classList.add("pulse");
      setFocus(`Click ${nxt.n} ${nxt.title}, or press Again.`);
    } else {
      $("dbg-step").classList.add("pulse");
      setFocus("All cases cleared. Press Again to replay one.");
    }
  }
}

function renderDots() {
  const c = currentCase();
  const ol = $("dbg-dots");
  if (!c || !session) {
    ol.innerHTML = "";
    return;
  }
  ol.innerHTML = c.steps
    .map((s, i) => {
      let cls = "";
      if (session.phase === "done") {
        cls = "did";
      } else if (session.phase === "inspect") {
        if (i < session.i - 1) cls = "did";
        else if (i === session.i - 1) cls = "now";
      } else if (session.phase === "intro") {
        if (i === 0) cls = "now";
      } else if (i < session.i) {
        cls = "did";
      } else if (i === session.i) {
        cls = "now";
      }
      return `<li class="${cls}" title="${s.label}">${i + 1} ${s.label}</li>`;
    })
    .join("");
}

function renderDebug() {
  const c = currentCase();
  const steppingNow = Boolean(session && (session.phase === "running" || session.auto));
  const needGuess =
    session &&
    session.phase === "wait-predict" &&
    !session.pick &&
    !session.skipThis;
  $("dbg-step").disabled = !session || steppingNow || needGuess;
  $("dbg-step").textContent = stepLabel();
  $("dbg-go").disabled =
    !session || steppingNow || session.phase === "done" || needGuess;
  $("dbg-stop").disabled = !session;
  $("debug").classList.toggle("busy", Boolean(session) && session.phase !== "done");
  renderDots();

  const main = $("dbg-main");
  if (!session) {
    main.innerHTML = `
      <p class="next">Do this now: click 01 Election on the left.</p>
      <ol class="play">
        <li class="go">Click a case</li>
        <li>Press Start (gold button, this bar)</li>
        <li>Watch A / B / C, then press Step</li>
        <li>If it asks, guess — or Skip guess</li>
      </ol>`;
    setLive("");
  } else if (session.phase === "intro") {
    main.innerHTML = `<p class="next">Do this now: press Start.</p><p>${c.blurb}</p>`;
  } else if (session.phase === "wait-predict") {
    const step = c.steps[session.i];
    const opts = step.predict.options
      .map(
        (o) =>
          `<button type="button" data-opt="${o.id}" class="${
            session.pick === o.id ? "picked" : ""
          }">${o.label}</button>`,
      )
      .join("");
    const next = session.pick
      ? "Do this now: press Step to see what the cluster does."
      : "Do this now: pick an answer (or Skip guess).";
    main.innerHTML = `<p class="next">${next}</p><p class="ask">${step.predict.ask}</p><div class="options">${opts}<button type="button" class="ghost" data-skip="1">Skip guess</button></div>`;
  } else if (session.phase === "running") {
    const step = c.steps[session.i];
    main.innerHTML = `<p class="next">Watch the highlighted node.</p><p>${step.say || step.label}</p>`;
  } else if (session.phase === "inspect") {
    const fb = session.feedback;
    let extra = "";
    if (fb && fb.kind === "ok") {
      extra = `<p class="note ok"><b>Yes.</b> ${fb.text}</p>`;
    } else if (fb && fb.kind === "bad") {
      extra = `<p class="note bad"><b>Not quite.</b> ${fb.text}</p>`;
    } else if (fb && fb.kind === "skip") {
      extra = `<p class="note plain">${fb.text}</p>`;
    }
    main.innerHTML = `<p class="next">Look at the nodes, then press Step.</p><p>${session.note}</p>${extra}`;
  } else if (session.phase === "done") {
    const scored = session.hits + session.misses;
    const score =
      scored === 0
        ? "You watched. Press Again and guess this time, or click the next case."
        : `Guesses ${session.hits}/${scored}.`;
    const nxt = CASES.find((x) => !doneSet().has(x.id));
    const go = nxt
      ? `Cleared. Click ${nxt.n} ${nxt.title} on the left — or press Again.`
      : "All six cleared. Press Again to replay, or How if you forget the controls.";
    main.innerHTML = `<p class="next">${go}</p><p class="note ok">${score}</p><p>${c.lesson}</p>`;
  }
  paintCoach();
}

function renderAll() {
  renderMissions();
  renderCluster();
  renderDebug();
}

function openCase(id) {
  runGen += 1;
  const c = CASES.find((x) => x.id === id);
  if (!c) return;
  session = {
    caseId: id,
    i: 0,
    phase: "intro",
    ctx: {},
    pick: null,
    skipThis: false,
    hits: 0,
    misses: 0,
    hot: [],
    feedback: null,
    note: "",
    auto: false,
  };
  setLive("");
  renderAll();
}

function stopSession() {
  runGen += 1;
  session = null;
  stepping = false;
  setFocus("Click 01 Election on the left.");
  renderAll();
}

function scorePredict(step, ctx) {
  if (!step.predict) {
    session.feedback = null;
    return;
  }
  if (session.skipThis) {
    session.feedback = { kind: "skip", text: "Skipped the guess. Here's what happened." };
    return;
  }
  const choice = session.pick;
  const ok = step.predict.ok(choice, ctx);
  const label =
    step.predict.options.find((o) => o.id === choice)?.label || choice;
  if (ok) {
    session.hits += 1;
    session.feedback = { kind: "ok", text: label };
  } else {
    session.misses += 1;
    session.feedback = { kind: "bad", text: `You picked “${label}”.` };
  }
}

async function stepOnce() {
  if (!session || stepping) return;
  if (session.phase === "done") {
    openCase(session.caseId);
    return;
  }

  const c = currentCase();
  if (session.phase === "wait-predict" && !session.pick && !session.skipThis) {
    return;
  }

  const step = c.steps[session.i];
  if (!step) {
    session.phase = "done";
    session.hot = [];
    markDone(c.id);
    session.auto = false;
    renderAll();
    return;
  }

  if (
    step.predict &&
    session.phase !== "predicted" &&
    session.phase !== "running" &&
    session.phase !== "wait-predict"
  ) {
    session.phase = "wait-predict";
    session.pick = null;
    session.skipThis = false;
    session.hot = asHot(step.hot && step.hot(session.ctx));
    renderAll();
    return "breakpoint";
  }

  if (session.phase === "wait-predict") {
    session.phase = "predicted";
  }

  stepping = true;
  session.phase = "running";
  session.hot = asHot(step.hot && step.hot(session.ctx));
  session.feedback = null;
  const mine = runGen;
  renderAll();
  try {
    await step.act(session.ctx);
    if (runGen !== mine || !session) return;
    session.hot = asHot(step.hot && step.hot(session.ctx));
    scorePredict(step, session.ctx);
    session.note = step.note ? step.note(session.ctx) : step.say || "";
    session.i += 1;
    session.pick = null;
    session.skipThis = false;
    if (session.i >= c.steps.length) {
      session.phase = "done";
      session.hot = [];
      session.auto = false;
      markDone(c.id);
    } else {
      session.phase = "inspect";
    }
  } catch (err) {
    if (runGen !== mine || !session) return;
    session.phase = "inspect";
    session.auto = false;
    session.note = String(err.message || err);
    session.feedback = { kind: "bad", text: session.note };
  } finally {
    stepping = false;
    if (session && runGen === mine) renderAll();
  }
  return session?.phase;
}

async function continueRun() {
  if (!session || stepping || session.auto) return;
  session.auto = true;
  renderDebug();
  while (session && session.auto) {
    const phase = await stepOnce();
    if (!session || !session.auto) break;
    if (phase === "breakpoint" || session.phase === "wait-predict") {
      session.auto = false;
      renderDebug();
      break;
    }
    if (session.phase === "done") break;
    await sleep(1100);
  }
}

$("missions").addEventListener("click", (e) => {
  const id = e.target.closest("[data-case]")?.dataset.case;
  if (!id) return;
  if (session && session.phase !== "done" && session.phase !== "intro") return;
  openCase(id);
});

$("debug").addEventListener("click", (e) => {
  const opt = e.target.closest("[data-opt]");
  if (opt && session && session.phase === "wait-predict") {
    session.pick = opt.dataset.opt;
    session.skipThis = false;
    renderDebug();
    return;
  }
  if (e.target.closest("[data-skip]") && session && session.phase === "wait-predict") {
    session.skipThis = true;
    session.pick = null;
    stepOnce();
  }
});

$("dbg-step").addEventListener("click", () => stepOnce());
$("dbg-go").addEventListener("click", () => continueRun());
$("dbg-stop").addEventListener("click", () => stopSession());

$("how-btn").addEventListener("click", () => {
  if (howOpen()) hideHow();
  else showHow();
});
$("how-close").addEventListener("click", () => hideHow());
$("how").addEventListener("click", (e) => {
  if (e.target.id === "how") hideHow();
});

document.addEventListener("keydown", (e) => {
  if (e.target.matches("input")) return;
  if (e.key === "Escape") {
    if (howOpen()) {
      hideHow();
      return;
    }
    stopSession();
    return;
  }
  if (howOpen()) return;
  if (e.key === "s" || e.key === "S") {
    e.preventDefault();
    stepOnce();
  } else if (e.key === "c" || e.key === "C") {
    e.preventDefault();
    continueRun();
  }
});

$("cluster").addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-act]");
  if (!btn || btn.disabled) return;
  api("POST", `/cluster/nodes/${btn.dataset.node}/${btn.dataset.act}`).catch(
    (err) => setFocus(String(err.message || err)),
  );
});

$("reset").addEventListener("click", () => {
  runGen += 1;
  session = null;
  stepping = false;
  setFocus("Resetting cluster…");
  renderAll();
  api("POST", "/cluster/reset")
    .then(() => setFocus("Click 01 Election on the left."))
    .catch((err) => setFocus(String(err.message || err)));
});

$("kv-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const key = $("kv-key").value.trim();
  if (!key) return;
  api("PUT", `/cluster/kv/${encodeURIComponent(key)}`, {
    value: $("kv-value").value,
  })
    .then((data) => {
      $("kv-out").textContent = JSON.stringify(data);
    })
    .catch((err) => {
      $("kv-out").textContent = String(err.message || err);
    });
});

$("kv-get").addEventListener("click", () => {
  const key = $("kv-key").value.trim();
  if (!key) return;
  api("GET", `/cluster/kv/${encodeURIComponent(key)}`)
    .then((data) => {
      $("kv-out").textContent = JSON.stringify(data);
    })
    .catch((err) => {
      $("kv-out").textContent = String(err.message || err);
    });
});

function applySnap(next) {
  diffSnaps(snap, next);
  snap = next;
  renderCluster();
  if (session?.phase === "running") {
    const leader = snap.leader ? `leader ${snap.leader}` : "no leader";
    const cand = NODE_IDS.filter((id) => nodeState(id).role === "candidate");
    setLive(cand.length ? `${leader} · candidate ${cand.join(",")}` : leader);
  }
}

renderMissions();
renderDebug();
ensureCards();
renderCluster();

if (!localStorage.getItem(HOW_KEY)) {
  showHow();
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
    /* ignore */
  }
};
src.onerror = () => {
  $("status").classList.remove("has-leader");
  $("status").textContent = "lost stream";
};
