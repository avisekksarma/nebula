We are building a distributed replicated key-value store from scratch in Python as a learning project.

Goal:
- Learn distributed systems by progressively implementing the system.
- Eventually have a 3-node replicated KV store using Raft.
- We have only 3 days, so keep the implementation focused and avoid unnecessary features.

Current stage:
- Three nodes elect a leader. Leader appends PUT/DELETE to the log and replicates via POST /internal/append (prev_log_index / prev_log_term, entries).
- Majority (2 of 3) advances commit_index. Committed entries are applied to the KV dict (once). Followers learn commit_index from append/heartbeat.
- Leader tracks next_index per follower. Prefix mismatch: decrement and retry. Conflicting uncommitted suffix is replaced.
- Raft log is a per-node JSONL WAL (flush + fsync). Term and votedFor are in raft.json. After restart, KV is rebuilt only when the leader sends leader_commit (do not replay the whole WAL).
- Local snapshot: POST /snapshot writes last_included_index/term + applied KV, then compacts the WAL prefix. Restart loads snapshot then leftover WAL (do not replay 1..N). If a follower is behind the snapshot, the leader sends POST /internal/install_snapshot, the follower installs that KV, then AppendEntries continues from last_included_index + 1.
- Linearizable GET: followers 409. Leader sends POST /internal/read_confirm (term + leader_id only; no log/KV change). Self + acks must be a majority, else 503. Higher term on a reply uses existing step-down. Value comes from committed _store only.

Development philosophy:
1. Do NOT implement future distributed-system features unless explicitly asked.
2. Keep the code simple and easy to understand.
3. Prefer the simplest implementation over production-level abstractions.
4. We are progressing problem-by-problem:
   problem → understand why we need something → implement → test → observe next problem.
5. Don't over-engineer or create abstractions for hypothetical future requirements.
6. When suggesting a change, explain briefly what problem it solves and why we need it NOW.
7. Do not generate the entire distributed KV store or Raft implementation upfront.

Eventually our progression will be roughly:
single-node KV
→ multiple nodes / node-to-node communication
→ replication
→ leader election
→ Raft log replication
→ commit/apply to state machine
→ failure testing
→ persistence/recovery if time permits.

For now, stay at log conflict repair (next_index + suffix replace) unless I explicitly ask to move forward.

Also, I am using ChatGPT separately to learn the distributed-systems concepts, so don't turn this into a long theory lesson. Help me implement and debug the current stage.