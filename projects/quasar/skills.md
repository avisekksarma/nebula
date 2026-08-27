We are building a distributed replicated key-value store from scratch in Python as a learning project.

Goal:
- Learn distributed systems by progressively implementing the system.
- Eventually have a 3-node replicated KV store using Raft.
- We have only 3 days, so keep the implementation focused and avoid unnecessary features.

Current stage:
- Three nodes, manually configured --leader. Only the leader accepts client PUT/DELETE.
- Leader naively broadcasts writes via /internal/kv. GET is local.
- Step 2 of election: followers become candidate after a randomized timeout. No votes, no automatic leader yet.
- There is NO Raft log, commit index, persistence, or Docker yet.

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

For now, stay at candidate-on-timeout (manual --leader, no votes) unless I explicitly ask to move forward.

Also, I am using ChatGPT separately to learn the distributed-systems concepts, so don't turn this into a long theory lesson. Help me implement and debug the current stage.