# Design decisions

Each entry says what was chosen, what was rejected, and why. This file feeds the README and interview answers.

## Kickoff decisions (2026-11-14)

### D1: Windows + WSL2, everything in containers
- **Chosen:** Docker is the only required local install. The solver is built in a multi-stage Dockerfile. Repo lives in the WSL filesystem. WSL memory is capped in `.wslconfig`.
- **Rejected:** installing Kafka, Temporal, Airflow and a C++ toolchain on the host (hard to reproduce, hard for a reviewer to run).
- **Consequence:** a cloud pair session has no Docker daemon, so Compose, kind and Helm steps are verified on the owner's machine.

### D2: Recovery is a ladder, not "refine the grid and roll back"
- **Problem:** a checkpoint from one grid does not fit a finer grid, so "refine and roll back" is impossible without interpolating fields.
- **Chosen:** (1) roll back to the last valid checkpoint and switch BGK to TRT collision (same grid, same viscosity, same physics); (2) if that fails, refine the grid 1.5× and restart from step 0 with a new cost estimate and approval if needed.
- **Rejected:** only the refine step (it is a restart, not a rollback); changing inlet velocity or viscosity (changes the physics); interpolating between grids (CFD work outside the project's scope).
- **Cost note:** a 1.5× finer grid needs about 1.5× more steps, so cost grows about 3.4×. Most refine recoveries will go back through approval.

### D3: Phase 4 core is the recovery agent; review agent and eval gate are stretch
- **Chosen:** recovery agent, rule checker, MCP server, replay scoreboard. Claude API with a pinned model, a token cap per recovery, a few dollars budget for a full replay.
- **Rejected:** building all three agent pieces in one week (the most likely phase to overrun).

### D4: PyIceberg writes with a Postgres SQL catalog; DuckDB reads
- **Chosen:** PyIceberg for writes, SQL catalog in a separate database of the shared Postgres container, DuckDB for reads (pointed at the table's metadata file; verify).
- **Rejected:** a REST catalog container (more realistic, but more RAM on a 16 GB laptop).

### D5: Private interview preparation stays out of the repo
- **Chosen:** `notes/` is gitignored. The plan describes production practices in general terms.
- **Commits:** carry a Claude co-author trailer. Chosen form: `Co-Authored-By: Claude <noreply@anthropic.com>`, so GitHub lists "claude" as a co-author the same way as the owner's earlier commits. The model name is left out so the trailer stays the same across model changes. The owner can change this.

### D6: Kubernetes after Phase 2 (Phase 2.5), for our own workers only
- **Chosen:** `kind` + Helm for worker, detector and sink. Kafka, Temporal, SeaweedFS, Postgres stay in Compose. Airflow is not running yet, so memory is available.
- **Rejected:** Kubernetes from day one (four new technologies at once; Airflow and Kafka charts are heavy on 16 GB; Kubernetes problems would hide the concepts being learned); Kubernetes only as a stretch goal (owner wants it earlier).
- **Not in scope:** operators, ingress, service mesh, autoscalers.

## Risks found at kickoff and how the plan handles them

| Risk | Handling |
|---|---|
| Refined grid cannot load the old checkpoint | D2 |
| Newest checkpoint may already be unstable | Checkpoint sidecar with a `valid` flag; detector alert carries `first_growth_step`; rollback uses the newest valid checkpoint before it |
| Unstable demo might diverge before any checkpoint exists | The unstable spec must diverge after at least two checkpoints; Phase 0 "done when" checks it |
| Airflow may merge several asset events into one DAG run | `run_rollup` reads every triggering event and is idempotent per `run_id` |
| Iceberg needs a catalog | D4 |
| "Same final field after restart" needs exact repeatability | Single thread, no `-ffast-math`, raw binary checkpoints; bit-for-bit test |
| Memory on 16 GB | Compose profiles, `.wslconfig` limit, kind and Airflow never run together, measured in `docs/sizing.md` |
| Phase 4 overrun | D3 |
| `extend_steps` cannot change a running solver | The update restarts the solver from the latest checkpoint with the new target |
| Detector window is lost on a consumer-group rebalance | Accepted: an alert can be late by up to K samples; noted in the plan |
