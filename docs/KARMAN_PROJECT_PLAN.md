# Karman: a self-healing simulation job platform

**One small project that teaches Kafka, Temporal and Airflow together.**
Plan version: 2026-11-14, revision 2 (kickoff decisions D1 to D6 applied). Owner: (your name). Status: Step 1 (review of the plan) done; Step 2 (`docs/BACKLOG.md`) is next.

---

## 0. About this file

**What it is.** A complete build brief for one portfolio project. It is written so that you can hand it to a coding agent (or a human pair) and work through it phase by phase.

**How to use it.**

- Read sections 1 to 4 once to understand the idea and the architecture.
- Build in the order of section 8. Each phase ends in something you can demo.
- Give the agent section 11 ("Instructions for the coding agent") together with the phase you are working on.
- Section 12 lists what to publish at the end (README, demo GIF, LinkedIn post).

**What it addresses.**

| Need | Where it is covered |
|---|---|
| Learn Temporal (durable workflows) | Phase 1, section 5.2 |
| Learn Kafka (event streaming) | Phase 2, section 5.3 |
| Learn Airflow (batch orchestration) | Phase 3, section 5.4 |
| Show agentic AI with guardrails and measured results | Phase 4, section 5.7 |
| Run our own workers on Kubernetes (kind + Helm) | Phase 2.5, section 8 |
| Understand how the three fit together | Sections 3 and 4 |
| Have something interesting to show a hiring manager | Sections 1, 2 and 12 |
| Practise interview questions | "Check yourself" lists in section 8, and section 13 |

### Decisions made at kickoff (revision 2)

These change the original brief. Reasons and rejected options are in `docs/decisions.md`.

| # | Decision | Where it shows up |
|---|---|---|
| D1 | Owner works on Windows 11 + WSL2 (x86). Docker is the only required local install. A cloud pair session has no Docker daemon, so Compose, kind and Helm steps are run by the owner, who pastes the output. | 7, 7.1, 11 |
| D2 | Recovery is a ladder: (1) roll back to the last valid checkpoint and switch the collision model from BGK to TRT; (2) refine the grid and restart from step 0. The original "refine the grid and roll back" cannot work, because a checkpoint does not fit a different grid. | 5.1, 5.2, 8 |
| D3 | Phase 4 core is the recovery agent, rule checker, MCP server and replay scoreboard. The review agent and the evaluation gate become stretch goals. Model provider is the Claude API with a pinned model. | 5.7, 8 |
| D4 | Iceberg tables are written with PyIceberg using a SQL catalog in Postgres. DuckDB only reads. | 5.5, 7 |
| D5 | Private interview preparation stays out of the repo (`notes/` is gitignored). Commits carry the owner's name plus a Claude co-author trailer (can be changed). | 3, 11 |
| D6 | Kubernetes arrives right after Phase 2 as a new Phase 2.5: `kind` + Helm for our own workers only. Kafka, Temporal, SeaweedFS and Airflow stay in Compose. | 8, 9, 10 |

---

## 1. The idea in one paragraph

Engineers submit fluid simulation jobs (2D flow past a cylinder, solved with the Lattice Boltzmann Method). Simulations are long-running and they sometimes blow up numerically. Karman runs each job as a **durable Temporal workflow**, streams solver telemetry through **Kafka**, detects divergence in real time, and **repairs the run automatically**: it rolls back to the last checkpoint, adjusts the parameters, and continues. When runs finish, **Airflow** turns raw telemetry into an analytics table that answers questions such as "which parameter ranges fail most?" and "what did each team's compute cost?".

The name comes from the Kármán vortex street, the wake pattern the simulation produces.

## 2. Why a reader will care

- **It has a demo that takes 30 seconds to understand.** Start a simulation, kill the worker process, and watch the run resume from its checkpoint. Then submit an unstable run and watch the platform notice, roll back and fix it.
- **It is a real platform problem, not a toy to-do app.** Long-running compute jobs, retries, human approval for expensive runs, telemetry, and batch analytics are what simulation and data platforms deal with daily. It maps directly to a company that sells engineering software.
- **The mix is unusual.** An engineering-simulation workload, real-time C++ and agentic AI rarely appear in one small project. The solver is only the workload; the platform around it is the new skill.
- **It answers the "Airflow or Temporal?" question with working code.** Each tool does the job it is good at, and the README says why.
- **The agent is part of the loop and is measured.** A recovery agent uses the history that Airflow builds to decide how to repair a failing run, a rule checker can veto it, and a scoreboard compares it with the plain rule.
- **A search on 2026-11-14 found no public project that combines all three tools.** (GitHub was not searched directly, so check before claiming this publicly.)

## 3. Which tool does which job

| Tool | Job in Karman | Why this tool |
|---|---|---|
| Temporal | Run one simulation from submit to cleanup, surviving crashes | Long, stateful, retryable process with waits and human input |
| Kafka | Carry solver telemetry and lifecycle events to many consumers | Durable, replayable log; consumers are decoupled |
| Airflow | Turn finished runs into analytics tables; daily platform report | Batch pipelines with dependencies, backfills and a UI |
| C++ solver | The heavy compute | Stands in for a distributed compute engine such as Spark |
| S3-compatible store + Iceberg | Checkpoints, artifacts, analytics tables | Stands in for S3 + Hive |
| DuckDB (or Trino) | Query the tables | Stands in for Trino / Looker |

**Production practices Karman mirrors.**

| Practice | Karman equivalent |
|---|---|
| Airflow orchestrates, does not process | DAG tasks call DuckDB or PySpark; no data in the scheduler |
| S3KeySensor waits for a file | Asset watcher on a Kafka topic (event-driven), plus one S3 sensor for comparison |
| XCom passes paths, not data | Tasks pass `s3://…` URIs only |
| Dev / UAT / prod | Compose profiles `dev` and `uat` (stretch) |
| Multi-tenancy for 50+ teams | `tenant_id` on every event, one Temporal task queue per tenant, per-tenant quota |
| Distributed compute (for example Spark on EMR) | C++ solver for compute, optional PySpark local mode for the rollup |

## 4. Architecture

```
 CLI / small web UI
        │ submit job spec (tenant, Reynolds number, grid, steps)
        ▼
 ┌──────────────── Temporal ────────────────┐
 │ SimulationRunWorkflow                    │
 │  validate → [approval if expensive]      │
 │  → run_solver (activity, heartbeats,     │
 │     checkpoints to S3)                   │
 │  → on "diverged" signal: roll back,      │
 │     adjust parameters, retry             │
 │  → publish artifacts → cleanup           │
 └──────┬──────────────────────▲────────────┘
        │ starts               │ signal: diverged
        ▼                      │
 C++ LBM solver ──telemetry──► Kafka
 (subprocess)                  topics: sim.telemetry, sim.lifecycle,
                                       sim.alerts, sim.telemetry.dlq
                                  │
        ┌─────────────────────────┼──────────────────────┐
        ▼                         ▼                      ▼
  divergence detector       lake sink              live dashboard
  (consumer group A)        (consumer group B)     (consumer group C)
  signals Temporal          writes Parquet         progress + residual plot
                            to S3 (bronze)
                                  │
              sim.lifecycle "completed" event
                                  ▼
 ┌──────────────── Airflow 3 ────────────────┐
 │ run_rollup DAG (asset-triggered)          │
 │  check artifacts → build silver table     │
 │  → compute run metrics → quality checks   │
 │ platform_report DAG (daily schedule)      │
 │  per-tenant usage, failure rates, cost    │
 └──────────────────┬────────────────────────┘
                    ▼
        Iceberg tables on S3  ◄── DuckDB / notebook / dashboard
```

## 5. Component design

### 5.1 Solver (C++)

- **Keep this part minimal. No CFD expertise is needed.** The solver is a black box that runs for a few minutes, writes checkpoints, prints telemetry and can be made to fail. The standard textbook example below is enough, and the coding agent can write it. Limit it to two days. If it takes longer, use a Python + NumPy version and move on.
- D2Q9 lattice, BGK collision, bounce-back cylinder, simple inlet and outlet. Grid about 400 × 100 so a run takes one to five minutes on a laptop. The collision model is chosen by the spec field `collision`: `bgk` (default) or `trt` (two-relaxation-time, more stable at the same viscosity, about 30 extra lines).
- CLI: `lbm_solver --spec spec.json --checkpoint-in <path> --checkpoint-out <dir> --checkpoint-every 2000`.
- Every N steps it prints one JSON line to stdout: `run_id, step, residual, max_velocity, mass_drift, lift, drag, wall_ms`.
- Exit codes: `0` done, `10` diverged (NaN or velocity above a limit), `20` bad spec.
- **How to make it fail on purpose:** set the relaxation time close to 0.5, or the inlet velocity too high. BGK becomes unstable there. This is what the platform will detect and repair. The unstable spec must be tuned to diverge **after** at least two checkpoints have been written (otherwise there is nothing to roll back to), and switching to `trt` must be able to rescue it. Tuning this spec is a real task, not a detail.
- **Checkpoints.** Raw binary doubles plus a small JSON sidecar: step, residual, mass drift, and a `valid` flag that the solver sets when residual and mass drift were under their limits at write time. "Last good checkpoint" means the newest one with `valid: true` that was written before the detector's `first_growth_step` (section 5.3). An instability can grow for a while before it turns into NaN, so the newest checkpoint is not always good.
- **Restart is bit-for-bit.** Single thread, no `-ffast-math`, fixed floating-point order. A run restarted from a checkpoint must give the identical final field to an uninterrupted run with the same collision model. This is a test, not a hope.
- Keep it under about 450 lines (TRT adds about 30). The solver is not the point; the platform is.

### 5.2 Temporal (Python SDK)

`SimulationRunWorkflow(spec)`:

1. `validate_spec` activity. Estimate cost from grid size × steps.
2. If the estimate is over a threshold, wait for an `approve` signal with a 24-hour timeout. Auto-reject on timeout.
3. `run_solver` activity. It launches the solver as a subprocess, forwards each telemetry line to Kafka, and **heartbeats** with the latest step and checkpoint path. Start-to-close timeout is long; heartbeat timeout is short (about 30 seconds).
4. If the worker dies, Temporal retries the activity. The new attempt reads the last heartbeat details and resumes from that checkpoint.
5. If a `diverged` signal arrives (from the Kafka detector) or the solver exits with code 10: cancel the activity, call `plan_recovery`, and restart. Maximum three recoveries, then fail with a clear reason. Recovery is a ladder (decision D2): recovery 1 is **rollback + TRT**: restart from the last good checkpoint (section 5.1) with `collision: trt`, same grid and viscosity, so the physics is unchanged and the checkpoint still loads. Recoveries 2 and 3 are **refine + restart**: grid 1.5× finer, restart from step 0. If the spec already uses TRT, start at the refine step.
6. `publish_artifacts`, then emit a `completed` lifecycle event to Kafka.
7. Cleanup always runs (saga-style compensation: delete scratch files, release the tenant quota slot).

Features to exercise, each one small:

| Feature | Use in Karman |
|---|---|
| Signal | `approve`, `diverged`, `cancel` |
| Query | `progress()` returns step, attempt, state |
| Update | `extend_steps(n)` validated, then applied mid-run by restarting the solver from the latest checkpoint with the new target |
| Child workflows | `ParameterSweepWorkflow` runs 5 Reynolds numbers in parallel |
| Schedule | Nightly regression run with a known-good spec |
| Task queues | One per tenant, so one team cannot starve another |
| Determinism | No clock, random or I/O in workflow code; only in activities |

`plan_recovery` starts as a plain rule that follows the ladder above and **keeps the Reynolds number fixed**, so the repaired run still solves the problem the user asked for. Do not "fix" a run by changing inlet velocity or viscosity alone, because that changes the physics. Refining the grid by 1.5× raises the lattice viscosity and moves the relaxation time away from 0.5, but a checkpoint from the coarse grid does not fit the fine grid, so that step restarts from step 0 (interpolating fields between grids is out of scope). It also needs about 1.5× more time steps, so cost grows roughly 3.4×: the recovery re-estimates the cost and goes back through the approval step if it crosses the threshold. Phase 4 adds a recovery agent beside this rule (section 5.7). The rule stays as the fallback and as the baseline the agent is measured against.

### 5.3 Kafka

| Topic | Key | Partitions | Content |
|---|---|---|---|
| `sim.telemetry` | `run_id` | 3 | Solver metrics, high volume |
| `sim.lifecycle` | `run_id` | 1 | submitted, started, recovered, completed, failed |
| `sim.alerts` | `run_id` | 1 | Divergence detections |
| `sim.telemetry.dlq` | `run_id` | 1 | Messages that failed validation |

- Keying by `run_id` keeps one run's events in order on one partition.
- Every message has `schema_version` and `tenant_id`. Validate with Pydantic models on both produce and consume. Bad messages go to the DLQ, never crash the consumer.
- Producer: idempotence on, `acks=all`.
- Three consumer groups, each receiving every event:
  - **Detector.** Sliding window per run. Flags divergence when the residual grows for K consecutive samples or max velocity passes a limit. Publishes to `sim.alerts` and signals the workflow. Must be idempotent: a replay must not signal twice (use the alert's step number as a dedupe key). The alert also carries `first_growth_step`, which recovery uses to pick a checkpoint written before the instability began. The window lives in memory; after a rebalance a replica starts with an empty window, so an alert can be late by up to K samples (accepted and noted in `docs/decisions.md`).
  - **Lake sink.** Batches telemetry into Parquet files on S3, partitioned by date and tenant. Commits offsets only after the file is written (at-least-once), and names files by topic-partition-offset range so a retry overwrites rather than duplicates.
  - **Dashboard.** Pushes progress to the browser over WebSocket or SSE.
- Write a short sizing note in the README: messages per run, bytes per message, events per second at 100 concurrent runs.

### 5.4 Airflow 3

**`run_rollup` (event-driven).** Triggered by an Asset whose watcher listens to `sim.lifecycle` for `completed` events. Aim for one DAG run per finished simulation, but Airflow may combine several asset events into one run (verify how 3.4 batches them). So the DAG reads every triggering event, whose payload carries `run_id`, and processes each run. Every task is idempotent per `run_id`, so merged or repeated events are harmless.

1. `check_artifacts`: confirm checkpoint and bronze files exist (S3 sensor, deferrable).
2. `build_silver`: clean and deduplicate that run's telemetry into an Iceberg table.
3. `compute_metrics`: steps to convergence, mean drag, Strouhal number from the lift signal, recoveries used, compute seconds.
4. `quality_checks`: row counts, no nulls in keys, mass drift within tolerance. Fail loudly.
5. `publish`: upsert one row into `gold.run_summary`.

**`platform_report` (daily schedule).** Per-tenant runs, failure rate, recoveries, compute cost. Supports backfill for past dates.

Rules to follow: XCom carries S3 URIs only. Tasks are idempotent (re-running a task for the same run gives the same table). Retries with backoff on every task. Use the Task SDK imports, not legacy ones.

### 5.5 Storage and query

- Local S3: **SeaweedFS** (or Garage). MinIO's community edition is no longer maintained, so do not build on it.
- Tables: Apache Iceberg. **Writes go through PyIceberg** with a SQL catalog stored in Postgres (a separate `iceberg_catalog` database in the shared Postgres container; decision D4). **Reads go through DuckDB** with the Iceberg extension, pointed at the table's current metadata file, which PyIceberg resolves (verify: DuckDB may not read a SQL catalog directly). PySpark local mode is an optional swap to mirror "Spark on EMR".
- Layers: bronze (raw Parquet from the sink), silver (clean per-run telemetry), gold (`run_summary`, `tenant_daily`).

### 5.6 Interface

- MVP: a Python CLI. `karman submit spec.json`, `karman status <id>`, `karman approve <id>`, `karman sweep`.
- Optional: a one-page Next.js dashboard with the live residual plot and a velocity-field image. Good for the demo GIF, not required for learning.

### 5.7 Agentic layer

Phase 4's core is the recovery agent (A) with its rule checker and scoreboard. The review agent (B) and the evaluation gate (C) are stretch goals (decision D3). Each agent has a deterministic check around it and a number that says whether it is worth having.

**A. Recovery agent (in the run loop).**

When a run diverges, the workflow asks the agent for a recovery plan instead of using the fixed rule.

- **What it sees, through tools:** the last telemetry window for the run (from Kafka), the checkpoint list, the job spec, and **the history of similar runs from the gold tables** ("for this Reynolds range, which grid refinement worked, and what did it cost?"). The last tool is the point: the analytics Airflow builds feed back into the live decision, so the three tools form a loop.
- **What it returns:** a structured plan: checkpoint to restart from, new grid and relaxation time, expected cost, a one-paragraph reason, and the evidence it used.
- **The rule checker decides.** A plain function validates the plan before anything runs: Reynolds number unchanged, relaxation time and lattice velocity inside safe bounds, cost inside the tenant's budget. A rejected plan falls back to the fixed rule. The agent proposes; code decides.
- **Human approval** if the new cost crosses the threshold, using the same signal as section 5.2.
- **Durable by construction.** Each model call and each tool call is a Temporal activity with retries and timeouts. If the worker dies mid-reasoning, the agent resumes without repeating finished steps. Temporal lists official integrations for agent frameworks, including Pydantic AI and the OpenAI Agents SDK; a small loop of your own is also fine.
- **Tools are exposed through a small MCP server,** so the same tools serve the agent and a human using an MCP client.
- **Measured.** Record about ten diverging runs. Replay each through the fixed rule and through the agent. Report: runs recovered, extra compute spent, plans vetoed by the checker, model cost per recovery. Show the table in the README whichever way it comes out.
- **Model and budget.** Claude API, one pinned model id set in config, and a token cap per recovery. Expect a few dollars for a full replay (about 10 runs, three repeats each). Keep the recorded cases fixed so replays are comparable.

**B. Review agent (in CI). Stretch goal.**

A pull-request reviewer that knows this platform's rules.

- **Deterministic checks run first and can block:** lint, tests, and a small script that fails on clock, random or network calls inside `workflows/`.
- **The agent runs second and only advises.** It reads the diff and `docs/review-rules.md` (about twelve rules, each with a reason), can open related files, and comments with the rule ID and line. Example rules: workflow code must be deterministic; XCom carries paths, not data; every consumer states how it is idempotent; every Airflow task has retries; every Kafka message has `schema_version`.
- **Limits:** it cannot approve or merge; it has a token budget; it runs only on branches of your own repository; it treats the diff as data, never as instructions.
- **Measured.** Keep a folder of about twelve small sample diffs, eight with a planted violation and four clean. Report how many violations it caught and how many false alarms it raised.

**C. Evaluation gate (in CI). Stretch goal.** The recovery-agent replay from part A runs on a schedule or on a label, with a pinned model. The job fails if the recovery rate drops below the stored baseline by more than a set margin. Run each case three times to allow for model variation.

**A third recording for the README:** a diverging run, the agent's plan with its evidence, the checker's verdict, and the run completing.

## 6. Languages

| Part | Recommended | Alternatives | Note |
|---|---|---|---|
| Agents and MCP server | Python | TypeScript | Reuses your MCP and multi-agent experience |
| Solver | C++20 | Rust; Python + NumPy if short on time | C++ shows your strongest skill |
| Temporal workers | Python | TypeScript, Go | Python shares models with Airflow and consumers |
| Kafka consumers | Python (`confluent-kafka`) | TypeScript (`kafkajs`), Go | Stretch: produce directly from C++ with `librdkafka` |
| Airflow DAGs | Python | Airflow 3.4 also accepts TypeScript, Java, Go | Python is what interviewers expect |
| CLI | Python (Typer) | Go | |
| Dashboard | TypeScript (Next.js) | Plain HTML + htmx | Optional |

Recommendation: **C++ for the solver, Python for everything else, TypeScript only if you build the dashboard.** Three languages with a clear reason for each reads better than five.

## 7. Versions and local stack

Researched on 2026-11-14. **Pin exact versions when you start and re-check anything marked "verify".**

| Component | Version found | Note |
|---|---|---|
| Apache Airflow | 3.4.0 | Image `apache/airflow:3.4.0`. 3.3.x is a safe fallback if providers lag |
| Airflow Kafka provider | `apache-airflow-providers-apache-kafka` 3.0.0 | Plus `apache-airflow-providers-common-messaging` |
| Airflow S3 | `apache-airflow-providers-amazon` | Version: verify |
| Apache Kafka | 4.4.0 | KRaft only, no ZooKeeper. Image `apache/kafka`; verify that the 4.4.0 tag exists, else pin the newest 4.x tag |
| Temporal server | 1.32.x | Run with `temporal server start-dev` first; Compose later. Verify CLI version |
| Temporal Python SDK | `temporalio` 1.36.x | Needs Python 3.11+ |
| Python Kafka client | `confluent-kafka` 2.16.x | |
| SeaweedFS | 4.x | Verify licence and S3 gateway setup |
| Apache Iceberg | 1.12.0 | Verify |
| DuckDB | Verify | Read side only (Iceberg extension); writes go through PyIceberg |
| PyIceberg | Verify | SQL catalog extra and pyarrow; check `upsert` support |
| PostgreSQL | Verify (16 or 17) | One container with separate databases for Temporal, Airflow and the Iceberg catalog |
| Docker Desktop (WSL2 backend) | Verify | Or Docker Engine inside the WSL distro |
| kind, Helm, kubectl | Verify | Phase 2.5 only; match the kubectl version to the kind node version |

Docker Compose services: `kafka`, `temporal` (+ UI), `seaweedfs`, one `postgres` (separate databases for Temporal, Airflow and the Iceberg catalog, to save RAM), `airflow` (api-server, scheduler, dag-processor, triggerer), `worker` (Temporal worker + solver binary), `detector`, `sink`, optional `dashboard`. Use Compose **profiles** so each phase starts only what it needs: `core` (kafka, seaweedfs, postgres, temporal), `workers`, `airflow`, `agent`, `dashboard`. Target: `make up` starts the profiles for the current phase on a 16 GB laptop.

### 7.1 Working environment (decision D1)

- **Machine:** Windows 11 with WSL2 (x86), 16 GB. Only Docker is required locally: Docker Desktop with WSL integration, or Docker Engine inside the distro. The C++ toolchain, Python, Kafka, Temporal and Airflow all run in containers. Local Python 3.11+ with `uv` is optional, for IDE support.
- **Repo location:** inside the Linux filesystem (`~/karman-flowops`), never under `/mnt/c`. Bind mounts on the Windows drive are very slow. Run `git config core.autocrlf input` so shell scripts keep LF endings.
- **WSL limits.** Without a limit, WSL2 may take half of the RAM. Create `C:\Users\<you>\.wslconfig`, then run `wsl --shutdown`:

```ini
[wsl2]
memory=10GB
processors=3
swap=4GB
```

- **Resource budget.** These are estimates. Phase 0 measures them with `docker stats` and records the real numbers in `docs/sizing.md`.

| Phase | Running | RAM (estimate) |
|---|---|---|
| 0 | kafka, seaweedfs; one solver run uses 1 core | 2 GB |
| 1 | + postgres, temporal, worker | 3 GB |
| 2 | + detector, sink | 4 GB |
| 2.5 | + kind cluster (Airflow off) | 5 to 6 GB |
| 3 | Airflow on (kind off) | 8 to 9 GB |
| 4 | + MCP server and agent activities | 8 to 9 GB |

- **Daily-work etiquette:** `make down` when you stop for the day, run only the profiles the current phase needs, and do not run kind and Airflow at the same time.
- **Cloud pair sessions** have no Docker daemon. The agent builds and tests the solver and the Python code there, and gives you the exact Compose, kind and Helm commands to run on your laptop. You paste back the output.

## 8. Build plan

About six weeks of evenings (Phase 2.5 adds three to four days; the Phase 4 cut saves about the same). Each phase is independently demoable, so you can stop after any phase and still have something to show.

### Phase 0: Solver and skeleton (2 to 3 days)

- Repo layout (section 9), Makefile (`make up`, `make down`, `make test`), Compose with Kafka and SeaweedFS only, using profiles from the start (section 7). Multi-stage Dockerfile for the solver, so no local compiler is needed. Record `docker stats` for the stack in `docs/sizing.md`.
- C++ solver with checkpoint (plus JSON sidecar), restart, BGK and TRT collision, and telemetry on stdout.
- Done when: a stable spec finishes; an unstable spec exits with code 10 after writing at least two checkpoints; restarting that run from its last valid checkpoint with `collision: trt` finishes; a run restarted from a checkpoint gives a bit-for-bit identical final field to an uninterrupted run with the same collision model.

### Phase 1: Temporal (about 1 week)

- Workflow and activities from 5.2 without Kafka (telemetry goes to a log).
- CLI: submit, status, approve, cancel.
- Tests with Temporal's time-skipping test environment, including a replay test.
- Done when: killing the worker mid-run leads to a resume from the checkpoint; an expensive job waits for approval; a diverging job recovers by exit code.
- Check yourself:
  - Why must workflow code be deterministic, and what breaks if it is not?
  - What is the difference between a heartbeat timeout and a start-to-close timeout?
  - Signal, query, update: when would you use each?
  - How would you deploy a changed workflow while old runs are in flight?

### Phase 2: Kafka (about 1 week)

- Topics, schemas, producer in the activity, the three consumers, DLQ.
- Detector signals the workflow. Divergence is now caught before the solver crashes.
- Done when: the unstable demo is repaired from a Kafka signal; restarting the sink produces no duplicate rows; replaying `sim.telemetry` from offset 0 rebuilds the bronze files; a malformed message lands in the DLQ.
- Check yourself:
  - Why key by `run_id`? What happens to ordering if you add partitions later?
  - At-least-once versus exactly-once: which do you have, and where is the dedupe?
  - What does consumer lag tell you, and how would you alert on it?
  - When would a share group (queue semantics) fit better than a consumer group?

### Phase 2.5: Kubernetes for our own workers (about 3 to 4 days)

**Why here.** After Phase 2 there are three stateless services worth scaling (worker, detector, sink), and Airflow is not running yet, so the laptop has memory to spare for a cluster. The scope is small on purpose: Kubernetes is the packaging, not the subject.

- A `kind` cluster (config in `deploy/helm/karman/kind-config.yaml`). Kafka, Temporal, SeaweedFS and Postgres keep running in Compose; the pods reach them over the Docker network. How exactly (shared network, host gateway address) is a time-boxed two-hour spike at the start. Check the networking against the official kind docs.
- Helm chart `deploy/helm/karman`: Deployments for worker, detector and sink; config and secrets from environment variables; readiness and liveness probes; resource requests and limits.
- **Graceful shutdown.** On SIGTERM the consumers stop polling and commit what they have, and the Temporal worker drains. Set `terminationGracePeriodSeconds` with the heartbeat timeout in mind. Scratch space is an `emptyDir`; checkpoints live in S3, so losing a pod loses nothing.
- Images reach the cluster with `kind load docker-image`.
- Tests: `helm lint`, `helm template` in CI, and a smoke script.
- Done when: `kubectl delete pod` on the worker mid-run leads to a resume from the checkpoint; `kubectl scale deploy/detector --replicas=3` shows the three partitions of `sim.telemetry` spread across the replicas, and a fourth replica sits idle; the sink and detector stay correct with several replicas.
- Check yourself:
  - A worker process killed versus a worker pod deleted: what is the same for Temporal, and what differs?
  - What should a liveness probe check for a Kafka consumer, and what goes wrong if it is too eager?
  - Why do replicas beyond the partition count sit idle? What does a rebalance do to in-flight work and to the detector's in-memory window?
  - Why must the worker handle SIGTERM, and how does the grace period relate to the heartbeat timeout?
  - Why are Kafka, Temporal and Airflow not moved into the cluster here?

### Phase 3: Airflow and the lake (about 1 week)

- Both DAGs from 5.4, Iceberg tables, DuckDB queries.
- One notebook or dashboard page: failure rate by Reynolds number, cost per tenant.
- Done when: finishing a simulation triggers `run_rollup` with no polling; re-running any task is safe; `platform_report` backfills last week; a failed quality check stops the publish.
- Check yourself:
  - Why does Airflow orchestrate rather than process?
  - Sensor versus deferrable operator versus asset watcher: cost and latency of each?
  - Why not run the simulation itself as an Airflow task? Why not do the rollup in Temporal?
  - What makes a task idempotent, and why does backfill depend on it?

### Phase 4: Agentic layer (about 5 to 6 days)

- MCP server with the four read tools, recovery agent as Temporal activities, rule checker, replay scoreboard.
- The review agent and the evaluation gate are stretch goals (section 5.7 B and C, decision D3).
- Done when: a diverging run is repaired from an agent plan that passed the checker; an unsafe plan is vetoed and the rule takes over; killing the worker during the agent's reasoning does not repeat finished steps; the recovery scoreboard is in the README.
- Check yourself:
  - Why is each model call an activity and not workflow code?
  - What can go wrong if the agent's plan is applied without the checker?
  - Where did the agent beat the rule, and where was it only more expensive?
  - How do you test something whose output varies from run to run?

### Phase 5: Polish and publish (3 to 4 days)

- README with the architecture diagram, the two demos as GIFs, and a "design decisions" section.
- Parameter sweep demo (5 runs in parallel, per-tenant queues).
- CI: lint, unit tests, one end-to-end smoke test in GitHub Actions.
- LinkedIn post (section 12).

### Stretch goals (pick at most two)

1. Operations assistant: extend the MCP server so a person can ask "why did run X fail?" or "what did team A spend last week?" (Temporal queries, Kafka consumer lag, gold tables).
2. OpenTelemetry traces across workflow, Kafka and DAG; OpenLineage from Airflow.
3. Schema registry with compatibility checks in CI.
4. Solver produces to Kafka directly through `librdkafka`.
5. Review agent in CI (section 5.7 B).
6. `uat` profile with masked tenant names, to mirror the dev / UAT / prod story.
7. Evaluation gate in CI (section 5.7 C); needs the Phase 4 replay.

### If you prefer two smaller projects

- **Project A (Temporal + Kafka):** Phases 0 to 2. "Self-healing simulation runner."
- **Project B (Airflow + Kafka):** Phase 3 fed by a telemetry replay script instead of live runs. "Event-driven simulation analytics."

One combined project is the better portfolio piece, because the interesting part is how the tools hand off to each other.

## 9. Repository layout

```
karman/
  README.md
  Makefile
  docker-compose.yml
  docker/            # Dockerfiles: solver (multi-stage), platform (Python)
  deploy/helm/karman/  # Phase 2.5: chart for worker, detector, sink; kind-config.yaml
  solver/            # C++: src/, CMakeLists.txt, tests/
  platform/
    models/          # Pydantic: JobSpec, Telemetry, LifecycleEvent
    workflows/       # Temporal workflows (deterministic code only)
    activities/      # Side effects: solver, S3, Kafka
    consumers/       # detector.py, sink.py, dashboard_feed.py
    agent/           # recovery agent, plan schema, rule checker, MCP server
    evals/           # recorded diverging runs, scoring (review sample diffs only if stretch B is built)
    cli/             # karman command
    tests/
  dags/              # run_rollup.py, platform_report.py
  lake/              # table DDL, DuckDB queries, notebook
  dashboard/         # optional Next.js app
  docs/              # architecture.md, decisions.md, sizing.md, BACKLOG.md, PROGRESS.md (review-rules.md only if stretch B is built)
  .github/workflows/ # ci.yml (review-agent.yml and eval-gate.yml are stretch goals)
  specs/             # stable.json, unstable.json, expensive.json, sweep.json
  notes/             # gitignored: private interview preparation, never published
```

## 10. Non-goals

- No real cloud deployment. Everything runs locally.
- No authentication beyond a tenant ID in the spec.
- No 3D, turbulence models or solver performance work.
- No custom UI framework; the Temporal and Airflow UIs are part of the demo.
- No Redis unless the dashboard needs it.
- Kubernetes is used only for Karman's own workers (Phase 2.5). No operators, ingress, service mesh or autoscalers. Kafka, Temporal, SeaweedFS and Airflow stay in Compose.

## 11. Instructions for the coding agent

You are helping the owner learn by building. The owner is a senior C++ developer who is new to Temporal, Kafka and Airflow.

1. Work on one phase at a time, in the order of section 8. Do not start a phase until the previous "Done when" list passes.
2. Before writing code for a phase, state the plan in five to ten lines and wait for the owner's agreement.
3. Explain each new concept in three to five sentences, with one small example, the first time it appears, then show the code. The owner should be able to answer that phase's "Check yourself" questions afterwards.
4. Prefer the smallest working version. Do not add features outside this file without asking.
5. Check current official documentation for every library before using an API. Versions in section 7 are a starting point and some are marked "verify". If an API has changed, say so and update section 7.
6. Keep workflow code deterministic. Any I/O, clock or randomness belongs in an activity.
7. Every consumer and every DAG task must be idempotent. Say how in a comment or the commit message.
8. Write a test with each feature: unit tests for the detector and models, a replay test for the workflow, a DAG import test, and one end-to-end smoke test.
9. After each phase, update `docs/decisions.md` with what was chosen, what was rejected, and why. This file feeds the README and interview answers.
10. Leave the owner one small exercise per phase to implement alone (suggested: the `extend_steps` update, the DLQ path, graceful SIGTERM shutdown and probes, the `quality_checks` task, the rule checker).
11. An agent's output never acts directly. The recovery plan passes the rule checker first; the review agent only comments. Add a test for each veto path.
12. Keep model keys in CI secrets, set a token budget per job, and do not run the agent jobs on pull requests from forks.
13. Report agent results as measured. Do not tune prompts against the recorded cases without noting it in `docs/decisions.md`.
14. Start every session by reading `docs/PROGRESS.md` and saying where we are. End every task by updating it.
15. One task at a time. Per task: explain the concept (3 to 5 sentences with a small example), state the plan and wait for the owner's OK, build it, run the check, recap in 3 lines with one interview-style question, then update `docs/PROGRESS.md` and `docs/decisions.md`. Never start the next task without the owner's OK.
16. If a task takes more than twice its estimate, stop and propose a simpler option.
17. The owner works on Windows with WSL2 and Docker (section 7.1). Give commands for WSL bash and do not assume tools outside containers. A cloud session may have no Docker daemon: say which checks the owner must run, and ask for the output.
18. Commits are small, one per task. When idempotency applies, say how it is handled in the message. Commits end with a `Co-Authored-By: Claude <noreply@anthropic.com>` trailer unless the owner says otherwise.
19. Keep private interview preparation out of committed files (`notes/` is gitignored).

## 12. What to publish

**README outline.** One-line pitch. Demo GIF. Architecture diagram. "Why three tools" table (section 3). How to run in three commands. Design decisions. What I would do differently at production scale.

**Two demo recordings, under 40 seconds each.**

1. Kill the worker, the run resumes from its checkpoint.
2. Unstable run: residual climbs, alert fires, workflow rolls back and completes.

**LinkedIn post, draft.**

> I wanted to understand where Kafka, Temporal and Airflow each belong, so I built a small platform that uses all three.
>
> Karman runs fluid simulations as durable workflows. If a worker dies, the run resumes from its last checkpoint. If the simulation starts to diverge, a Kafka consumer spots it in the telemetry, and the workflow rolls back, adjusts the parameters and finishes the run. When a run completes, an event triggers an Airflow pipeline that builds the analytics tables.
>
> What I learned: (three specific lessons from `docs/decisions.md`).
>
> Repo and demo: (link)

**Message to send with it (one line).** "I built a small platform that runs and repairs long compute jobs using Temporal, Kafka and Airflow. The 30-second demo is at the top of the README."

## 13. Talking points for interviews

- Why the simulation is a Temporal workflow and the rollup is an Airflow DAG, and what would go wrong if they were swapped.
- How a crashed worker resumes: heartbeat details, checkpoint, activity retry.
- Where duplicates can appear (detector signals, sink files) and how each is made idempotent.
- Why the agent proposes and code decides, what the checker vetoed in practice, and what the scoreboard showed against the fixed rule.
- How tenant isolation works today and what a real platform would add (quotas, namespaces, cost attribution).
- What changes at 1,000 concurrent runs: partitions, consumer scaling, worker autoscaling, compaction of small files.
- What Kubernetes adds and what it does not: replicas of the detector and sink share Kafka partitions, a deleted worker pod resumes from its checkpoint, and why Kafka, Temporal and Airflow stay outside the cluster in this project.
- How you would move this to the cloud: managed Kafka, Temporal Cloud, managed Airflow, real S3, Spark for the rollup.

## 14. Sources used for the version research

- Airflow release notes: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- Airflow event-driven scheduling: https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/event-scheduling.html
- Temporal for AI: https://docs.temporal.io/ai
- Kafka 4.4.0 announcement: https://www.confluent.io/blog/introducing-apache-kafka-4-4-0/
- Kafka Docker image: https://hub.docker.com/r/apache/kafka
- MinIO status and alternatives: https://glukhov.org/data-infrastructure/object-storage/self-hosted-s3-alternatives/
- Iceberg releases: https://iceberg.apache.org/releases/
- Airflow and Temporal as complementary tools: https://www.astronomer.io/solutions/briefings/airflow-temporal-for-ai-workloads/
