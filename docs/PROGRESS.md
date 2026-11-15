# Progress

Last updated: 2026-11-14

## Where we are

Kickoff. No code yet.

## Done

- Step 1 (understand): plan reviewed, risks listed, questions answered.
- Kickoff decisions D1 to D6 applied to `docs/KARMAN_PROJECT_PLAN.md` (revision 2). Reasons are in `docs/decisions.md`.
- Repo scaffolding: `CLAUDE.md`, `.gitignore`, `docs/PROGRESS.md`, `docs/decisions.md`.

## Next

- **Step 2: write `docs/BACKLOG.md`** in the format described in `CLAUDE.md`, then stop and wait for the owner's approval. Phase 0 and Phase 1 are detailed to task level (1 to 2 hours each); Phase 2, 2.5, 3, 4 and 5 stay at feature level.
- Note: the plan now has a Phase 2.5 (Kubernetes) that the original brief did not.

## Open questions

- Commit trailer: resolved. Use `Co-Authored-By: Claude <noreply@anthropic.com>` (see D5 in `docs/decisions.md`).
- Versions to verify before first use (section 7 of the plan): Airflow 3.4.0 and its Kafka, common-messaging and Amazon providers; Kafka 4.4.0 image tag; Temporal server 1.32 and CLI; `temporalio` 1.36; SeaweedFS licence and S3 gateway; Iceberg 1.12.0; PyIceberg SQL catalog and `upsert`; DuckDB reading Iceberg by metadata path; PostgreSQL version; Docker Desktop with WSL2; kind, Helm, kubectl.
- Claim "no public project combines all three tools" is unchecked; do not publish it before searching GitHub.

## Verification pending on the owner's machine

Nothing yet. Compose, kind and Helm steps cannot be run in a cloud session (no Docker daemon).
