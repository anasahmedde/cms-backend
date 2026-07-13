# cms-backend — Rules

FastAPI monolith (`device_video_shop_group.py`, port 8005) + PostgreSQL + S3, WebSocket `/ws/devices`. Deploys via GitHub Actions: push to `staging`/`main` → Docker → ECR → ECS `dgx-{staging|production}-backend` (us-east-2).

## Operational warnings

- **The live Android device fleet points at the STAGING API** — merging to `staging` deploys to real devices. Treat staging merges with production-level care.
- `dgx_staging` and `dgx_production` share one RDS instance (1-day backup retention).
- ECS rolling deploys run old + new tasks concurrently against the same DB — schema changes must be backward-compatible.

## Git flow (server-enforced)

- Never push directly to `staging` or `main` (blocked by rulesets, no admin bypass).
- Flow: `feature/*` | `fix/*` | `chore/*` branch → PR → `staging`; releases: PR `staging → main` (the only head branch main accepts — required `enforce-branch-flow` check).
- Descriptive branch names; never reuse throwaway branches (`temp3`-style is banned); delete after merge. Conventional commits (`feat:`, `fix:`, ...). One logical change per PR; the body says what/why/how verified.
- Rollback = `git revert` via PR + redeploy forward. Code rollbacks do NOT undo schema — migrations are forward-only, so keep them additive (no RENAME/DROP while old code may run).
- `[skip ci]` only for CI-only or docs-only commits.

## Engineering rules

- **Every new endpoint gets auth + tenant scoping by default**: `Depends(get_current_user)` + filter by the caller's `tenant_id` — never operate on client-supplied IDs across tenants. Device routes keyed by `mobile_id` are unauthenticated; add nothing sensitive to them without an auth plan.
- Validate all input with Pydantic models. All SQL parameterized.
- No process-memory state that must survive multiple replicas (in-memory sessions, progress stores, WS registries are known single-replica debt — don't add more; new shared state goes in Postgres).
- Mutations that change device/content/approval state must broadcast the matching `notify_*` WebSocket event — the dashboard depends on it.
- New domains get their own router module — do not grow `device_video_shop_group.py`. Check the route table before adding paths (three known duplicate registrations).
- Schema changes: additive, idempotent `ensure_*` functions in `migrations/`.
- No silent failures: no bare `except:` / `except: pass`; log with context and return the correct HTTP status. No `print` as logging.
- No secrets in code/scripts/docs (known hardcoded DB fallback creds exist — remove on touch, never add). Config via env vars / SSM.
- No committed junk: `*-backup.py`, stale patches, `.DS_Store`. Legacy standalone services (`device.py`, `group.py`, `shop_service.py`, `video_service.py`) are dead — don't extend them.
- New logic ships with pytest tests (start a real suite; `test_expiration.py` is a manual script, not a test).

## Definition of Done — link the minor features

- [ ] Endpoint + validation + auth/tenant + WS broadcast if state changes
- [ ] Additive migration if data changes
- [ ] **Both player paths** covered: Android device routes AND `/webapp` (Linux player) routes
- [ ] Group-level config vs per-device override semantics respected
- [ ] Behavior under company/device expiration + suspension checked
- [ ] Platform-admin (`platform_api`) view updated if per-company data is affected
- [ ] Verified end-to-end on staging before any `staging → main` PR
