# Mission Control

Mission Control is a local-first AI task orchestration system built to run continuously on a mini-PC or server.

It includes:
- a FastAPI API
- a durable RQ worker
- a scheduler for recurring and retry-due work
- Postgres for tasks, runs, and artifacts
- Redis for queueing
- a React operator UI

The current first-class workflows are:
- Jobs v2 search, ranking, shortlist, digest, and notification
- Deals scanning and notification
- Planner-driven recurring workflows
- Runs, Alerts, and Observability views for debugging and operations

## How It Runs

Mission Control is meant to stay up on the mini-PC even when your laptop disconnects.

Durable services:
- `api`
- `worker`
- `scheduler`
- `postgres`
- `redis`
- `adminer` (optional database UI)

Recommended run mode:

```bash
docker compose up -d --build
```

That starts everything in detached mode with `restart: unless-stopped`.

## Quick Start

### 1. Prerequisites

- Docker + Docker Compose
- Node.js 20+ if you want to run the frontend dev server
- an OpenAI API key if you want real LLM execution

### 2. Create `.env`

Create a `.env` file in the repo root.

Minimum useful local setup:

```env
POSTGRES_USER=mission
POSTGRES_PASSWORD=mission
POSTGRES_DB=mission_control

DATABASE_URL=postgresql+psycopg://mission:mission@postgres:5432/mission_control
REDIS_URL=redis://redis:6379/0

API_KEY=replace-with-a-strong-key
OPENAI_API_KEY=sk-...
USE_LLM=true

DAILY_BUDGET_USD=1.00
BUDGET_BUFFER_USD=0.02
MISSION_CONTROL_DAY_BOUNDARY_TZ=America/New_York

SCRAPE_TIMEOUT_SECONDS=15
SCRAPE_RATE_LIMIT_SECONDS=2
SCRAPE_RETRY_ATTEMPTS=3

NOTIFY_DEDUPE_TTL_SECONDS=21600
NOTIFY_DISCORD_ALLOWLIST=deals_scan_v1,unicorn_deals_poll_v1,unicorn_deals_rank_v1,jobs_digest_v2,ops_report_v1
NOTIFY_DEV_MODE=false
```

Important notes:
- `DATABASE_URL` and `REDIS_URL` should use the Docker service hostnames `postgres` and `redis`.
- Set `USE_LLM=false` if you want the pipeline to run without OpenAI calls.
- If you override `NOTIFY_DISCORD_ALLOWLIST`, keep `jobs_digest_v2` included or Jobs digests will not flow through `notify_v1`.
- The compose stack mounts `./data` into the containers and stores the SQLite telemetry/history files there.

### 3. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

### 4. Confirm the core services are healthy

```bash
docker compose ps
docker compose logs --no-color --tail=60 api
docker compose logs --no-color --tail=60 worker
docker compose logs --no-color --tail=60 scheduler
```

## Open the UI

Mission Control now has two ways to use the UI:

- recommended: the built React app served by the API at `/app/`
- optional for frontend development: the Vite dev server on port `5173`

### Recommended: built UI served by the API

Open:

- local on the mini-PC: `http://localhost:8000/app/`
- legacy pages: `http://localhost:8000/legacy` and `http://localhost:8000/legacy/observability`
- Swagger docs: `http://localhost:8000/docs`

### If you connect from your laptop over SSH

The Docker services bind to `127.0.0.1`, so your laptop needs SSH port forwarding.

Recommended SSH command:

```bash
ssh -L 8000:127.0.0.1:8000 -L 8080:127.0.0.1:8080 your_user@mini-pc
```

Then open on your laptop:

- Mission Control UI: `http://localhost:8000/app/`
- Adminer: `http://localhost:8080/`

This is the simplest and most reliable way to use Mission Control from a laptop.

### If you use VS Code Remote SSH and want the direct numeric URL

If you normally SSH into the mini-PC from VS Code and then `cmd` + click the link that Vite prints in the terminal, use the Vite dev server with a public bind address:

```bash
cd frontend
npm run dev -- --host 0.0.0.0 --port 5173 --strictPort
```

Vite will print links like:

- `http://192.168.18.210:5173/`
- `http://100.110.193.90:5173/`

In this setup, the direct numeric URL is the right one to open from your laptop browser.

For this machine, the Tailscale address is usually the easiest:

- `http://100.110.193.90:5173/`

Why this works:
- `--host 0.0.0.0` makes the dev server reachable from outside the mini-PC
- `--strictPort` prevents Vite from silently switching to `5174`, `5175`, and so on
- the numeric URL is often easier than relying on `localhost` forwarding when you are already working through VS Code Remote SSH

## Frontend Development

Only use this when you are actively working on the React frontend.

### Start the backend first

```bash
docker compose up -d api worker scheduler redis postgres
```

### Run the Vite dev server

On the mini-PC:

```bash
cd frontend
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```

Recommended improvement:

```bash
npm run dev -- --host 0.0.0.0 --port 5173 --strictPort
```

This is the best choice if you want the terminal to print one stable clickable numeric URL.

If you are connecting from your laptop and want localhost-style forwarding instead, start SSH with both forwards:

```bash
ssh -L 8000:127.0.0.1:8000 -L 5173:127.0.0.1:5173 your_user@mini-pc
```

Then open on your laptop:

`http://localhost:5173/`

Notes:
- The frontend dev server proxies API calls to `http://localhost:8000`.
- If the tab appears to load forever, it is usually because the port was not forwarded, Vite was not started with `--host 0.0.0.0`, or Vite moved to a different port because `5173` was already in use.
- If you prefer the direct-IP method, open the numeric `Network:` URL Vite prints in the terminal instead of `localhost`.

Useful frontend commands:

```bash
cd frontend
npm run build
npm run test
npm run generate:openapi
```

## Daily Operator Flow

The normal UI path is:

1. `Workflows`
   Configure or review recurring automations, especially the Jobs watcher.
2. `Runs`
   Inspect stage-by-stage execution artifacts for collect, normalize, rank, shortlist, digest, and notify.
3. `Alerts`
   Review grouped failures, intentional notify skips, weak source coverage, and direct next actions.
4. `Observability`
   Check API, worker, scheduler, Redis, heartbeat, and runtime health signals.

The Jobs watcher is designed to be operated from the UI without hand-editing JSON.

## Common Commands

### Start or rebuild everything

```bash
docker compose up -d --build
```

### Restart one service

```bash
docker compose restart api
docker compose restart worker
docker compose restart scheduler
```

### Follow logs

```bash
docker compose logs -f api
docker compose logs -f worker
docker compose logs -f scheduler
```

### Stop everything

```bash
docker compose down
```

## API Quickstart

Read the API key from `.env`:

```bash
API_KEY=$(grep "^API_KEY=" .env | cut -d= -f2-)
```

### Create a Jobs v2 collection task

```bash
curl -s -X POST http://localhost:8000/tasks \
  -H "X-API-Key: $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "task_type": "jobs_collect_v1",
    "payload_json": "{\"request\":{\"collectors_enabled\":true,\"profile_mode\":\"resume_profile\",\"sources\":[\"linkedin\",\"indeed\",\"glassdoor\",\"handshake\"],\"titles\":[\"Machine Learning Engineer\"],\"desired_title_keywords\":[\"machine learning engineer\",\"ai engineer\"],\"keywords\":[\"python\",\"llm\"],\"excluded_keywords\":[\"staff\"],\"locations\":[\"Remote\",\"New York, NY\"],\"work_modes\":[\"remote\",\"hybrid\"],\"desired_salary_min\":160000,\"experience_levels\":[\"entry\",\"mid\",\"senior\"],\"result_limit_per_source\":250,\"max_queries_per_run\":12,\"shortlist_count\":5,\"jobs_notification_cooldown_days\":3,\"resurface_seen_jobs\":true}}",
    "model": null
  }' | jq
```

### List tasks

```bash
curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/tasks | jq
```

### Get task runs

```bash
curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/tasks/<TASK_ID>/runs | jq
```

### Get the latest result artifact

```bash
curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/tasks/<TASK_ID>/result | jq
```

### Get today’s stats

```bash
curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/stats/today | jq
```

## Useful URLs

When running locally on the mini-PC:

- React operator UI: `http://localhost:8000/app/`
- legacy UI: `http://localhost:8000/legacy`
- legacy observability: `http://localhost:8000/legacy/observability`
- Swagger: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- Adminer: `http://localhost:8080/`

## Services and Architecture

Docker Compose services:
- `api`: FastAPI server
- `worker`: RQ worker execution runtime
- `scheduler`: recurring and retry-due task creation
- `postgres`: primary relational store
- `redis`: queue backend
- `adminer`: optional database browser

High-level flow:
1. A client or watcher creates a task with `POST /tasks`.
2. The API validates auth, budget, and payload shape.
3. The API stores the task and enqueues it in Redis.
4. The worker executes the handler and writes runs, artifacts, telemetry, and followup tasks.
5. The scheduler handles recurring schedules, retry-due tasks, and planner-related loops.

## Repository Layout

- `api/main.py` - FastAPI app, routes, models, and frontend serving under `/app`
- `api/scheduler.py` - scheduler loop
- `api/router.py` - model routing
- `api/models/catalog.py` - model catalog helpers
- `worker/worker.py` - worker runtime and task execution
- `worker/task_handlers/` - task handlers
- `worker/llm/openai_adapter.py` - OpenAI adapter
- `integrations/` - external collectors and scrapers
- `frontend/` - React + Vite operator UI
- `migrations/` - SQL migration files
- `examples/` - verification scripts
- `docker-compose.yml` - local orchestration

## Jobs v2 Notes

Jobs v2 is now a full pipeline:

1. `jobs_collect_v1`
2. `jobs_normalize_v1`
3. `jobs_rank_v1`
4. `jobs_shortlist_v1`
5. `jobs_digest_v2`
6. optional `notify_v1`

Important behavior:
- collection is intentionally broad and can run multiple queries per source
- dedupe happens within a run, not as permanent cross-run suppression
- previously seen but non-winning jobs can resurface in later runs
- recently notified jobs can be cooled down to reduce spam
- Runs and Alerts now expose jobs-specific observability and next actions

## Data and Observability Storage

Mission Control uses both Postgres and SQLite-backed operational stores.

Postgres:
- tasks
- runs
- artifacts
- schedules

SQLite-backed operational data in `./data/task_run_history.sqlite3` by default:
- task run history
- AI usage logs
- event log
- system metrics
- agent heartbeats
- candidate resume profile
- daily ops reports
- deal alert dedupe state

Helpful verification scripts:

```bash
python examples/verify_task_run_history.py
python examples/verify_ai_usage_logging.py
python examples/print_recent_events.py
python examples/verify_system_metrics.py
python examples/verify_agent_heartbeats.py
python examples/verify_resume_profile.py
python examples/generate_daily_ops_report.py
python examples/verify_autonomous_planner.py
python examples/verify_planner_controls.py
```

## Database Migrations

For a fresh local database, startup will create the current tables automatically.

For an existing database, apply the SQL files in `migrations/` in order:

```bash
docker compose up -d postgres
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/20260225_add_artifacts_table.sql
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/20260225_cost_precision_numeric.sql
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/20260225_reliability_scheduler_auth.sql
```

## Endpoints

Core:
- `POST /tasks`
- `GET /tasks`
- `GET /tasks/{task_id}`
- `GET /tasks/{task_id}/runs`
- `GET /tasks/{task_id}/result`
- `GET /runs`
- `POST /schedules`
- `GET /schedules`

Health and telemetry:
- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /stats/today`
- `GET /telemetry/events`
- `GET /telemetry/ai-usage`
- `GET /telemetry/ai-usage/today`
- `GET /telemetry/ai-usage/summary`
- `GET /telemetry/runtime-status`

Resume profile:
- `GET /profile/resume?include_text=true|false`
- `POST /profile/resume/upload`
- `PUT /profile/resume`
- `DELETE /profile/resume`

## Troubleshooting

### The UI keeps loading forever

Most common causes:
- the backend stack is not running
- you opened `localhost` on your laptop without SSH port forwarding
- you are using Vite dev mode without forwarding port `5173`
- Vite was started without `--host 0.0.0.0`
- Vite silently moved to another port because `5173` was already in use

### The built UI works, but the Vite dev UI does not

That usually means the backend is fine and the problem is only the dev-server access path. Use:

```bash
ssh -L 8000:127.0.0.1:8000 -L 5173:127.0.0.1:5173 your_user@mini-pc
cd frontend
npm run dev -- --host 0.0.0.0 --port 5173
```

If you use VS Code Remote SSH and the numeric URL works better for you, use this instead:

```bash
cd frontend
npm run dev -- --host 0.0.0.0 --port 5173 --strictPort
```

Then open the exact `Network:` URL Vite prints, for example:

```text
http://100.110.193.90:5173/
```

If `5173` is already in use, clear old Vite processes first:

```bash
pkill -f vite
```

### Tasks should continue when my laptop disconnects

Yes. The durable path is the Docker Compose stack on the mini-PC. Your laptop browser, SSH session, and Vite dev server are not required for already-created tasks to continue running.
