# Mission Control Mini-PC Deployment

Mission Control is set up to run as an always-on private stack on a mini-PC. The intended runtime is Docker Compose in detached mode, with the React UI served by the API at `/app/` so the stack does not depend on a Vite dev server, browser tab, or laptop shell staying open.

## Initial Setup

1. Install Docker Engine with the Docker Compose plugin on the mini-PC.
2. Install Tailscale, sign in, and confirm the machine has a stable Tailscale IP.
3. Clone this repo onto the mini-PC.
4. Create the root `.env` from `.env.example` and fill in real values.
5. Create the local data directory:

```bash
mkdir -p data
```

6. Start the stack:

```bash
scripts/ops/mission-control.sh start
```

7. Verify health:

```bash
scripts/ops/mission-control.sh health
```

## Core Runtime Model

Core services:
- `api`
- `worker`
- `scheduler`
- `postgres`
- `redis`

Runtime characteristics:
- all core services use `restart: unless-stopped`
- the root `.env` file is loaded by Compose
- Postgres uses a named persistent volume
- Redis uses append-only persistence in a named persistent volume
- Mission Control operational SQLite state stays in `./data`
- the frontend is built into the API image and served at `http://127.0.0.1:8000/app/`

Ports stay bound to `127.0.0.1` so the stack remains private to the mini-PC unless you reach it through Tailscale, SSH port forwarding, or another intentional private-network hop.

## Routine Operations

Start:

```bash
scripts/ops/mission-control.sh start
```

Stop:

```bash
scripts/ops/mission-control.sh stop
```

Restart:

```bash
scripts/ops/mission-control.sh restart
```

Rebuild after code changes:

```bash
scripts/ops/mission-control.sh rebuild
```

Follow logs:

```bash
scripts/ops/mission-control.sh logs
scripts/ops/mission-control.sh logs api
scripts/ops/mission-control.sh logs worker
scripts/ops/mission-control.sh logs scheduler
```

Show container state:

```bash
scripts/ops/mission-control.sh ps
```

Verify runtime health:

```bash
scripts/ops/mission-control.sh health
```

If you want the optional Adminer container too:

```bash
COMPOSE_PROFILES=ops scripts/ops/mission-control.sh start
```

## Health Verification Workflow

The health workflow checks:
- API `/health`
- API `/ready`
- Redis reachability
- queue depth
- worker heartbeat freshness
- scheduler heartbeat freshness
- last scheduler tick freshness

Primary command:

```bash
scripts/ops/verify-health.sh
```

This uses the API container to query the live runtime endpoints and prints a compact JSON summary suitable for manual inspection during updates or after a reboot.

## Access Over Tailscale

The recommended runtime UI is the built app served by the API:

- on the mini-PC itself: `http://127.0.0.1:8000/app/`
- over Tailscale without exposing Docker ports publicly: configure Tailscale Serve or a comparable tailnet-only forwarding rule on the mini-PC so Tailscale proxies to `http://127.0.0.1:8000`
- over Tailscale SSH forwarding from a laptop:

```bash
ssh -L 8000:127.0.0.1:8000 your_user@<tailscale-ip>
```

Then open:

```text
http://localhost:8000/app/
```

The Vite dev server is only for active frontend development and is not part of the always-on deployment.

Because the Compose ports stay on `127.0.0.1`, direct `http://<tailscale-ip>:8000` access is not the default path. That is intentional. It keeps Mission Control private to the host unless you explicitly add a tailnet-only access layer such as Tailscale Serve.

## Backup Guidance

Back up these items:
- root `.env`
- `./data/`
- Postgres named volume `pgdata`
- Redis named volume `redisdata` if you want queue and append-only persistence backed up too

Simple backup examples:

```bash
tar czf mission-control-env-and-data.tgz .env data
docker run --rm -v mission-control_pgdata:/from -v "$PWD:/backup" alpine tar czf /backup/mission-control-pgdata.tgz -C /from .
docker run --rm -v mission-control_redisdata:/from -v "$PWD:/backup" alpine tar czf /backup/mission-control-redisdata.tgz -C /from .
```

Restore steps depend on your host naming and deployment path, so test the backup flow before you rely on it.

## Update Workflow

1. Pull the latest code on the mini-PC.
2. Review `.env.example` for any newly added settings and update `.env` if needed.
3. Rebuild and restart:

```bash
scripts/ops/mission-control.sh rebuild
```

4. Re-run health verification:

```bash
scripts/ops/mission-control.sh health
```

5. Check recent logs if anything looks off:

```bash
scripts/ops/mission-control.sh logs api
scripts/ops/mission-control.sh logs worker
scripts/ops/mission-control.sh logs scheduler
```

## Operator Checklist

- Docker installed and enabled on boot
- Tailscale installed, signed in, and connected
- mini-PC sleep/hibernate disabled
- repo cloned onto persistent storage
- `.env` created and backed up
- `data/` directory present
- stack started with `scripts/ops/mission-control.sh start`
- health verified with `scripts/ops/mission-control.sh health`
- optional: startup-on-boot automation added for the compose stack

## Manual Mini-PC Tasks Still Required

Mission Control can ship the container/runtime side, but the operator still needs to do these machine-level steps manually:
- install Docker and Docker Compose
- install and configure Tailscale
- configure Tailscale Serve or your preferred tailnet-only access method if you want browser access without an active SSH session
- disable sleep, hibernate, or aggressive power saving
- make sure Docker starts on boot
- configure the Mission Control stack to start on boot, such as a systemd unit or a login-time startup task
