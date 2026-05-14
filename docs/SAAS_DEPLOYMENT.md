# SaaS Deployment

This project should run as a persistent Docker service for LinkedIn automation. Vercel can host the dashboard, but it cannot reliably run long Chrome sessions. The Docker deployment below serves the dashboard and runs Python, Chromium, Xvfb, and Temporal from one hosted server.

## Recommended Host

Use a small VPS first:

- Ubuntu 22.04 or 24.04
- 2 vCPU minimum
- 4 GB RAM minimum
- Docker and Docker Compose installed

Render, Railway, and Fly can also run this Dockerfile, but a VPS is easiest because the automation needs persistent browser/profile data.

This repo includes `render.yaml` and `railway.json` for hosted Docker platforms. On those platforms, set the same variables from `.env.saas.example` in the provider dashboard and attach a persistent disk/volume mounted at `/data`.

## Deploy

```bash
git clone <your-repo-url> rocketflow
cd rocketflow
cp .env.saas.example .env.saas
nano .env.saas
docker compose --env-file .env.saas up --build -d
```

Open:

```text
http://your-server-ip:3000
```

Put a reverse proxy like Nginx, Caddy, or Cloudflare Tunnel in front of it for HTTPS.

## Runtime Notes

- No ngrok is required.
- No local laptop process is required.
- Pipeline state is stored in the Docker volume `pipeline-data`.
- Chrome profile data is stored in the Docker volume `chrome-data`.
- The dashboard starts automation by calling the same server's `/api/pipeline/start` endpoint.

## Logs

```bash
docker compose logs -f app
docker compose exec app sh
```

Inside the container:

```bash
ls -la /data/pipeline
tail -n 100 /data/pipeline/logs/launcher/temporal-worker.stderr.log
```

## Upgrade

```bash
git pull
docker compose --env-file .env.saas up --build -d
```

## Important

LinkedIn automation may trigger account verification or bot-detection checks. Use conservative limits and safe mode. The service stores credentials through environment variables; for a public multi-user SaaS, add user accounts, encrypted per-user secrets, and isolated worker queues before opening it to other customers.
