#!/bin/bash
set -euxo pipefail
exec > >(tee -a /var/log/ceaser-bootstrap.log) 2>&1

export DEBIAN_FRONTEND=noninteractive

# 2GB swap safety net (t4g.small only has 2GB RAM)
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

apt-get update -y
apt-get install -y ca-certificates curl gnupg git

# Docker
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable --now docker
usermod -aG docker ubuntu

APP_DIR=/opt/ceaser
mkdir -p $APP_DIR
cd $APP_DIR

if [ ! -d repo ]; then
  git clone --branch ${git_branch} ${git_repo_url} repo
fi

# backend/.env — rendered from terraform env_vars map
cat > repo/backend/.env <<'ENVEOF'
DATABASE_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/ceaser
REDIS_URL=redis://redis:6379/0
%{ for key, val in env_vars ~}
${key}=${val}
%{ endfor ~}
ENVEOF
chmod 600 repo/backend/.env

cat > $APP_DIR/docker-compose.yml <<'COMPOSEEOF'
services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: ceaser
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    restart: unless-stopped
    volumes:
      - redisdata:/data

  backend:
    build: ./repo/backend
    restart: unless-stopped
    env_file: ./repo/backend/.env
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_started
    expose:
      - "8000"
    volumes:
      - uploads:/app/uploads

  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    depends_on:
      - backend

volumes:
  pgdata:
  redisdata:
  uploads:
  caddy_data:
  caddy_config:
COMPOSEEOF

cat > $APP_DIR/Caddyfile <<CADDYEOF
${api_fqdn} {
    reverse_proxy backend:8000
    encode gzip zstd
    request_body {
        max_size 5GB
    }
}
CADDYEOF

# Deploy helper: `sudo /opt/ceaser/deploy.sh` to pull + rebuild
cat > $APP_DIR/deploy.sh <<'DEPLOYEOF'
#!/bin/bash
set -euo pipefail
cd /opt/ceaser/repo
git pull
cd /opt/ceaser
docker compose up -d --build
docker image prune -f
DEPLOYEOF
chmod +x $APP_DIR/deploy.sh

cd $APP_DIR
docker compose up -d --build
