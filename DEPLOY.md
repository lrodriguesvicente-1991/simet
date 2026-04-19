# Deploy SIMET V2

Stack: FastAPI + Playwright (API) · React/Vite + Nginx (frontend) · Supabase (DB externo).

## 1. Pre-requisitos no servidor
- Docker 24+ e Docker Compose v2
- Porta 80 (e 443 se usar HTTPS) liberadas
- Projeto Supabase ja provisionado com o schema do SIMET

## 2. Configuracao
```bash
git clone <repo> simet && cd simet
cp .env.example .env
# edite .env com valores reais (DB_*, JWT_SECRET_KEY, API_USER/PASS, CORS_ALLOWED_ORIGINS)
```

Gere um JWT secret forte:
```bash
python -c "import secrets; print(secrets.token_urlsafe(64))"
```

## 3. Build e start
```bash
docker compose build
docker compose up -d
docker compose logs -f
```

Frontend: `http://SEU_IP/`  ·  API (via proxy): `http://SEU_IP/api/...`

## 4. HTTPS (recomendado)
Use Caddy ou Traefik na frente, ou adicione Certbot ao Nginx. Exemplo Caddy (host):
```
seu-dominio.com.br {
    reverse_proxy localhost:80
}
```
Depois atualize `CORS_ALLOWED_ORIGINS` no `.env` para o dominio HTTPS e `docker compose restart api`.

## 5. Atualizacoes
```bash
git pull
docker compose build
docker compose up -d
```

## 6. Troubleshooting
- **API nao sobe**: veja `docker compose logs api`. Se reclamar de `JWT_SECRET_KEY`, preencha o `.env`.
- **Playwright falha**: confirme `shm_size: 1gb` no compose e que o build usou a imagem `mcr.microsoft.com/playwright/python`.
- **Frontend 502 em /api**: API ainda subindo ou caiu. `docker compose ps`.
- **CORS bloqueado**: adicione o dominio em `CORS_ALLOWED_ORIGINS` e reinicie a API.

## 7. Alternativas de hospedagem
- **VPS barata** (Hetzner/Contabo ~5 EUR/mes): roda o compose acima direto.
- **Railway/Render**: suba `api` e `frontend` como servicos separados, use os dominios gerados e ajuste CORS.
- **Cloudflare Tunnel**: expoe o compose local sem abrir portas, com HTTPS gratis.
