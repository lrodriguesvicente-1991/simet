# =====================================================================
# Dockerfile - API SIMET V2 (FastAPI + Playwright)
# =====================================================================
# Usa imagem oficial Playwright Python (Chromium + deps já instalados)
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Sao_Paulo

WORKDIR /app

# Instala dependências primeiro (melhor cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install chromium

# Copia o código da aplicação
COPY api.py main.py ./
COPY database/ ./database/
COPY robots/ ./robots/

# Usuário não-root (segurança)
RUN useradd -m -u 1001 simet && chown -R simet:simet /app
USER simet

EXPOSE 8000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
