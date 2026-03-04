# Dockerfile
# APEX PREDATOR NEO v3 – Imagem de produção otimizada para latência mínima
# Python 3.11 slim + compilação nativa de hiredis/orjson/uvloop

FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Dependências de compilação + limpeza no mesmo layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libffi-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependências primeiro (cache Docker layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Diretório de logs
RUN mkdir -p /app/logs

# Usuário não-root
RUN useradd -m -r -s /bin/false apexuser && chown -R apexuser:apexuser /app
USER apexuser

# Health check via Redis ping
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import redis; redis.Redis(host='${REDIS_HOST:-redis}').ping()" || exit 1

CMD ["python", "-u", "main.py"]
