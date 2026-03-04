"""
utils/redis_pubsub.py
APEX PREDATOR NEO v3 – Camada Redis Pub/Sub de baixa latência.
Serialização via orjson (3-5× mais rápido que json stdlib).
Medição de latência em nanossegundos para cada mensagem.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine, Dict, Optional

import orjson
import redis.asyncio as aioredis
from loguru import logger

from config.config import cfg


class RedisPubSub:
    """Gerenciador central de Pub/Sub entre Scanner ↔ Executors."""

    def __init__(self) -> None:
        self._redis: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._handlers: Dict[str, Callable] = {}
        self._running: bool = False

    # ── Conexão ──────────────────────────────────────────
    async def connect(self) -> None:
        """Conecta ao Redis com pool de conexões."""
        pool = aioredis.ConnectionPool(
            host=cfg.REDIS_HOST,
            port=cfg.REDIS_PORT,
            db=cfg.REDIS_DB,
            password=cfg.REDIS_PASSWORD or None,
            max_connections=20,
            decode_responses=False,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            retry_on_timeout=True,
        )
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._redis.ping()
        logger.success(f"✅ Redis conectado em {cfg.REDIS_HOST}:{cfg.REDIS_PORT}")

    async def disconnect(self) -> None:
        """Desconecta de forma limpa."""
        self._running = False
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()
        logger.info("🔌 Redis desconectado")

    # ── Publicação ───────────────────────────────────────
    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """Publica mensagem com timestamp em nanossegundos."""
        if not self._redis:
            return 0
        data["_ts_ns"] = time.time_ns()
        data["_origin"] = cfg.APEX_REGION
        try:
            return await self._redis.publish(channel, orjson.dumps(data))
        except Exception as exc:
            logger.error(f"Erro publish {channel}: {exc}")
            return 0

    # ── Subscrição ───────────────────────────────────────
    async def subscribe(self, channel: str, handler: Callable[[Dict], Coroutine]) -> None:
        """Registra handler assíncrono para um canal."""
        self._handlers[channel] = handler
        logger.info(f"📡 Subscrito em {channel}")

    async def listen(self) -> None:
        """Loop de escuta com deserialização orjson + cálculo de latência."""
        if not self._redis or not self._handlers:
            return
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(*list(self._handlers.keys()))
        self._running = True
        logger.info(f"🎧 Escutando {list(self._handlers.keys())}")

        try:
            while self._running:
                msg = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.005
                )
                if msg and msg["type"] == "message":
                    ch = msg["channel"]
                    if isinstance(ch, bytes):
                        ch = ch.decode()
                    try:
                        data = orjson.loads(msg["data"])
                    except Exception:
                        continue

                    # Latência em microssegundos
                    ts = data.pop("_ts_ns", 0)
                    if ts:
                        data["_latency_us"] = (time.time_ns() - ts) / 1_000

                    handler = self._handlers.get(ch)
                    if handler:
                        try:
                            await handler(data)
                        except Exception as exc:
                            logger.error(f"Handler {ch} erro: {exc}")

                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    # ── Estado compartilhado ─────────────────────────────
    async def set_state(self, key: str, data: Dict, ttl: int = 300) -> None:
        if self._redis:
            await self._redis.set(f"apex:v3:{key}", orjson.dumps(data), ex=ttl)

    async def get_state(self, key: str) -> Optional[Dict]:
        if self._redis:
            raw = await self._redis.get(f"apex:v3:{key}")
            if raw:
                return orjson.loads(raw)
        return None

    async def heartbeat(self, extra: Dict = None) -> None:
        hb = {"role": cfg.APEX_ROLE, "region": cfg.APEX_REGION, "status": "alive"}
        if extra:
            hb.update(extra)
        await self.publish(cfg.CH_HEARTBEAT, hb)


# Singleton global
redis_bus = RedisPubSub()
