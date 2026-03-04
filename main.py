"""
main.py
APEX PREDATOR NEO v3 – Entry Point

Roteamento por APEX_ROLE (Docker ENV):
 - scanner  → dynamic_tri_scanner (Curitiba)
 - executor → singapore_executor / tokyo_executor

100% assíncrono com uvloop.
"""
from __future__ import annotations

import asyncio
import signal
import sys

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from loguru import logger
from config.config import cfg


def setup_logging() -> None:
    """Configura loguru: console colorido + arquivo com rotação."""
    logger.remove()
    fmt = (
        "<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>"
    )
    logger.add(sys.stderr, level=cfg.LOG_LEVEL, format=fmt, colorize=True)
    logger.add(
        f"/app/logs/apex_{cfg.APEX_ROLE}_{cfg.APEX_REGION}.log",
        level="DEBUG", rotation=cfg.LOG_ROTATION,
        retention=cfg.LOG_RETENTION, compression="gz", enqueue=True,
    )
    logger.add(
        f"/app/logs/apex_errors_{cfg.APEX_REGION}.log",
        level="ERROR", rotation="10 MB", retention="30 days",
        compression="gz", enqueue=True,
    )


async def run_scanner() -> None:
    from core.binance_connector import connector
    from core.robin_hood_risk import robin_hood
    from scanners.dynamic_tri_scanner import scanner
    from utils.redis_pubsub import redis_bus

    logger.info("═" * 55)
    logger.info("  APEX PREDATOR NEO v3 — SCANNER")
    logger.info(f"  Testnet: {cfg.TESTNET} | Capital: ${cfg.CAPITAL_TOTAL}")
    logger.info(f"  Scan: {cfg.SCAN_INTERVAL_MS}ms | Region: {cfg.APEX_REGION}")
    logger.info("═" * 55)

    await redis_bus.connect()
    await connector.connect()

    bal = await connector.get_balance("USDT")
    await robin_hood.initialize(bal)
    logger.info(f"💰 Saldo USDT: ${bal:.4f}")

    n = await scanner.discover()
    if n == 0:
        logger.error("❌ Zero triângulos — verifique BASE_ASSETS e QUOTE_ASSETS")
        return

    try:
        await scanner.run()
    except asyncio.CancelledError:
        pass
    finally:
        scanner.stop()
        await connector.disconnect()
        await redis_bus.disconnect()


async def run_executor() -> None:
    from core.binance_connector import connector
    from core.robin_hood_risk import robin_hood
    from utils.redis_pubsub import redis_bus

    if cfg.APEX_REGION == "singapore":
        from executors.singapore_executor import SingaporeExecutor
        executor = SingaporeExecutor()
    elif cfg.APEX_REGION == "tokyo":
        from executors.tokyo_executor import TokyoExecutor
        executor = TokyoExecutor()
    else:
        logger.error(f"Região desconhecida: {cfg.APEX_REGION}")
        return

    logger.info("═" * 55)
    logger.info(f"  APEX PREDATOR NEO v3 — EXECUTOR [{cfg.APEX_REGION.upper()}]")
    logger.info(f"  Testnet: {cfg.TESTNET}")
    logger.info("═" * 55)

    await redis_bus.connect()
    await connector.connect()
    bal = await connector.get_balance("USDT")
    await robin_hood.initialize(bal)

    await executor.start()

    try:
        await redis_bus.listen()
    except asyncio.CancelledError:
        pass
    finally:
        await connector.disconnect()
        await redis_bus.disconnect()


def main() -> None:
    setup_logging()
    logger.info(f"🦈 APEX PREDATOR NEO v3 | Role: {cfg.APEX_ROLE} | Region: {cfg.APEX_REGION}")

    if not cfg.api_key or not cfg.api_secret:
        logger.critical("❌ API keys não configuradas — preencha o .env")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(sig, frame):
        logger.warning(f"⚠️ Sinal {sig} — encerrando...")
        for t in asyncio.all_tasks(loop):
            t.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        coro = run_scanner() if cfg.APEX_ROLE == "scanner" else run_executor()
        loop.run_until_complete(coro)
    except KeyboardInterrupt:
        pass
    finally:
        pending = asyncio.all_tasks(loop)
        for t in pending:
            t.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
        logger.info("🏁 APEX PREDATOR NEO v3 encerrado")


if __name__ == "__main__":
    main()
