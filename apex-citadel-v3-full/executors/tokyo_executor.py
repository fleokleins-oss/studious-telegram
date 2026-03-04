"""
executors/tokyo_executor.py
APEX PREDATOR NEO v3 – Executor Tokyo

Especialização:
 - Mais tolerante em latência (100ms) para compensar distância
 - Exige profit mínimo mais alto (0.12%) para justificar latência
 - Funciona como backup do Singapore
"""
from __future__ import annotations

from typing import Any, Dict

from loguru import logger

from executors.base_executor import BaseExecutor


class TokyoExecutor(BaseExecutor):
    MAX_LATENCY_MS = 100.0
    MIN_PROFIT_PCT = 0.12
    MIN_SCORE = 65

    def __init__(self) -> None:
        super().__init__(region="tokyo")

    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        lat_us = data.get("_latency_us", 0)
        lat_ms = lat_us / 1000
        if lat_ms > self.MAX_LATENCY_MS:
            logger.debug(f"[TK] Rejeitado: latência {lat_ms:.1f}ms > {self.MAX_LATENCY_MS}ms")
            return
        if data.get("net_pct", 0) < self.MIN_PROFIT_PCT:
            return
        if data.get("confluence_score", 0) < self.MIN_SCORE:
            return
        await super()._on_opportunity(data)

    async def start(self) -> None:
        await super().start()
        logger.info(
            f"🇯🇵 Tokyo | Max lat: {self.MAX_LATENCY_MS}ms | "
            f"Min profit: {self.MIN_PROFIT_PCT}% | Min score: {self.MIN_SCORE}"
        )
