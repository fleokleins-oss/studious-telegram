"""
executors/singapore_executor.py
APEX PREDATOR NEO v3 – Executor Singapore

Especialização:
 - Latência máxima aceita: 60ms (rejeita se msg Redis demorar mais)
 - Score mínimo de confluência: 70 (mais exigente que padrão)
 - Prioriza pares de alta liquidez asiática
"""
from __future__ import annotations

from typing import Any, Dict

from loguru import logger

from executors.base_executor import BaseExecutor


class SingaporeExecutor(BaseExecutor):
    MAX_LATENCY_MS = 60.0
    MIN_SCORE = 70

    def __init__(self) -> None:
        super().__init__(region="singapore")

    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        # Filtro de latência
        lat_us = data.get("_latency_us", 0)
        lat_ms = lat_us / 1000
        if lat_ms > self.MAX_LATENCY_MS:
            logger.debug(f"[SG] Rejeitado: latência {lat_ms:.1f}ms > {self.MAX_LATENCY_MS}ms")
            return
        # Filtro de score
        if data.get("confluence_score", 0) < self.MIN_SCORE:
            return
        await super()._on_opportunity(data)

    async def start(self) -> None:
        await super().start()
        logger.info(
            f"🇸🇬 Singapore | Max lat: {self.MAX_LATENCY_MS}ms | Min score: {self.MIN_SCORE}"
        )
