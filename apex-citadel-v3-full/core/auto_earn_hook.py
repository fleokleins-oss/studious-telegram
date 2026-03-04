"""
core/auto_earn_hook.py
APEX PREDATOR NEO v3 – Auto-Earn Hook

Após cada ciclo lucrativo > US$ 0.10:
 1. Busca produto Simple Earn de MAIOR APR atual
 2. Move o lucro automaticamente via API
 3. Publica confirmação via Redis
"""
from __future__ import annotations

import time
from typing import Dict, Optional

from loguru import logger

from config.config import cfg
from core.binance_connector import connector
from utils.redis_pubsub import redis_bus


class AutoEarnHook:
    """Hook automático para inscr. de lucro no Simple Earn."""

    def __init__(self) -> None:
        self._total_earned: float = 0.0
        self._total_subscribed: float = 0.0
        self._sub_count: int = 0
        self._product_cache: Optional[Dict] = None
        self._cache_exp: float = 0.0

    async def process(self, net_profit: float, asset: str = "USDT") -> bool:
        """Processa lucro; se >= threshold, inscreve no Simple Earn."""
        self._total_earned += net_profit

        if net_profit < cfg.AUTO_EARN_MIN_PROFIT:
            logger.debug(
                f"💤 Earn: ${net_profit:.4f} < mín ${cfg.AUTO_EARN_MIN_PROFIT:.2f}"
            )
            return False

        logger.info(f"🏦 Auto-Earn: processando ${net_profit:.4f} {asset}")

        product = await self._best_product(asset)
        if not product:
            logger.warning(f"Nenhum produto Earn para {asset}")
            return False

        pid = product.get("productId", "")
        apr = float(product.get("latestAnnualPercentageRate", 0))

        result = await connector.subscribe_earn(pid, net_profit)
        if not result:
            return False

        self._total_subscribed += net_profit
        self._sub_count += 1

        await redis_bus.publish(cfg.CH_EARN, {
            "type": "SUBSCRIBED",
            "amount": net_profit,
            "asset": asset,
            "product_id": pid,
            "apr": apr,
            "total": self._total_subscribed,
            "count": self._sub_count,
        })
        logger.success(
            f"✅ Earn: ${net_profit:.4f} → APR {apr*100:.2f}% | "
            f"Total: ${self._total_subscribed:.4f}"
        )
        return True

    async def _best_product(self, asset: str) -> Optional[Dict]:
        """Melhor produto Simple Earn (cache 5 min)."""
        now = time.time()
        if self._product_cache and now < self._cache_exp:
            return self._product_cache
        products = await connector.get_earn_products(asset)
        if not products:
            return None
        self._product_cache = products[0]
        self._cache_exp = now + 300
        return self._product_cache

    def summary(self) -> Dict:
        return {
            "earned": round(self._total_earned, 4),
            "subscribed": round(self._total_subscribed, 4),
            "count": self._sub_count,
        }


# Singleton global
auto_earn = AutoEarnHook()
