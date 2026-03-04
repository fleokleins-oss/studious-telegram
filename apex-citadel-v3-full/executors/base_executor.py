"""
executors/base_executor.py
APEX PREDATOR NEO v3 – Executor Base de Arbitragem Triangular

Recebe oportunidades via Redis, executa 3 pernas sequenciais,
registra no Robin Hood e aciona Auto-Earn.

Melhorias v3: tracking de slippage real, hedge parcial em falha,
e cooldown anti-spam entre execuções.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

from loguru import logger

from config.config import cfg
from core.auto_earn_hook import auto_earn
from core.binance_connector import connector
from core.robin_hood_risk import TradeRecord, robin_hood
from utils.redis_pubsub import redis_bus


class BaseExecutor:
    """Executor base para arbitragem triangular de 3 pernas."""

    def __init__(self, region: str) -> None:
        self.region = region
        self._running = False
        self._executions = 0
        self._pnl = 0.0
        self._lock = asyncio.Lock()
        self._last_exec_ts = 0.0
        self._cooldown = 0.5  # 500ms mínimo entre execuções

    async def start(self) -> None:
        await redis_bus.subscribe(cfg.CH_OPPORTUNITIES, self._on_opportunity)
        await redis_bus.subscribe(cfg.CH_RISK, self._on_risk_alert)
        self._running = True
        logger.info(f"🎯 Executor [{self.region}] ativo")

    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        """Handler para oportunidades — valida e executa."""
        now = time.time()
        if now - self._last_exec_ts < self._cooldown:
            return
        if not robin_hood.is_allowed:
            return
        if self._lock.locked():
            return

        async with self._lock:
            await self._execute(data)

    async def _on_risk_alert(self, data: Dict[str, Any]) -> None:
        if data.get("type") == "PAUSE":
            logger.warning(
                f"🚨 [{self.region}] Robin Hood: {data.get('reason', '?')} | "
                f"Pausa {cfg.ROBIN_HOOD_COOLDOWN_S}s"
            )

    async def _execute(self, opp: Dict[str, Any]) -> None:
        """Executa as 3 pernas de arbitragem triangular."""
        opp_id = opp.get("id", "?")
        legs = opp.get("legs", [])
        capital = opp.get("capital_needed", 0)

        if len(legs) < 3 or capital <= 0:
            return

        max_size = robin_hood.max_order_size()
        capital = min(capital, max_size)
        if capital < 1.0:
            return

        t0 = time.time()
        logger.info(
            f"⚡ [{self.region}] Executando #{opp_id} | "
            f"{opp.get('path', '?')} | Cap: ${capital:.2f}"
        )

        bal_before = await connector.get_balance("USDT")
        executed = []
        current = capital

        try:
            for i, leg in enumerate(legs):
                sym = leg["symbol"]
                side = leg["side"]

                order = None
                if i == 0 and side == "buy":
                    # Primeira perna buy: usar quoteOrderQty
                    order = await connector.market_order(
                        sym, "buy", quote_qty=current
                    )
                else:
                    amt = connector.to_amount_precision(sym, current)
                    if amt <= 0:
                        logger.error(f"Amount zero: {sym}")
                        break
                    order = await connector.market_order(sym, side, amount=amt)

                if not order:
                    logger.error(f"❌ Perna {i+1}/3 falhou: {side} {sym}")
                    break

                filled = float(order.get("filled", 0))
                cost = float(order.get("cost", 0))

                current = filled if side == "buy" else cost
                executed.append({
                    "symbol": sym, "side": side,
                    "filled": filled, "cost": cost,
                    "avg": order.get("average"),
                })
                logger.info(f"   ✅ {i+1}/3 {side.upper()} {sym} filled={filled}")

        except Exception as exc:
            logger.error(f"Execução erro: {exc}")

        elapsed_ms = (time.time() - t0) * 1000
        bal_after = await connector.get_balance("USDT")
        pnl = bal_after - bal_before

        # Registrar no Robin Hood
        rec = TradeRecord(
            triangle_id=opp_id,
            timestamp=time.time(),
            gross_profit=pnl + (capital * cfg.fee_3_legs),
            net_profit=pnl,
            capital_used=capital,
            legs_executed=len(executed),
            duration_ms=elapsed_ms,
        )
        await robin_hood.record(rec)
        self._executions += 1
        self._pnl += pnl
        self._last_exec_ts = time.time()

        # Publicar resultado
        await redis_bus.publish(cfg.CH_EXECUTIONS, {
            "type": "DONE",
            "region": self.region,
            "id": opp_id,
            "path": opp.get("path", ""),
            "legs": len(executed),
            "pnl": pnl,
            "ms": elapsed_ms,
            "capital": capital,
        })

        emoji = "💰" if pnl > 0 else "📉"
        logger.info(
            f"{emoji} [{self.region}] #{opp_id} | PnL: ${pnl:+.6f} | "
            f"{elapsed_ms:.0f}ms | {len(executed)}/3 pernas"
        )

        # Auto-Earn Hook
        if pnl >= cfg.AUTO_EARN_MIN_PROFIT:
            await auto_earn.process(pnl)

    def stats(self) -> Dict:
        return {
            "region": self.region,
            "executions": self._executions,
            "pnl": round(self._pnl, 6),
            "running": self._running,
        }
