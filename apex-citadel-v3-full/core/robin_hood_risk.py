"""
core/robin_hood_risk.py
APEX PREDATOR NEO v3 – Robin Hood Risk Engine

Regras invioláveis:
 • Drawdown total > 4.0%  → PAUSA TOTAL por 30 minutos
 • Equity < 50% do capital → SHUTDOWN permanente
 • Capital por ciclo nunca excede MAX_POR_CICLO
 • Publica alerta via Redis para sincronizar todos os serviços
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List

from loguru import logger

from config.config import cfg
from utils.redis_pubsub import redis_bus


@dataclass
class TradeRecord:
    triangle_id: str
    timestamp: float
    gross_profit: float
    net_profit: float
    capital_used: float
    legs_executed: int
    duration_ms: float


@dataclass
class RiskState:
    initial_capital: float = cfg.CAPITAL_TOTAL
    equity: float = cfg.CAPITAL_TOTAL
    peak: float = cfg.CAPITAL_TOTAL
    total_pnl: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    paused: bool = False
    pause_until: float = 0.0
    pause_reason: str = ""
    history: List[TradeRecord] = field(default_factory=list)


class RobinHoodRisk:
    """Motor de risco com pausa automática por drawdown."""

    def __init__(self) -> None:
        self.st = RiskState()
        self._lock = asyncio.Lock()

    async def initialize(self, balance: float = None) -> None:
        """Inicializa com saldo real da exchange."""
        if balance and balance > 0:
            self.st.initial_capital = balance
            self.st.equity = balance
            self.st.peak = balance
        logger.info(
            f"🛡️ Robin Hood | Capital: ${self.st.initial_capital:.2f} | "
            f"Max DD: {cfg.MAX_DRAWDOWN_PCT}% | Max/ciclo: ${cfg.MAX_POR_CICLO:.2f}"
        )

    @property
    def drawdown_pct(self) -> float:
        if self.st.peak <= 0:
            return 0.0
        return max(0.0, (self.st.peak - self.st.equity) / self.st.peak * 100)

    @property
    def win_rate(self) -> float:
        return (self.st.wins / max(1, self.st.trades)) * 100

    @property
    def is_allowed(self) -> bool:
        """Verifica se pode operar agora."""
        if self.st.paused:
            if time.time() < self.st.pause_until:
                return False
            # Pausa expirou
            self.st.paused = False
            self.st.pause_reason = ""
            logger.success("▶️ Robin Hood: pausa encerrada")

        if self.drawdown_pct >= cfg.MAX_DRAWDOWN_PCT:
            asyncio.ensure_future(self._pause(
                f"Drawdown {self.drawdown_pct:.2f}% >= {cfg.MAX_DRAWDOWN_PCT}%"
            ))
            return False

        # Shutdown permanente: equity < 50% do capital
        if self.st.equity < self.st.initial_capital * 0.50:
            logger.critical(
                f"🚨 SHUTDOWN: equity ${self.st.equity:.2f} < 50% "
                f"de ${self.st.initial_capital:.2f}"
            )
            return False

        return True

    def max_order_size(self) -> float:
        """Tamanho máximo para próximo ciclo, ajustado por drawdown."""
        if not self.is_allowed:
            return 0.0
        size = min(cfg.MAX_POR_CICLO, self.st.equity * 0.40)
        dd = self.drawdown_pct
        if dd > 2.0:
            # Redução proporcional ao drawdown (até 50% de corte)
            factor = 1.0 - ((dd - 2.0) / (cfg.MAX_DRAWDOWN_PCT - 2.0)) * 0.5
            size *= max(0.1, factor)
        return max(0.0, size)

    async def record(self, rec: TradeRecord) -> None:
        """Registra resultado de trade e verifica drawdown."""
        async with self._lock:
            self.st.history.append(rec)
            self.st.trades += 1
            self.st.total_pnl += rec.net_profit
            self.st.equity += rec.net_profit
            if rec.net_profit > 0:
                self.st.wins += 1
            else:
                self.st.losses += 1
            if self.st.equity > self.st.peak:
                self.st.peak = self.st.equity

            emoji = "✅" if rec.net_profit > 0 else "❌"
            logger.info(
                f"{emoji} Trade #{self.st.trades} | PnL: ${rec.net_profit:+.4f} | "
                f"Equity: ${self.st.equity:.2f} | DD: {self.drawdown_pct:.2f}% | "
                f"WR: {self.win_rate:.0f}%"
            )

            if self.drawdown_pct >= cfg.MAX_DRAWDOWN_PCT:
                await self._pause(f"DD {self.drawdown_pct:.2f}% após trade")

    async def _pause(self, reason: str) -> None:
        """Ativa pausa total e notifica via Redis."""
        if self.st.paused:
            return
        self.st.paused = True
        self.st.pause_until = time.time() + cfg.ROBIN_HOOD_COOLDOWN_S
        self.st.pause_reason = reason
        logger.warning(
            f"🚨 ROBIN HOOD ATIVADO | {reason} | "
            f"Pausa: {cfg.ROBIN_HOOD_COOLDOWN_S}s | "
            f"Equity: ${self.st.equity:.2f}"
        )
        await redis_bus.publish(cfg.CH_RISK, {
            "type": "PAUSE",
            "reason": reason,
            "pause_until": self.st.pause_until,
            "equity": self.st.equity,
            "dd_pct": self.drawdown_pct,
        })
        await redis_bus.set_state("risk", {
            "paused": True,
            "pause_until": self.st.pause_until,
            "equity": self.st.equity,
        }, ttl=cfg.ROBIN_HOOD_COOLDOWN_S + 60)

    def summary(self) -> Dict:
        return {
            "equity": round(self.st.equity, 4),
            "peak": round(self.st.peak, 4),
            "pnl": round(self.st.total_pnl, 4),
            "dd_pct": round(self.drawdown_pct, 2),
            "trades": self.st.trades,
            "wr": round(self.win_rate, 1),
            "paused": self.st.paused,
            "max_size": round(self.max_order_size(), 4),
        }


# Singleton global
robin_hood = RobinHoodRisk()
