"""
core/confluence_engine.py
APEX PREDATOR NEO v3 – ConfluenceEngine Completo

Integra 100% dos módulos proprietários do Léo Sabino:
 1. Tire Pressure              – pressão direcional do book
 2. Lead-Lag Gravitacional     – sincronia temporal entre pernas
 3. Fake Momentum Filter       – detecta momentum artificial/manipulado
 4. Consistência Temporal OI   – OI spike sustentado vs ruído
 5. OI_delta / Volume Ratio    – equilíbrio compra/venda
 6. Reversão Pós-Spike         – risco de price reversal iminente
 7. Order Book Entropy         – anti-spoof via Shannon entropy

Score final: 0-100. Só executa se >= MIN_CONFLUENCE_SCORE e sem red flags.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger

from config.config import cfg


@dataclass
class ConfluenceResult:
    """Resultado completo da análise de confluência."""
    score: float = 0.0
    tire_pressure: float = 0.0          # -1 a +1
    lead_lag_signal: float = 0.0        # -1 a +1
    fake_momentum_flag: bool = False    # True = suspeito
    oi_spike_consistency: float = 0.0   # 0 a 1
    oi_delta_vol_ratio: float = 0.0     # 0 a 1
    reversal_risk: float = 0.0         # 0 a 1
    book_entropy: float = 0.0          # 0 a 1
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """Passa em todos os filtros?"""
        return (
            self.score >= cfg.MIN_CONFLUENCE_SCORE
            and not self.fake_momentum_flag
            and self.reversal_risk < 0.70
            and self.book_entropy > 0.20
        )


class ConfluenceEngine:
    """Motor de confluência multi-fator para arbitragem triangular."""

    # Pesos dos 7 fatores (soma = 1.0)
    W = {
        "tire":     0.18,
        "leadlag":  0.14,
        "fake":     0.16,
        "oi_cons":  0.10,
        "oi_ratio": 0.10,
        "reversal": 0.16,
        "entropy":  0.16,
    }

    def __init__(self) -> None:
        self._vol_hist: Dict[str, List[float]] = {}
        self._max_hist = 200

    def analyze(
        self,
        triangle: Dict[str, Any],
        orderbooks: Dict[str, Dict],
        tickers: Dict[str, Dict],
    ) -> ConfluenceResult:
        """Análise completa de confluência para um triângulo de 3 pernas."""
        r = ConfluenceResult()
        legs = triangle.get("legs", [])
        if len(legs) < 3:
            r.details["error"] = "Triângulo incompleto"
            return r

        try:
            r.tire_pressure       = self._tire_pressure(legs, orderbooks)
            r.lead_lag_signal     = self._lead_lag(legs, tickers)
            r.fake_momentum_flag  = self._fake_momentum(legs, orderbooks, tickers)
            r.oi_spike_consistency = self._oi_consistency(legs, tickers)
            r.oi_delta_vol_ratio  = self._oi_delta_ratio(legs, tickers)
            r.reversal_risk       = self._reversal_risk(legs, tickers)
            r.book_entropy        = self._book_entropy(legs, orderbooks)
            r.score               = self._final_score(r)
            r.details = {
                "legs": [l.get("symbol", "") for l in legs],
                "weights": self.W,
            }
        except Exception as exc:
            logger.error(f"Confluência erro: {exc}")
            r.details["error"] = str(exc)
        return r

    # ═════════════════════════════════════════════════════
    # 1. TIRE PRESSURE – Pressão direcional do book
    # ═════════════════════════════════════════════════════
    def _tire_pressure(self, legs: List[Dict], obs: Dict[str, Dict]) -> float:
        """Mede imbalance bid/ask nos top-5 níveis para cada perna.
        Positivo = pressão a favor do trade. Faixa: -1 a +1."""
        vals = []
        for leg in legs:
            ob = obs.get(leg.get("symbol", ""), {})
            bids = ob.get("bids", [])[:5]
            asks = ob.get("asks", [])[:5]
            if not bids or not asks:
                vals.append(0.0)
                continue
            bv = sum(q for _, q in bids)
            av = sum(q for _, q in asks)
            total = bv + av
            if total == 0:
                vals.append(0.0)
                continue
            imb = (bv - av) / total
            # Compra → queremos bids fortes; venda → queremos asks fortes
            vals.append(imb if leg.get("side") == "buy" else -imb)
        return float(np.mean(vals)) if vals else 0.0

    # ═════════════════════════════════════════════════════
    # 2. LEAD-LAG GRAVITACIONAL
    # ═════════════════════════════════════════════════════
    def _lead_lag(self, legs: List[Dict], tickers: Dict[str, Dict]) -> float:
        """Verifica sincronia de preço entre as 3 pernas.
        Spread baixo de %change entre elas = arbitragem mais segura.
        Retorna -1 (descorrelacionado) a +1 (sincronizado)."""
        pcts = []
        for leg in legs:
            t = tickers.get(leg.get("symbol", ""), {})
            pcts.append(t.get("percentage", 0) or 0)
        if len(pcts) < 3:
            return 0.0
        spread = max(pcts) - min(pcts)
        if spread > 5.0:
            return -1.0
        if spread < 0.3:
            return 1.0
        return 1.0 - (spread / 5.0)

    # ═════════════════════════════════════════════════════
    # 3. FAKE MOMENTUM FILTER
    # ═════════════════════════════════════════════════════
    def _fake_momentum(
        self, legs: List[Dict], obs: Dict[str, Dict], tickers: Dict[str, Dict],
    ) -> bool:
        """Detecta sinais de momentum artificial em qualquer perna.
        Red flags: spread anormal, book fino, volume 24h < US$10k."""
        for leg in legs:
            sym = leg.get("symbol", "")
            ob = obs.get(sym, {})
            tk = tickers.get(sym, {})
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])

            # Spread anormal > 0.5%
            if bids and asks:
                bb, ba = bids[0][0], asks[0][0]
                if bb > 0 and ((ba - bb) / bb) * 100 > 0.5:
                    return True

            # Book muito fino
            if len(bids) < 3 or len(asks) < 3:
                return True

            # Volume 24h insignificante
            if (tk.get("quoteVolume", 0) or 0) < 10_000:
                return True

        return False

    # ═════════════════════════════════════════════════════
    # 4. CONSISTÊNCIA TEMPORAL DE OI SPIKE
    # ═════════════════════════════════════════════════════
    def _oi_consistency(self, legs: List[Dict], tickers: Dict[str, Dict]) -> float:
        """Avalia se spikes de volume são sustentados (não ruído pontual).
        Em spot usamos volume como proxy de OI. Retorna 0-1."""
        vals = []
        for leg in legs:
            sym = leg.get("symbol", "")
            vol = tickers.get(sym, {}).get("quoteVolume", 0) or 0

            if sym not in self._vol_hist:
                self._vol_hist[sym] = []
            self._vol_hist[sym].append(vol)
            self._vol_hist[sym] = self._vol_hist[sym][-self._max_hist:]

            hist = self._vol_hist[sym]
            if len(hist) < 10:
                vals.append(0.5)
                continue

            recent = np.mean(hist[-10:])
            older = np.mean(hist[-30:-10]) if len(hist) > 30 else recent
            if older <= 0:
                vals.append(0.5)
                continue

            ratio = recent / older
            if 0.7 <= ratio <= 1.5:
                vals.append(0.85)
            elif ratio > 1.5:
                vals.append(0.45)  # Spike recente, pode reverter
            else:
                vals.append(0.30)  # Volume caindo

        return float(np.mean(vals)) if vals else 0.5

    # ═════════════════════════════════════════════════════
    # 5. OI DELTA / VOLUME RATIO
    # ═════════════════════════════════════════════════════
    def _oi_delta_ratio(self, legs: List[Dict], tickers: Dict[str, Dict]) -> float:
        """Ratio entre imbalance de bid/ask e volume total.
        Valores equilibrados (0.01-0.1) = mercado saudável. Retorna 0-1."""
        vals = []
        for leg in legs:
            tk = tickers.get(leg.get("symbol", ""), {})
            bv = tk.get("bidVolume", 0) or 0
            av = tk.get("askVolume", 0) or 0
            tv = tk.get("baseVolume", 0) or 1
            delta = abs(bv - av)
            ratio = delta / tv if tv > 0 else 0
            if 0.01 <= ratio <= 0.1:
                vals.append(0.90)
            elif ratio < 0.01:
                vals.append(0.60)
            elif ratio <= 0.3:
                vals.append(0.45)
            else:
                vals.append(0.20)
        return float(np.mean(vals)) if vals else 0.5

    # ═════════════════════════════════════════════════════
    # 6. REVERSÃO PÓS-SPIKE
    # ═════════════════════════════════════════════════════
    def _reversal_risk(self, legs: List[Dict], tickers: Dict[str, Dict]) -> float:
        """Probabilidade de reversão com base na posição do preço no range 24h.
        Retorna 0 (sem risco) a 1 (reversão iminente)."""
        vals = []
        for leg in legs:
            tk = tickers.get(leg.get("symbol", ""), {})
            hi = tk.get("high", 0) or 0
            lo = tk.get("low", 0) or 0
            last = tk.get("last", 0) or 0
            if hi <= lo or last <= 0:
                vals.append(0.5)
                continue
            rng = (hi - lo) / lo * 100
            pos = (last - lo) / (hi - lo)
            if rng > 5 and (pos > 0.85 or pos < 0.15):
                vals.append(0.80)
            elif rng > 3:
                vals.append(0.40)
            else:
                vals.append(0.15)
        return float(np.mean(vals)) if vals else 0.5

    # ═════════════════════════════════════════════════════
    # 7. ORDER BOOK ENTROPY (Anti-Spoof)
    # ═════════════════════════════════════════════════════
    def _book_entropy(self, legs: List[Dict], obs: Dict[str, Dict]) -> float:
        """Shannon entropy normalizada do order book.
        Entropia alta = distribuição natural (saudável).
        Entropia baixa = poucas ordens gigantes (possível spoof).
        Retorna 0-1."""
        vals = []
        for leg in legs:
            ob = obs.get(leg.get("symbol", ""), {})
            for side in ("bids", "asks"):
                orders = ob.get(side, [])[:10]
                if len(orders) < 3:
                    vals.append(0.0)
                    continue
                vols = [q for _, q in orders if q > 0]
                total = sum(vols)
                if total <= 0 or len(vols) < 2:
                    vals.append(0.0)
                    continue
                probs = [v / total for v in vols]
                ent = -sum(p * math.log2(p) for p in probs if p > 0)
                max_ent = math.log2(len(probs))
                vals.append(ent / max_ent if max_ent > 0 else 0)
        return float(np.mean(vals)) if vals else 0.5

    # ═════════════════════════════════════════════════════
    # SCORE FINAL PONDERADO (0-100)
    # ═════════════════════════════════════════════════════
    def _final_score(self, r: ConfluenceResult) -> float:
        s = {
            "tire":     (r.tire_pressure + 1) / 2,
            "leadlag":  (r.lead_lag_signal + 1) / 2,
            "fake":     0.0 if r.fake_momentum_flag else 1.0,
            "oi_cons":  r.oi_spike_consistency,
            "oi_ratio": r.oi_delta_vol_ratio,
            "reversal": 1.0 - r.reversal_risk,
            "entropy":  r.book_entropy,
        }
        weighted = sum(s[k] * self.W[k] for k in self.W)
        return max(0.0, min(100.0, weighted * 100))


# Singleton global
confluence = ConfluenceEngine()
