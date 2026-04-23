#cleaner
from __future__ import annotations
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datakit.models.schema import (CorporateActionType, Dividend, Frequency, Fundamental, MacroSeries, OHLCV, Split)
logger = logging.getLogger(__name__)
_ZERO = Decimal("0")
_ONE = Decimal("1")

def normalize_prices(bars: List[OHLCV], splits: Optional[List[Split]] = None, dividends: Optional[List[Dividend]] = None) -> List[OHLCV]:
    if not bars:
        return []
    bars = _sort_bars(bars)
    bars = _dedupe_bars(bars)
    bars = [_repair_bar(b) for b in bars]
    bars = _normalise_timestamps(bars)
    if (splits or dividends) and not bars[0].adjusted:
        bars = _apply_adjustments(bars, splits or [], dividends or [])
    return bars

def _sort_bars(bars: List[OHLCV]) -> List[OHLCV]:
    return sorted(bars, key=lambda b: b.timestamp)

def _dedupe_bars(bars: List[OHLCV]) -> List[OHLCV]:
    seen: set = set()
    out = []
    for b in bars:
        if b.timestamp not in seen:
            seen.add(b.timestamp)
            out.append(b)
    return out

def _normalise_timestamps(bars: List[OHLCV]) -> List[OHLCV]:
    out = []
    for b in bars:
        ts = b.timestamp
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        out.append(OHLCV(ticker=b.ticker, timestamp=ts, frequency=b.frequency,
                         open=b.open, high=b.high, low=b.low, close=b.close,
                         volume=b.volume, adjusted=b.adjusted, source=b.source))
    return out

def _repair_bar(b: OHLCV) -> OHLCV:
    lo, hi = b.low, b.high
    if lo < _ZERO: lo = abs(lo)
    if hi < _ZERO: hi = abs(hi)
    if lo > hi: lo, hi = hi, lo
    op = max(lo, min(hi, b.open))
    cl = max(lo, min(hi, b.close))
    if lo == b.low and hi == b.high and op == b.open and cl == b.close:
        return b
    return OHLCV(ticker=b.ticker, timestamp=b.timestamp, frequency=b.frequency,
                 open=op, high=hi, low=lo, close=cl,
                 volume=b.volume, adjusted=b.adjusted, source=b.source)

def _apply_adjustments(bars: List[OHLCV], splits: List[Split], dividends: List[Dividend]) -> List[OHLCV]:
    bar_dates = [b.timestamp.date() for b in bars]
    factors = [_ONE] * len(bars)
    for s in splits:
        if s.ratio is None or s.ratio <= _ZERO:
            continue
        f = _ONE / s.ratio
        for i, d in enumerate(bar_dates):
            if d < s.ex_date:
                factors[i] *= f
    for div in dividends:
        if div.amount is None or div.amount <= _ZERO:
            continue
        prev_close = next((bars[i].close for i in range(len(bars) - 1, -1, -1) if bar_dates[i] < div.ex_date),
                          None)
        if not prev_close or prev_close <= _ZERO:
            continue
        f = _ONE - (div.amount / prev_close)
        if f <= _ZERO:
            continue
        for i, d in enumerate(bar_dates):
            if d < div.ex_date:
                factors[i] *= f
    out = []
    for i, bar in enumerate(bars):
        f = factors[i]
        if f == _ONE:
            out.append(bar)
            continue
        out.append(OHLCV(ticker=bar.ticker, timestamp=bar.timestamp, frequency=bar.frequency,
                         open=bar.open * f, high=bar.high * f,
                         low=bar.low * f,  close=bar.close * f,
                         volume=int(bar.volume / f) if f != _ZERO else bar.volume,
                         adjusted=True, source=bar.source))
    return out

def normalize_fundamentals(rows: List[Fundamental]) -> List[Fundamental]:
    if not rows:
        return []
    rows = sorted(rows, key=lambda r: r.period_end)
    rows = _dedupe_fundamentals(rows)
    rows = [_fill_fundamentals(r) for r in rows]
    return rows

def compute_ttm(rows: List[Fundamental]) -> Optional[Fundamental]:
    quarterly = [r for r in rows if r.frequency == Frequency.QUARTERLY]
    if len(quarterly) < 4:
        return None
    recent = quarterly[-4:]
    latest = recent[-1]

    def _sum(field: str) -> Optional[Decimal]:
        vals = [getattr(r, field) for r in recent if getattr(r, field) is not None]
        return sum(vals) if vals else None
        
    import dataclasses
    return dataclasses.replace(latest,
                               frequency=Frequency.ANNUAL,
                               fiscal_quarter=None,
                               revenue=_sum("revenue"),
                               gross_profit=_sum("gross_profit"),
                               operating_income=_sum("operating_income"),
                               ebitda=_sum("ebitda"),
                               net_income=_sum("net_income"),
                               eps_basic=_sum("eps_basic"),
                               eps_diluted=_sum("eps_diluted"),
                               operating_cf=_sum("operating_cf"),
                               capex=_sum("capex"),
                               free_cash_flow=_sum("free_cash_flow"),
                               source=f"TTM:{latest.source}")

def _dedupe_fundamentals(rows: List[Fundamental]) -> List[Fundamental]:
    seen: Dict[Tuple, Fundamental] = {}
    for r in rows:
        key = (r.ticker, r.period_end, r.frequency)
        seen[key] = r
    return sorted(seen.values(), key=lambda r: r.period_end)

def _fill_fundamentals(r: Fundamental) -> Fundamental:
    if r.free_cash_flow is None and r.operating_cf is not None and r.capex is not None:
        import dataclasses
        return dataclasses.replace(r, free_cash_flow=r.operating_cf + r.capex)
    return r

def normalize_macro(observations: List[MacroSeries]) -> List[MacroSeries]:
    if not observations:
        return []
    seen: Dict[Tuple, MacroSeries] = {}
    for obs in observations:
        key = (obs.series_id, obs.observation_date)
        seen[key] = obs
    return sorted(seen.values(), key=lambda o: o.observation_date)

