from __future__ import annotations
import logging
from datetime import date
from typing import Any, Dict, List, Optional
from datakit.adapters.base import AdapterError, MacroAdapter, PriceAdapter
from datakit.models.schema import Dividend, Fundamental, MacroSeries, OHLCV, Split
from datakit.normalizers import (compute_ttm, normalize_fundamentals, normalize_macro, normalize_prices)
logger = logging.getLogger(__name__) # use logger to save log messages (rate limits, retry attempts, etc)

class Pipeline:

    def __init__(
        self,
        price_adapter: Optional[PriceAdapter] = None,
        macro_adapter: Optional[MacroAdapter] = None,
        cache: Optional[Dict[str, Any]] = None,
    ):
        self._price = price_adapter
        self._macro = macro_adapter
        self._cache = cache

    def get_prices(
        self,
        ticker: str,
        start: date,
        end: date,
        frequency: str = "daily",
        adjusted: bool = True,
    ) -> List[OHLCV]:
        key = f"prices:{ticker}:{start}:{end}:{frequency}:{adjusted}"
        if self._cache is not None and key in self._cache:
            return self._cache[key]
        self._require_price()
        bars = self._price.get_ohlcv(ticker, start, end, frequency, adjusted)
        splits = dividends = []
        if not adjusted:
            splits = self._price.get_splits(ticker, start, end)
            dividends = self._price.get_dividends(ticker, start, end)
        result = normalize_prices(bars, splits or None, dividends or None)
        if self._cache is not None:
            self._cache[key] = result
        return result

    def get_dividends(
        self,
        ticker: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dividend]:
        self._require_price()
        return self._price.get_dividends(ticker, start, end)

    def get_splits(
        self,
        ticker: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Split]:
        self._require_price()
        return self._price.get_splits(ticker, start, end)

    def get_fundamentals(
        self,
        ticker: str,
        frequency: str = "quarterly",
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Fundamental]:
        self._require_price()
        raw = self._price.get_fundamentals(ticker, frequency, start, end)
        return normalize_fundamentals(raw)

    def get_ttm(self, ticker: str) -> Optional[Fundamental]:
        rows = self.get_fundamentals(ticker, frequency="quarterly")
        return compute_ttm(rows)

    def get_macro(
        self,
        series_id: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[MacroSeries]:
        self._require_macro()
        raw = self._macro.get_macro_series(series_id, start, end)
        return normalize_macro(raw)

    def close(self) -> None:
        for adapter in (self._price, self._macro):
            if adapter is not None:
                adapter.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _require_price(self) -> None:
        if self._price is None:
            raise AdapterError("No price_adapter configured.")

    def _require_macro(self) -> None:
        if self._macro is None:
            raise AdapterError("No macro_adapter configured.")