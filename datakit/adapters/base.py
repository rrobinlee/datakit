# AdapterBase: lifecycle
# PriceAdapter: get_ohlcv, get_dividends, get_splits, get_fundamentals
# MacroAdapter: get_macro_series
# FilingsAdapter: get_filings
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List, Optional
from datakit.models.schema import Dividend, Fundamental, MacroSeries, OHLCV, SECFiling, Split

class AdapterError(Exception):
    pass

class AdapterBase:
    SOURCE_NAME: str = "unknown"

    def __init__(self, api_key: Optional[str] = None, **kwargs: Any):
        self.api_key = api_key
        self._session: Any = None
        self._configure(**kwargs)

    def _configure(self, **kwargs: Any) -> None:
        pass

    def close(self) -> None:
        if hasattr(self._session, "close"):
            self._session.close()

    def __enter__(self) -> "AdapterBase":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        caps = []
        if isinstance(self, PriceAdapter):
            caps.append("price")
        if isinstance(self, MacroAdapter):
            caps.append("macro")
        return f"<{self.__class__.__name__} source={self.SOURCE_NAME!r} caps={caps}>"

class PriceAdapter(ABC):
    @abstractmethod
    def get_ohlcv(self, ticker: str, start: date, end: date, frequency: str = "daily", adjusted: bool = True) -> List[OHLCV]: 
        pass

    @abstractmethod
    def get_dividends(self, ticker: str, start: Optional[date] = None, end: Optional[date] = None) -> List[Dividend]: 
        pass

    @abstractmethod
    def get_splits(self, ticker: str, start: Optional[date] = None, end: Optional[date] = None) -> List[Split]: 
        pass

    @abstractmethod
    def get_fundamentals(self, ticker: str, frequency: str = "quarterly", start: Optional[date] = None, end: Optional[date] = None) -> List[Fundamental]: 
        pass

class MacroAdapter(ABC):
    @abstractmethod
    def get_macro_series(self, series_id: str, start: Optional[date] = None, end: Optional[date] = None) -> List[MacroSeries]: 
        pass

class FilingsAdapter(ABC):
    @abstractmethod
    def get_filings(self, ticker: str, filing_type: Optional[str] = None, limit: int = 10) -> List[SECFiling]: 
        pass


