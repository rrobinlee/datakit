from __future__ import annotations
import logging
import os
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from datakit.adapters.base import AdapterBase, AdapterError, MacroAdapter
from datakit.models.schema import Frequency, MacroSeries
logger = logging.getLogger(__name__)
_BASE_URL = "https://api.stlouisfed.org/fred"
_FREQ_MAP: dict[str, Frequency] = {"Daily": Frequency.DAILY,
                                   "Weekly": Frequency.WEEKLY,
                                   "Biweekly": Frequency.WEEKLY,
                                   "Monthly": Frequency.MONTHLY,
                                   "Quarterly": Frequency.QUARTERLY,
                                   "Semiannual": Frequency.MONTHLY,
                                   "Annual": Frequency.ANNUAL}

class FREDAdapter(AdapterBase, MacroAdapter):
    SOURCE_NAME = "FRED"
    def _configure(self, **kwargs: Any) -> None:
        try:
            import requests 
            self._requests = requests
        except ImportError as exc:
            raise AdapterError("need pip install requests") from exc
        self.api_key = self.api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise AdapterError("FRED API key required. Pass api_key= or set FRED_API_KEY")
        self._session = self._requests.Session()
        self._session.params = {"api_key": self.api_key, "file_type": "json"}

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{_BASE_URL}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_macro_series(self, series_id: str, start: Optional[date] = None, end: Optional[date] = None) -> List[MacroSeries]:
        series_id = series_id.upper()
        meta = self._get("series", {"series_id": series_id})
        info = (meta.get("seriess") or [{}])[0]
        name = info.get("title", series_id)
        unit = info.get("units", "")
        freq_enum = _FREQ_MAP.get(info.get("frequency", ""), Frequency.MONTHLY)

        params: Dict[str, Any] = {"series_id": series_id}
        if start:
            params["observation_start"] = start.isoformat()
        if end:
            params["observation_end"] = end.isoformat()
        data = self._get("series/observations", params)

        results: List[MacroSeries] = []
        for obs in data.get("observations", []):
            raw = obs.get("value", ".")
            value = None if raw in (".", "") else Decimal(raw)
            results.append(MacroSeries(series_id=series_id,
                                       name=name,
                                       observation_date=date.fromisoformat(obs["date"]),
                                       value=value,
                                       unit=unit,
                                       frequency=freq_enum,
                                       source=self.SOURCE_NAME))
        return results

    def search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        data = self._get("series/search", {"search_text": query, "limit": limit})
        return data.get("seriess", [])

