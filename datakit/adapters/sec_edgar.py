from __future__ import annotations
import logging
import time
from datetime import date
from typing import Any, Dict, Iterator, List, Optional
from datakit.adapters.base import AdapterBase, AdapterError, FilingsAdapter
from datakit.models.schema import FilingType, SECFiling

logger = logging.getLogger(__name__)
_EDGAR_BASE = "https://data.sec.gov"
_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_RATE_LIMIT_DELAY = 0.15


class SECEdgarAdapter(AdapterBase, FilingsAdapter):
    SOURCE_NAME = "SEC EDGAR"
    def _configure(self, user_agent: str = "datakit dev@example.com", **kwargs: Any) -> None:
        try:
            import requests
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": user_agent})
        except ImportError as exc:
            raise AdapterError("need pip install requests") from exc
        self._ticker_cik_map: Dict[str, str] = {}
        self._last_request: float = 0.0

    def _get(self, url: str) -> Any:
        elapsed = time.time() - self._last_request
        if elapsed < _RATE_LIMIT_DELAY:
            time.sleep(_RATE_LIMIT_DELAY - elapsed)
        resp = self._session.get(url, timeout=30)
        self._last_request = time.time()
        resp.raise_for_status()
        return resp.json()

    def _load_ticker_map(self) -> None:
        if self._ticker_cik_map:
            return
        data = self._get(_TICKERS_URL)
        for entry in data.values():
            ticker = str(entry.get("ticker", "")).upper()
            cik = str(entry.get("cik_str", "")).zfill(10)
            if ticker:
                self._ticker_cik_map[ticker] = cik

    def resolve_cik(self, ticker: str) -> str:
        self._load_ticker_map()
        cik = self._ticker_cik_map.get(ticker.upper())
        if not cik:
            raise AdapterError(f"CIK not found for ticker: {ticker}")
        return cik

    def get_filings(self, ticker: str, filing_type: Optional[str] = None, limit: int = 10) -> List[SECFiling]:
        cik = self.resolve_cik(ticker)
        data = self._get(f"{_EDGAR_BASE}/submissions/CIK{cik}.json")
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        filed_dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        periods = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])
        _aliases = {"10-K": FilingType.TEN_K, "10-KSB": FilingType.TEN_K,
                    "10-Q": FilingType.TEN_Q, "10-QSB": FilingType.TEN_Q,
                    "8-K": FilingType.EIGHT_K,
                    "DEF 14A": FilingType.DEF_14A,
                    "S-1": FilingType.S_1, "S-1/A": FilingType.S_1,
                    "4": FilingType.FOUR, "4/A": FilingType.FOUR,
                    "SC 13G": FilingType.SC_13G, "SC 13G/A": FilingType.SC_13G,
                    "SC 13D": FilingType.SC_13D, "SC 13D/A": FilingType.SC_13D}

        results: List[SECFiling] = []
        for i, form in enumerate(forms):
            if filing_type and form != filing_type:
                continue
            if form not in _aliases:
                continue
            acc = accessions[i] if i < len(accessions) else ""
            acc_clean = acc.replace("-", "")
            doc = primary_docs[i] if i < len(primary_docs) else ""
            period_str = periods[i] if i < len(periods) else None

            results.append(SECFiling(ticker=ticker.upper(),
                                     cik=cik,
                                     filing_type=_aliases[form],
                                     filed_date=date.fromisoformat(filed_dates[i]),
                                     period_of_report=date.fromisoformat(period_str) if period_str else None,
                                     accession_number=acc,
                                     primary_document_url=(f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{doc}" if doc else ""),
                                     full_submission_url=(f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}&dateb=&owner=include&count=40"),
                                     description=descriptions[i] if i < len(descriptions) else ""))
            if len(results) >= limit:
                break
        return results

