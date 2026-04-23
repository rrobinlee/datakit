from __future__ import annotations
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional
from datakit.adapters.base import AdapterBase, AdapterError, PriceAdapter
from datakit.models.schema import (CorporateActionType, Dividend, Frequency, Fundamental, OHLCV, Split)
logger = logging.getLogger(__name__)

_FREQ_MAP = {"1min": ("1m", Frequency.MINUTE_1),
             "5min": ("5m", Frequency.MINUTE_5),
             "15min": ("15m", Frequency.MINUTE_15),
             "1h": ("1h", Frequency.HOUR_1),
             "daily": ("1d", Frequency.DAILY),
             "weekly": ("1wk", Frequency.WEEKLY),
             "monthly": ("1mo", Frequency.MONTHLY)}

class YahooAdapter(AdapterBase, PriceAdapter):
    SOURCE_NAME = "yahoo_finance"
    def _configure(self, **kwargs: Any) -> None:
        try:
            import yfinance 
            self._yf = yfinance
        except ImportError as exc:
            raise AdapterError("need pip install yfinance") from exc

    def get_ohlcv(self, ticker: str, start: date, end: date, frequency: str = "daily", adjusted: bool = True) -> List[OHLCV]:
        yf_interval, freq_enum = _FREQ_MAP.get(frequency, ("1d", Frequency.DAILY))
        df = self._yf.Ticker(ticker).history(start=start.isoformat(),
                                             end=end.isoformat(),
                                             interval=yf_interval,
                                             auto_adjust=adjusted,
                                             actions=False)
        if df is None or df.empty:
            logger.warning("Yahoo returned no OHLCV for %s", ticker)
            return []
        bars: List[OHLCV] = []
        for ts, row in df.iterrows():
            ts_utc = (ts.to_pydatetime().astimezone(timezone.utc).replace(tzinfo=None)
                      if getattr(ts, "tzinfo", None) else ts.to_pydatetime())
            bars.append(OHLCV(ticker=ticker.upper(),
                              timestamp=ts_utc,
                              frequency=freq_enum,
                              open=Decimal(str(row["Open"])),
                              high=Decimal(str(row["High"])),
                              low=Decimal(str(row["Low"])),
                              close=Decimal(str(row["Close"])),
                              volume=int(row.get("Volume", 0)),
                              adjusted=adjusted,
                              source=self.SOURCE_NAME))
        return bars

    def get_dividends(self, ticker: str, start: Optional[date] = None, end: Optional[date] = None) -> List[Dividend]:
        series = self._yf.Ticker(ticker).dividends
        if series is None or series.empty:
            return []
        results: List[Dividend] = []
        for ts, amount in series.items():
            ex_dt = ts.date() if hasattr(ts, "date") else ts
            if start and ex_dt < start:
                continue
            if end and ex_dt > end:
                continue
            results.append(Dividend(ticker=ticker.upper(),
                                    ex_date=ex_dt,
                                    amount=Decimal(str(amount)),
                                    source=self.SOURCE_NAME))
        return results

    def get_splits(self, ticker: str, start: Optional[date] = None, end: Optional[date] = None) -> List[Split]:
        series = self._yf.Ticker(ticker).splits
        if series is None or series.empty:
            return []
        results: List[Split] = []
        for ts, ratio in series.items():
            ex_dt = ts.date() if hasattr(ts, "date") else ts
            if start and ex_dt < start:
                continue
            if end and ex_dt > end:
                continue
            results.append(Split(ticker=ticker.upper(),
                                 ex_date=ex_dt,
                                 ratio=Decimal(str(ratio)),
                                 action_type=(CorporateActionType.SPLIT if ratio >= 1 else CorporateActionType.REVERSE_SPLIT),
                                 source=self.SOURCE_NAME))
        return results

    def get_fundamentals(self, ticker: str, frequency: str = "quarterly", start: Optional[date] = None, end: Optional[date] = None) -> List[Fundamental]:
        t = self._yf.Ticker(ticker)
        freq_enum = Frequency.QUARTERLY if frequency == "quarterly" else Frequency.ANNUAL
        if frequency == "quarterly":
            income = t.quarterly_income_stmt
            balance = t.quarterly_balance_sheet
            cashflow = t.quarterly_cashflow
        else:
            income = t.income_stmt
            balance = t.balance_sheet
            cashflow = t.cashflow
        if income is None or income.empty:
            return []
        results: List[Fundamental] = []
        for col in income.columns:
            period_end = col.date() if hasattr(col, "date") else col
            if start and period_end < start:
                continue
            if end and period_end > end:
                continue
                
            def _get(df: Any, row: str) -> Optional[Decimal]:
                try:
                    val = df.at[row, col]
                    if val is None or str(val) == "nan":
                        return None
                    return Decimal(str(val))
                except Exception:
                    return None
            results.append(Fundamental(ticker=ticker.upper(),
                                       period_end=period_end,
                                       frequency=freq_enum,
                                       fiscal_year=period_end.year,
                                       fiscal_quarter=((period_end.month - 1) // 3 + 1) if frequency == "quarterly" else None,
                                       revenue=_get(income, "Total Revenue"),
                                       gross_profit=_get(income, "Gross Profit"),
                                       operating_income=_get(income, "Operating Income"),
                                       ebitda=_get(income, "EBITDA"),
                                       net_income=_get(income, "Net Income"),
                                       eps_basic=_get(income, "Basic EPS"),
                                       eps_diluted=_get(income, "Diluted EPS"),
                                       total_assets=_get(balance, "Total Assets"),
                                       total_liabilities=_get(balance, "Total Liabilities Net Minority Interest"),
                                       total_equity=_get(balance, "Total Equity Gross Minority Interest"),
                                       cash=_get(balance, "Cash And Cash Equivalents"),
                                       long_term_debt=_get(balance, "Long Term Debt"),
                                       operating_cf=_get(cashflow, "Operating Cash Flow"),
                                       capex=_get(cashflow, "Capital Expenditure"),
                                       free_cash_flow=_get(cashflow, "Free Cash Flow"),
                                       source=self.SOURCE_NAME))
            return results

