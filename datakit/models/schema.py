from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
# data models

class Frequency(str, Enum):
    MINUTE_1 = "1min"
    MINUTE_5 = "5min"
    MINUTE_15 = "15min"
    HOUR_1 = "1h"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    
class CorporateActionType(str, Enum):
    SPLIT = "split"
    REVERSE_SPLIT = "reverse_split"
    DIVIDEND_CASH = "dividend_cash"

class FilingType(str, Enum):
    TEN_K = "10-K"
    TEN_Q = "10-Q"
    EIGHT_K = "8-K"
    DEF_14A = "DEF 14A"
    S_1 = "S-1"
    FOUR = "4"
    SC_13G = "SC 13G"
    SC_13D = "SC 13D"

@dataclass
class OHLCV:
    ticker: str
    timestamp: datetime          
    frequency: Frequency
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted: bool = False
    source: str  = "unknown"

@dataclass
class Dividend:
    ticker: str
    ex_date: date
    amount: Decimal
    currency: str = "USD"
    source: str = "unknown"

@dataclass
class Split:
    ticker: str
    ex_date: date
    ratio: Decimal #new/old
    action_type: CorporateActionType = CorporateActionType.SPLIT
    source: str = "unknown"

@dataclass
class Fundamental:
    ticker: str
    period_end: date
    frequency: Frequency
    fiscal_year: int
    fiscal_quarter: Optional[int] #None when annual
    #income statement
    revenue: Optional[Decimal] = None
    gross_profit: Optional[Decimal] = None
    operating_income: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None
    net_income: Optional[Decimal] = None
    eps_basic: Optional[Decimal] = None
    eps_diluted: Optional[Decimal] = None
    #balance sheet
    total_assets: Optional[Decimal] = None
    total_liabilities:Optional[Decimal] = None
    total_equity: Optional[Decimal] = None
    cash: Optional[Decimal] = None
    long_term_debt: Optional[Decimal] = None
    #cash flow
    operating_cf: Optional[Decimal] = None
    capex: Optional[Decimal] = None
    free_cash_flow: Optional[Decimal] = None
    currency: str = "USD"
    source: str = "unknown"

@dataclass
class MacroSeries:
    series_id: str #refer to fred for id
    name: str
    observation_date: date
    value: Optional[Decimal]
    unit: str
    frequency: Frequency
    source: str = "FRED"

@dataclass
class SECFiling:
    ticker: str
    cik: str
    filing_type: FilingType
    filed_date: date
    period_of_report: Optional[date]
    accession_number: str
    primary_document_url: str
    full_submission_url: str
    description: str = ""
    source: str = "SEC EDGAR"

