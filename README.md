# datakit

A Python library to ingest and normalize yfinance, FRED, and SEC EDGAR data; includes cleaned prices, dividends, filings, fundamentals, and macro series. Consolidated frequently-used items from previous university projects into a single framework.

```
datakit/
├── __init__.py
├── models/
│   └── schema.py
├── adapters/
│   ├── base.py             
│   ├── yahoo.py
│   ├── sec_edgar.py       
│   └── fred.py
├── normalizers/
│   └── normalizers.py
└── pipeline/
    └── pipeline.py

