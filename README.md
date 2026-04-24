# datakit

A Python library to ingest and normalize yfinance, FRED, and SEC EDGAR data; includes cleaned prices, dividends, filings, fundamentals, and macro series. Future sources: Quandl

```
datakit/
├── __init__.py
├── models/
│   └── schema.py
├── adapters/
│   ├── base.py             
│   ├── yahoo.py      
│   └── fred.py
├── normalizers/
│   └── normalizers.py
├── pipeline/
│   └── pipeline.py
└── tests/
    └── test_datakit.py
