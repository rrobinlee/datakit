# datakit

A Python library to ingest and normalize yfinance and FRED data, including cleaned prices, dividends, splits, fundamentals, and macro series. Future sources: SEC EDGAR, Quandl

## Installation

```bash
pip install yfinance requests
cd /path/to/datakit
pip install -e .
```

Jupyter:

```python
!pip install yfinance requests
!pip install /path/to/datakit
!pip install git+https://github.com/rrobinlee/datakit.git
```

## Project structure

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