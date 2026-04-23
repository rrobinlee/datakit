from setuptools import setup, find_packages

setup(
    name="datakit",
    version="0.1.0",
    description="Python library to ingest and normalize yfinance and FRED data",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "requests>=2.31",
        "yfinance>=0.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7",
        ],
    }
)