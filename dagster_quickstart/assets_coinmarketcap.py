#Python code that uses Dagster to define and download OHLCV (Open, High, Low, Close, Volume) data from CoinMarketCap on a daily schedule. 
# First, make sure you have Dagster and the CoinMarketCap API package installed: ```bash pip install dagster dagster-core dagster-cron requests pandas ``` Now, here's the Python code: ```python 
import os 
import requests 
import json
import pandas as pd 
from datetime import datetime, timedelta 
from dagster import asset, op, ConfigurableResource, Config, job, OpExecutionContext, ScheduleDefinition, Definitions
import requests


# CMC_API_KEY = os.environ.get('CMC_API_KEY') 
CMC_API_KEY : str ="62f1e2cd-f9ba-46bf-8d50-8d7dd7706613"
CMC_BASE_URL : str = 'https://pro-api.coinmarketcap.com' 

# Custom headers to bypass rate limits 
HEADERS : dict = { 'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY, } 

# Top cryptocurrencies by market cap 
SYMBOLS = ['BTC', 'ETH', 'XRP', 'ADA', 'DOT'] 

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_quickstart.configurations import CMCRankingConfig

# Function to get cryptocurrency IDs from CoinMarketCap 
def fetch_cmc_crypto_map(): 
    url = f"{CMC_BASE_URL}/v1/cryptocurrency/map" 
    try:
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error {e} occured in request, no symbols returned")
        return None

    result = resp.json()
    return result["data"]

# Function to get OHLCV data from CoinMarketCap 
def fetch_cmc_rank_data(): 
    url = f"{CMC_BASE_URL}/v1/cryptocurrency/listings/latest"
    try:
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error {e} occured in request, no symbols returned")
        return None
    result = resp.json()
    data = result["data"]
    return data


@asset(deps=[])
def cmc_symbol_ids(config: CMCRankingConfig):
    """Get crypto asset symbols from Coinmarketcap"""
    symbol_ids = fetch_cmc_crypto_map()
    if symbol_ids is None:
        print("error!")
        
    else:
        with open(config.cmc_symbols_path, "w") as f:
            json.dump(symbol_ids[: config.marketcap_limit], f)
            
    return MaterializeResult(
        metadata={
            "exchange" : "coinmarketcap",
            "type"  : "crypto symbols",
            "num_records": (0 if (symbol_ids is None) else len(symbol_ids)),
            "preview": ("Download Failed" if symbol_ids is None else MetadataValue.md(str(pd.Series(symbol_ids[:20]).to_markdown()))),
        }
    )

@asset(deps=[])
def cmc_rank_data(config: CMCRankingConfig) -> MaterializeResult:
    """Get crypto asset market capitalisation data from coinmarketcap api endpoint"""
    rankings = fetch_cmc_rank_data()
    df = pd.DataFrame.from_dict(rankings, orient='index', columns=["symbol", "name", "cmc_rank", "market_cap_dominance", "date_added", "last_updated"])
    if df is None:
        print("DF error!")
    else:
        df.to_csv(config.cmc_marketcap_path, index=False)

    return MaterializeResult(
        metadata={
            "exchange" : "coinmarketcap",
            "type"  : "market capitalisation data",
            "num_records": (0 if (df is None) else len(df)),
            "preview": ("Failed" if df is None else MetadataValue.md(str(df[["symbol", "name", "cmc_rank", "date_added", "market_cap_dominance", "last_updated"]].to_markdown()))),
        }
    )


defs = Definitions(
    assets=[cmc_symbol_ids, cmc_rank_data],
)


