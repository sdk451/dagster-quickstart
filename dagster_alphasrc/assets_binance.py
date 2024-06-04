
from dagster import asset 
from binance.client import Client 
import pandas as pd 
import os
import requests
import json
from datetime import datetime, timedelta 
 # Initialize Binance client 
 
api_key = os.environ.get('BINANCE_API_KEY') 
api_secret = os.environ.get('BINANCE_API_SECRET') 
client = Client(api_key=api_key, api_secret=api_secret)

# Define the list of symbols and intervals 
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT'] 
INTERVALS = [Client.KLINE_INTERVAL_1HOUR, Client.KLINE_INTERVAL_1DAY] 

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_alphasrc.configurations import BinanceConfig

# Function to fetch symbol data from Binance 
def fetch_symbols(): 
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Error {e} occured in request, no symbols returned")
        return None
    
    result = data["symbols"]
    return result
   

# Function to fetch OHLCV data from Binance 
def fetch_ohlcv_data(symbol, interval, start_date, end_date): 
    klines = client.get_historical_klines(symbol, interval, start_date, end_date) 
    df = pd.DataFrame(klines, columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore']) 
    df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
    df['Symbol'] = symbol
    df['Interval'] = interval 
    df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric) 
    return df[['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume' ]] 

@asset
def binance_symbol_ids(config: BinanceConfig):
    """Get crypto asset symbols from Binance."""
    symbol_ids = fetch_symbols()
    if not symbol_ids is None:
        with open(config.symbols_path, "w") as f:
            json.dump(symbol_ids[: config.symbols_limit], f)
    
    return MaterializeResult(
        metadata={
            "exchange" : config.exchange,
            "asset_type": "spot",
            "num_records": (0 if (symbol_ids is None) else len(symbol_ids)),
            "preview": MetadataValue.md(str(pd.Series(symbol_ids[:20]).to_markdown())),
        }
    )

# TODO: whats the best way to iterate through symbols and intervals to gather kline data? 
# do it in the config and attach materialise metadata to each symbol/interval combo?
# how to schedule a data asset materialisation run and check if we are up to date, or if we need to get new data?

@asset(deps=[binance_symbol_ids])
def binance_ohlcv_data(config: BinanceConfig) -> MaterializeResult:
    """Get crypto asset kline data based on symbols and intervals from Binance api endpoint"""
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=365) 
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            # Fetch 1 year of data
            # context.log.info(f"Fetching {interval} OHLCV data for {symbol} from {start_date} to {end_date}") 
            df = fetch_ohlcv_data(symbol, interval, start_date.strftime("%d %b %Y %H:%M:%S"), end_date.strftime("%d %b %Y %H:%M:%S")) 

    return MaterializeResult(
        metadata={
            "exchange" : config.exchange,
            "symbol" : symbol,
            "interval" : interval,
            "num_records" : len(df),
            "preview": MetadataValue.md(str(df[["Open Time", "Open", "High", "Low", "Close", "Volume"]].to_markdown())),
        }
    )

    