
from dagster import asset 
import pandas as pd 
import os
import requests
import json
from datetime import datetime, timedelta 

api_key = os.environ.get('BYBIT_API_KEY') 
api_secret = os.environ.get('BYBIT_API_SECRET') 

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_alphasrc.configurations import BybitConfig

# Function to fetch symbol data from Binance 
def fetch_symbols(category: str): 
    url = url = f"https://api-testnet.bybit.com//v5/market/tickers?category={category}"
    if category == "option":
        url = url + "&baseCoin={basecoin}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Error {e} occured in request, no symbols returned")
        return None
    
    retMsg = data["retMsg"] 
    if not retMsg == "OK":
        print(f"error {retMsg} in request, no symbols returned")
        return False
    
    result = data["result"]["list"]
    return result


# Function to fetch symbols from Bybit
@asset
def bybit_symbol_ids(config: BybitConfig):
    """Get crypto asset symbols from Bybit exchange."""
    symbol_ids = fetch_symbols("spot")
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
    

def fetch_ohlcv_data(category, symbol, interval, start_date = "1715850000", end_date = "1715901000"): 
    url = f"https://api-testnet.bybit.com/v5/market/kline?category={category}&symbol={symbol}&interval={interval}"
    # url = f"https://api-testnet.bybit.com/v5/market/kline?category={category}&symbol={symbol_id}&interval={interval}&start={start}&end={end}"
    if category == "option":
        url = url + "&baseCoin={basecoin}"   
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Error {e} occured in request, no symbols returned")
        return None

    retMsg = data["retMsg"] 
    if not retMsg == "OK":
        print(f"error {retMsg} in request, no symbols returned")
        return data

    result = data["result"]["list"]
    return result

# TODO: whats the best way to iterate through symbols and intervals to gather kline data? 
# do it in the config and attach materialise metadata to each symbol/interval combo?
# how to schedule a data asset materialisation run and check if we are up to date, or if we need to get new data?

# Define the list of symbols and intervals 
bybit_kline_intervals = ["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"]
product_types = ["spot","linear","inverse","option"]

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT'] 
INTERVALS = ["15", "60", "D"] 

# TODO: how to separate symbol/interval combos into distinct assets?
@asset(deps=[bybit_symbol_ids])
def bybit_ohlcv_data(config: BybitConfig) -> MaterializeResult:
    """Get crypto asset kline data based on symbols and intervals from Bybit api endpoint"""
    product_category = "spot"
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=365)
    data = {}
    kline = {}
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            lists = fetch_ohlcv_data(product_category, symbol, interval) # list of OHLCV (list)
            # Create the pandas DataFrame 
            df = pd.DataFrame(lists, columns = ['StartTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']) 
            print(f"Symbol: {symbol}, interval: {interval}\n")
            print(df)
            kline[interval] = df
            
        data[symbol] = kline

    result = pd.DataFrame(data)
    date = datetime.today().strftime('%Y-%m-%d')
    datafile = os.path.normpath(f"C:\\Users\\simon\\source\\repos\\alpha_sauce\\stack\\dagster-quickstart\\data\\cdd\\{config.exchange}\\data{date}.csv")
    result.to_csv(datafile, encoding='utf-8', index=False)
    print(f"RESULT is\n", result)
    
    return MaterializeResult(
        metadata={
            "exchange" : config.exchange,
            "symbol" : symbol,
            "interval" : interval,
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["StartTime", "Open", "High", "Low", "Close", "Volume"]].to_markdown()))
        }
    )

    