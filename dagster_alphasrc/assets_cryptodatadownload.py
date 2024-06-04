
from dagster import asset 
import pandas as pd 
from datetime import datetime
import os

api_key = os.environ.get('CRYPTO_DATA_DOWNLOAD_API_KEY') 
api_secret = os.environ.get('CRYPTO_DATA_DOWNLOAD_API_SECRET') 

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_alphasrc.configurations import CryptoDataDownloadConfig


# https://www.cryptodatadownload.com/data/ - full table of 12,569 timeseries

# exchange symbol / data list
# https://www.cryptodatadownload.com/data/kucoin/
# https://www.cryptodatadownload.com/data/gemini/ - daily, hourly, minute data for gemini exchange
# https://www.cryptodatadownload.com/data/binance/
# https://www.cryptodatadownload.com/data/poloniex/

print(os.getcwd())
datapath = os.path.normpath("\\data\\cdd")

# import exchange symbols for cryptodatadownload
def load_symbol_ids(exchange:str, filename:str):
    
    symbols_file = os.path.normpath(f"C:\\Users\\simon\\source\\repos\\alpha_sauce\\stack\\dagster-quickstart\\data\\cdd\\{exchange}\\{filename}")
    # symbols_file = os.path.normpath(os.path.join(datapath, exchange, filename))
    cdd_symbols = pd.read_csv(symbols_file, skiprows=1)
    symbol_links = cdd_symbols[['Symbol','Url']]
    print(f"LOADED {len(symbol_links)} {exchange} symbols")
    return symbol_links
   

# Function to fetch OHLCV data from cryptodatadownload
def fetch_ohlcv_data(exchange, symbol_file, symbols): 
    # get csv data files for each symbol
    symbols_urls = load_symbol_ids(exchange, symbol_file)
    metadata = None
    symbols = {}
    if not symbols_urls.empty:
        metadata = {}
        date = datetime.today().strftime('%Y-%m-%d')
        for row in symbols_urls.iloc[0:symbols].itertuples(index=False):
            symbol = row[0]
            url = row[1]
            print(f"{symbol_file}: loading {symbol} from {url}...")
            try:
                df = pd.read_csv(url, skiprows=1)
            except pd.errors as e:
                print(f"An exception {e} occurred")
                continue

            filename = f"{symbol}_{date}.csv"
            datafile = os.path.normpath(f"C:\\Users\\simon\\source\\repos\\alpha_sauce\\stack\\dagster-quickstart\\data\\cdd\\{exchange}\\{filename}")
            # TODO: save each symbol data as individual csv file here, write out a metadata dataframe of exchange, symbol, start / end data date 
            df.to_csv(datafile, encoding='utf-8', index=False)
            metadata[symbol] = date
    
    return metadata


@asset
def cdd_gemini_1m(config: CryptoDataDownloadConfig):
    """Get crypto gemini 1m data from CDD."""
    exchange = 'gemini'
    filename = 'CDD_Gemini_1m_symbols.csv'

    metadata = fetch_ohlcv_data(exchange, filename, config.symbols_limit)
    result = pd.DataFrame(metadata)
    
    return MaterializeResult(
        metadata={
            "exchange" : exchange,
            "interval" : "1 minute",
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["Symbol", "Date"]].to_markdown())),
        }
    )
    
@asset
def cdd_gemini_1h_1d(config: CryptoDataDownloadConfig):
    """Get crypto gemini 1h and 1D data from CDD."""
    exchange = 'gemini'
    filename = 'CDD_Gemini_symbols.csv'
    
    metadata = fetch_ohlcv_data(exchange, filename, config.symbols_limit)
    result = pd.DataFrame(metadata)
    
    return MaterializeResult(
        metadata={
            "exchange" : exchange,
            "interval" : "1 hour, 1 day",
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["Symbol", "Date"]].to_markdown())),
        }
    )

@asset
def cdd_kucoin_1h_1d(config: CryptoDataDownloadConfig):
    """Get crypto kucoin 1h and 1D data from CDD."""
    exchange = 'kucoin'
    filename = 'CDD_Kucoin_symbols.csv'
    
    metadata = fetch_ohlcv_data(exchange, filename, config.symbols_limit)
    result = pd.DataFrame(metadata)
    
    return MaterializeResult(
        metadata={
            "exchange" : exchange,
            "interval" : "1 hour, 1 day",
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["Symbol", "Date"]].to_markdown())),
        }
    )
    

@asset
def cdd_binance_futures_um(config: CryptoDataDownloadConfig):
    """Get crypto kucoin 1h and 1D data from CDD."""
    exchange = 'binance'
    filename = 'CDD_Binance_UM_futures_symbols.csv'
    
    metadata = fetch_ohlcv_data(exchange, filename, config.symbols_limit)
    result = pd.DataFrame(metadata)
    
    return MaterializeResult(
        metadata={
            "exchange" : exchange,
            "interval" : "1 hour, 1 day",
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["Symbol", "Date"]].to_markdown())),
        }
    )

@asset
def cdd_polionex_1h_1d(config: CryptoDataDownloadConfig):
    """Get crypto kucoin 1h and 1D data from CDD."""
    
    exchange = 'polionex'
    filename = 'CDD_Polionex_symbols.csv'
    
    metadata = fetch_ohlcv_data(exchange, filename, config.symbols_limit)
    result = pd.DataFrame(metadata)
    
    return MaterializeResult(
        metadata={
            "exchange" : exchange,
            "interval" : "1 hour, 1 day",
            "num_records": len(result),
            "preview" : MetadataValue.md(str(result[["Symbol", "Date"]].to_markdown())),
        }
    )


    