from dagster import Config

class HNStoriesConfig(Config):
    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"

class CoinMarketCapConfig(Config):
    exchange : str = "coinmarketcap"
    symbols_limit: int = 500
    symbols_path: str = "coinmarketcap_symbols.json"
    marketcap_path: str = "coinmarketcap_rankings.csv"
    
class BinanceConfig(Config):
    exchange : str = "binance"
    symbols_limit: int = 100
    symbols_path: str = "binance_symbols.json"
    intervals_path: str = "binance_klines.csv"
    
class BybitConfig(Config):
    exchange: str = 'bybit'
    symbols_limit: int = 100
    symbols_path: str = "bybit_symbols.json"
    intervals_path: str = "bybit_klines.csv"
    
class CryptoDataDownloadConfig(Config):
    exchange: str = 'cryptodatadownload'
    symbols_limit: int = 10
    symbols_path: str = "bybit_symbols.json"
    intervals_path: str = "bybit_klines.csv"