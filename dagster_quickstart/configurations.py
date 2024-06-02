from dagster import Config

class HNStoriesConfig(Config):
    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"

class CMCRankingConfig(Config):
    marketcap_limit: int = 500
    cmc_symbols_path: str = "coinmarketcap_symbols.json"
    cmc_marketcap_path: str = "coinmarketcap_rankings.csv"
    
class BinanceKlineConfig(Config):
    symbols_limit: int = 100
    binance_symbols_path: str = "binance_symbols.json"
    binance_intervals_path: str = "binance_klines.csv"