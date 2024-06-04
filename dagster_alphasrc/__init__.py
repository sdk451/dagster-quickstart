from dagster import Definitions, load_assets_from_modules

from . import assets, assets_binance, assets_bybit, assets_coinmarketcap, assets_cryptodatadownload

all_assets = load_assets_from_modules([assets, assets_binance, assets_bybit, assets_coinmarketcap, assets_cryptodatadownload])

defs = Definitions(
    assets=all_assets,
)
