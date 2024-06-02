from dagster import Definitions, load_assets_from_modules

from . import assets, assets_binance, assets_coinmarketcap

all_assets = load_assets_from_modules([assets, assets_binance, assets_coinmarketcap])

defs = Definitions(
    assets=all_assets,
)
