"""
Ingestion module for the Crypto Market Pipeline.

Pulls a snapshot of market data for the top N cryptocurrencies (by market
cap) from the CoinGecko public API (no API key required) and returns it
as a list of tuples ready to be inserted into the raw layer of the
warehouse.

"""
from __future__ import annotations

import datetime as dt
from typing import Any

import requests

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
REQUEST_TIMEOUT_SECONDS = 30


def fetch_top_coins_snapshot(top_n: int = 25, vs_currency: str = "usd") -> list[tuple[Any, ...]]:
    """Fetch current market data for the top `top_n` coins by market cap.

    Returns a list of tuples in the exact column order expected by the
    `raw.crypto_price_snapshots` upsert statement:
    (coin_id, symbol, name, current_price, market_cap, market_cap_rank,
     total_volume, price_change_pct_24h, snapshot_date, fetched_at)
    """
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": top_n,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    response = requests.get(COINGECKO_MARKETS_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()

    snapshot_date = dt.date.today().isoformat()
    fetched_at = dt.datetime.utcnow().isoformat()

    records: list[tuple[Any, ...]] = []
    for coin in payload:
        records.append(
            (
                coin.get("id"),
                coin.get("symbol"),
                coin.get("name"),
                coin.get("current_price"),
                coin.get("market_cap"),
                coin.get("market_cap_rank"),
                coin.get("total_volume"),
                coin.get("price_change_percentage_24h"),
                snapshot_date,
                fetched_at,
            )
        )
    return records


if __name__ == "__main__":
    for row in fetch_top_coins_snapshot(top_n=5):
        print(row)
