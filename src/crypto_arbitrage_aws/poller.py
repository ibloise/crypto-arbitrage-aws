import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .contracts import make_tick

# ---------------------------------------------------------------------------
# Stablecoins to exclude: they're pegged to USD so arbitrage isn't meaningful
# ---------------------------------------------------------------------------
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "GUSD", "FRAX"}

# Kraken uses non-standard ticker symbols for a few coins
KRAKEN_SYMBOL_MAP = {
    "BTC": "XBT",
    "DOGE": "XDG",
}


# ---------------------------------------------------------------------------
# Step 1 — Get top 30 coins by market cap (CoinGecko, free, no API key)
# ---------------------------------------------------------------------------

def get_top30_symbols() -> list[str]:
    """Returns top 30 non-stablecoin coin symbols by market cap."""
    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,   # fetch extra to absorb stablecoin filtering
            "page": 1,
        },
        timeout=10,
    )
    r.raise_for_status()

    symbols = [
        coin["symbol"].upper()
        for coin in r.json()
        if coin["symbol"].upper() not in STABLECOINS
    ]
    return symbols[:30]


# ---------------------------------------------------------------------------
# Step 2 — Get available symbols per exchange
# ---------------------------------------------------------------------------

def get_binance_available() -> set[str]:
    """Coins listed on Binance with an active USDT spot pair."""
    r = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=10)
    r.raise_for_status()
    return {
        s["baseAsset"]
        for s in r.json()["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


def get_kraken_available() -> set[str]:
    """Coins listed on Kraken with an active USD spot pair, normalized to standard symbols."""
    r = requests.get("https://api.kraken.com/0/public/AssetPairs", timeout=10)
    r.raise_for_status()

    kraken_to_standard = {v: k for k, v in KRAKEN_SYMBOL_MAP.items()}
    symbols = set()

    for pair_info in r.json()["result"].values():
        if pair_info.get("quote") not in ("ZUSD", "USD"):
            continue
        # wsname is the cleanest field: "XBT/USD", "ETH/USD", "SOL/USD", etc.
        wsname = pair_info.get("wsname", "")
        if "/" not in wsname:
            continue
        kraken_base = wsname.split("/")[0]
        standard = kraken_to_standard.get(kraken_base, kraken_base)
        symbols.add(standard)

    return symbols


def get_coinbase_available() -> set[str]:
    """Coins listed on Coinbase Exchange with an active USD spot pair."""
    r = requests.get("https://api.exchange.coinbase.com/products", timeout=10)
    r.raise_for_status()
    return {
        p["base_currency"]
        for p in r.json()
        if p.get("quote_currency") == "USD" and p.get("status") == "online"
    }


def get_bybit_available() -> set[str]:
    """Coins listed on Bybit with an active USDT spot pair."""
    r = requests.get(
        "https://api.bybit.com/v5/market/instruments-info",
        params={"category": "spot"},
        timeout=10,
    )
    r.raise_for_status()
    return {
        i["baseCoin"]
        for i in r.json()["result"]["list"]
        if i.get("quoteCoin") == "USDT" and i.get("status") == "Trading"
    }


# ---------------------------------------------------------------------------
# Step 3 — Intersection: keep only coins present on ALL 4 exchanges
# ---------------------------------------------------------------------------

def get_tradeable_coins(top30: list[str]) -> list[str]:
    """Returns the subset of top30 that is listed on every exchange."""
    fetchers = {
        "binance": get_binance_available,
        "kraken": get_kraken_available,
        "coinbase": get_coinbase_available,
        "bybit": get_bybit_available,
    }

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {name: ex.submit(fn) for name, fn in fetchers.items()}
        available = {name: future.result() for name, future in futures.items()}

    print("Coins available per exchange:")
    for name, coins in available.items():
        print(f"  {name}: {len(coins)} coins")

    tradeable = [
        coin for coin in top30
        if all(coin in available[ex] for ex in available)
    ]

    print(f"\nTop-30 coins present on ALL exchanges: {tradeable}")
    return tradeable


# ---------------------------------------------------------------------------
# Step 4 — Fetch current price per coin per exchange
# ---------------------------------------------------------------------------

def _price_binance(coin: str) -> Optional[float]:
    r = requests.get(
        "https://api.binance.com/api/v3/ticker/price",
        params={"symbol": f"{coin}USDT"},
        timeout=5,
    )
    r.raise_for_status()
    return float(r.json()["price"])


def _price_kraken(coin: str) -> Optional[float]:
    kraken_coin = KRAKEN_SYMBOL_MAP.get(coin, coin)
    r = requests.get(
        "https://api.kraken.com/0/public/Ticker",
        params={"pair": f"{kraken_coin}USD"},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        return None
    pair_key = next(iter(data["result"]))
    return float(data["result"][pair_key]["c"][0])  # "c" = last trade close price


def _price_coinbase(coin: str) -> Optional[float]:
    r = requests.get(
        f"https://api.coinbase.com/v2/prices/{coin}-USD/spot",
        timeout=5,
    )
    r.raise_for_status()
    return float(r.json()["data"]["amount"])


def _price_bybit(coin: str) -> Optional[float]:
    r = requests.get(
        "https://api.bybit.com/v5/market/tickers",
        params={"category": "spot", "symbol": f"{coin}USDT"},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json()
    if data["retCode"] != 0 or not data["result"]["list"]:
        return None
    return float(data["result"]["list"][0]["lastPrice"])


PRICE_FETCHERS = {
    "binance": _price_binance,
    "kraken": _price_kraken,
    "coinbase": _price_coinbase,
    "bybit": _price_bybit,
}


def fetch_all_prices(coins: list[str]) -> dict[str, dict[str, Optional[float]]]:
    """
    Fetches prices for every coin on every exchange in parallel.

    Returns:
        {
            "BTC": {"binance": 67000.0, "kraken": 67012.5, "coinbase": 66998.1, "bybit": 67005.0},
            "ETH": { ... },
            ...
        }
    """
    results: dict[str, dict[str, Optional[float]]] = {coin: {} for coin in coins}

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {
            ex.submit(fetcher, coin): (coin, exchange)
            for coin in coins
            for exchange, fetcher in PRICE_FETCHERS.items()
        }
        for future in as_completed(futures):
            coin, exchange = futures[future]
            try:
                results[coin][exchange] = future.result()
            except Exception as e:
                print(f"  [WARN] {coin} @ {exchange}: {e}")
                results[coin][exchange] = None

    return results


def build_ticks(prices: dict[str, dict[str, Optional[float]]]) -> list[dict]:
    return [
        make_tick(exchange, coin, price, source_mode="rest")
        for coin, exchange_prices in prices.items()
        for exchange, price in exchange_prices.items()
        if price is not None
    ]
