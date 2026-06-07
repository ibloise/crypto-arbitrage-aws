import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

import requests

from .contracts import make_tick

STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "GUSD", "FRAX"}
KRAKEN_SYMBOL_MAP = {"BTC": "XBT", "DOGE": "XDG"}
KRAKEN_TO_STANDARD = {value: key for key, value in KRAKEN_SYMBOL_MAP.items()}

DEFAULT_COIN_UNIVERSE = [
    "BTC",
    "ETH",
    "XRP",
    "BNB",
    "SOL",
    "DOGE",
    "ADA",
    "TRX",
    "AVAX",
    "LINK",
    "DOT",
    "BCH",
    "LTC",
    "XLM",
    "UNI",
    "ETC",
    "ATOM",
    "FIL",
    "APT",
    "ARB",
]

RequestGet = Callable[..., object]


def _warn(message: str) -> None:
    print(f"  [WARN] {message}")


class RestClient:
    base_url = ""
    base_url_env = ""

    def __init__(
        self,
        base_url: str | None = None,
        request_get: RequestGet | None = None,
    ) -> None:
        self.base_url = base_url or os.environ.get(self.base_url_env, self.base_url)
        self.request_get = request_get or requests.get

    def endpoint(self, path: str) -> str:
        return self.endpoint_for(self.base_url, path)

    @staticmethod
    def endpoint_for(base_url: str, path: str) -> str:
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    def get_json(
        self,
        path: str,
        *,
        params: dict | None = None,
        timeout: int,
        base_url: str | None = None,
    ):
        response = self.request_get(
            self.endpoint_for(base_url, path) if base_url else self.endpoint(path),
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()


class CoinUniverseClient(RestClient):
    base_url = "https://api.coingecko.com/api/v3"
    base_url_env = "COINGECKO_REST_URL"

    def top_symbols(self, limit: int = 30) -> list[str]:
        try:
            payload = self.get_json(
                "coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": max(limit, 50),
                    "page": 1,
                },
                timeout=10,
            )
            symbols = [
                coin["symbol"].upper()
                for coin in payload
                if coin["symbol"].upper() not in STABLECOINS
            ]
            return symbols[:limit]
        except Exception as exc:
            _warn(f"CoinGecko unavailable, using fallback universe: {exc}")
            return DEFAULT_COIN_UNIVERSE[:limit]


class ExchangeRestClient(RestClient, ABC):
    name: str

    @abstractmethod
    def available_coins(self) -> set[str]: ...

    @abstractmethod
    def price(self, coin: str) -> Optional[float]: ...


class BinanceRestClient(ExchangeRestClient):
    name = "binance"
    base_url = "https://api.binance.com"
    base_url_env = "BINANCE_REST_URL"

    def available_coins(self) -> set[str]:
        payload = self.get_json("api/v3/exchangeInfo", timeout=10)
        return {
            symbol["baseAsset"]
            for symbol in payload["symbols"]
            if symbol["quoteAsset"] == "USDT" and symbol["status"] == "TRADING"
        }

    def price(self, coin: str) -> float:
        payload = self.get_json(
            "api/v3/ticker/price",
            params={"symbol": f"{coin}USDT"},
            timeout=5,
        )
        return float(payload["price"])


class KrakenRestClient(ExchangeRestClient):
    name = "kraken"
    base_url = "https://api.kraken.com"
    base_url_env = "KRAKEN_REST_URL"

    def available_coins(self) -> set[str]:
        payload = self.get_json("0/public/AssetPairs", timeout=10)
        symbols = set()
        for pair_info in payload["result"].values():
            if pair_info.get("quote") not in ("ZUSD", "USD"):
                continue
            wsname = pair_info.get("wsname", "")
            if "/" not in wsname:
                continue
            kraken_base = wsname.split("/")[0]
            symbols.add(KRAKEN_TO_STANDARD.get(kraken_base, kraken_base))
        return symbols

    def price(self, coin: str) -> Optional[float]:
        kraken_coin = KRAKEN_SYMBOL_MAP.get(coin, coin)
        payload = self.get_json(
            "0/public/Ticker",
            params={"pair": f"{kraken_coin}USD"},
            timeout=5,
        )
        if payload.get("error"):
            return None
        pair_key = next(iter(payload["result"]))
        return float(payload["result"][pair_key]["c"][0])


class CoinbaseRestClient(ExchangeRestClient):
    name = "coinbase"
    base_url = "https://api.exchange.coinbase.com"
    base_url_env = "COINBASE_PRODUCTS_REST_URL"
    price_base_url = "https://api.coinbase.com"
    price_base_url_env = "COINBASE_PRICE_REST_URL"

    def __init__(
        self,
        base_url: str | None = None,
        price_base_url: str | None = None,
        request_get: RequestGet | None = None,
    ) -> None:
        super().__init__(base_url, request_get)
        self.price_base_url = price_base_url or os.environ.get(
            self.price_base_url_env,
            self.price_base_url,
        )

    def available_coins(self) -> set[str]:
        payload = self.get_json("products", timeout=10)
        return {
            product["base_currency"]
            for product in payload
            if product.get("quote_currency") == "USD"
            and product.get("status") == "online"
        }

    def price(self, coin: str) -> float:
        payload = self.get_json(
            f"v2/prices/{coin}-USD/spot",
            timeout=5,
            base_url=self.price_base_url,
        )
        return float(payload["data"]["amount"])


class BybitRestClient(ExchangeRestClient):
    name = "bybit"
    base_url = "https://api.bybit.com"
    base_url_env = "BYBIT_REST_URL"

    def available_coins(self) -> set[str]:
        payload = self.get_json(
            "v5/market/instruments-info",
            params={"category": "spot"},
            timeout=10,
        )
        return {
            instrument["baseCoin"]
            for instrument in payload["result"]["list"]
            if instrument.get("quoteCoin") == "USDT"
            and instrument.get("status") == "Trading"
        }

    def price(self, coin: str) -> Optional[float]:
        payload = self.get_json(
            "v5/market/tickers",
            params={"category": "spot", "symbol": f"{coin}USDT"},
            timeout=5,
        )
        if payload["retCode"] != 0 or not payload["result"]["list"]:
            return None
        return float(payload["result"]["list"][0]["lastPrice"])


@dataclass(frozen=True)
class PollerPlan:
    coins: list[str]
    clients: list[ExchangeRestClient]


def _enabled_exchange_names() -> set[str] | None:
    value = os.environ.get("ENABLED_EXCHANGES")
    if not value:
        return None
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def build_poller_plan(
    top30: list[str],
    clients: list[ExchangeRestClient] | None = None,
) -> PollerPlan:
    exchange_clients = build_exchange_clients() if clients is None else clients
    if not exchange_clients:
        return PollerPlan(coins=top30.copy(), clients=[])

    available: dict[str, set[str]] = {}
    reachable_clients: list[ExchangeRestClient] = []

    with ThreadPoolExecutor(max_workers=len(exchange_clients)) as executor:
        futures = {
            executor.submit(client.available_coins): client
            for client in exchange_clients
        }

        for future in as_completed(futures):
            client = futures[future]
            try:
                coins = future.result()
                available[client.name] = coins
                reachable_clients.append(client)
            except Exception as exc:
                _warn(
                    f"{client.name} availability unavailable, "
                    f"continuing without it: {exc}"
                )

    print("Coins available per reachable exchange:")
    for name, coins in available.items():
        print(f"  {name}: {len(coins)} coins")

    tradeable = (
        [coin for coin in top30 if all(coin in coins for coins in available.values())]
        if available
        else top30.copy()
    )

    print(f"\nTop coins present on all reachable exchanges: {tradeable}")
    print(
        "Reachable exchanges: " + ", ".join(client.name for client in reachable_clients)
    )

    return PollerPlan(coins=tradeable, clients=reachable_clients)


def build_exchange_clients() -> list[ExchangeRestClient]:
    clients: list[ExchangeRestClient] = [
        BinanceRestClient(),
        KrakenRestClient(),
        CoinbaseRestClient(),
        BybitRestClient(),
    ]

    enabled = _enabled_exchange_names()
    if enabled is None:
        return clients

    return [client for client in clients if client.name in enabled]


def get_top30_symbols(client: CoinUniverseClient | None = None) -> list[str]:
    return (client or CoinUniverseClient()).top_symbols()


def get_tradeable_coins(
    top30: list[str],
    clients: list[ExchangeRestClient] | None = None,
) -> list[str]:
    return build_poller_plan(top30, clients).coins


def fetch_all_prices(
    coins: list[str],
    clients: list[ExchangeRestClient] | None = None,
) -> dict[str, dict[str, Optional[float]]]:
    exchange_clients = build_exchange_clients() if clients is None else clients
    results: dict[str, dict[str, Optional[float]]] = {coin: {} for coin in coins}
    if not exchange_clients:
        return results

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(client.price, coin): (coin, client)
            for coin in coins
            for client in exchange_clients
        }
        for future in as_completed(futures):
            coin, client = futures[future]
            try:
                results[coin][client.name] = future.result()
            except Exception as exc:
                _warn(f"{coin} @ {client.name}: {exc}")
                results[coin][client.name] = None

    return results


def build_ticks(prices: dict[str, dict[str, Optional[float]]]) -> list[dict]:
    return [
        make_tick(exchange, coin, price, source_mode="rest")
        for coin, exchange_prices in prices.items()
        for exchange, price in exchange_prices.items()
        if price is not None
    ]
