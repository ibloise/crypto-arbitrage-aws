"""
WebSocket collector for normalized exchange ticks.

Each exchange client owns its URL, subscription and message normalization.
The base client owns connection lifecycle and emits contract dictionaries.
"""

import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Awaitable, Callable

from websockets.asyncio.client import ClientConnection, connect

from .contracts import make_tick
from .kinesis import publish_ticks
from .poller import KRAKEN_SYMBOL_MAP, get_top30_symbols, get_tradeable_coins

KINESIS_STREAM = os.environ.get("KINESIS_STREAM", "")
BATCH_INTERVAL = int(os.environ.get("BATCH_INTERVAL", "30"))
RECONNECT_DELAY = 5
WS_OPEN_TIMEOUT = int(os.environ.get("WS_OPEN_TIMEOUT", "10"))
WS_CLOSE_TIMEOUT = int(os.environ.get("WS_CLOSE_TIMEOUT", "10"))
WS_PING_INTERVAL = int(os.environ.get("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = int(os.environ.get("WS_PING_TIMEOUT", "20"))
BATCH_PROCESSING_TIMEOUT = int(os.environ.get("BATCH_PROCESSING_TIMEOUT", "60"))
KRAKEN_TO_STD = {value: key for key, value in KRAKEN_SYMBOL_MAP.items()}
LOGGER = logging.getLogger(__name__)

RawMessage = dict[str, Any] | list[Any] | str
TickEmitter = Callable[[dict[str, Any]], Awaitable[None]]


class ExchangeTickerClient(ABC):
    name: str
    ws_url: str
    ws_url_env: str

    def __init__(
        self,
        coins: list[str],
        emit: TickEmitter,
        reconnect_seconds: int = RECONNECT_DELAY,
        ws_url: str | None = None,
    ) -> None:
        self.coins = coins
        self.emit = emit
        self.reconnect_seconds = reconnect_seconds
        self.ws_url = ws_url or os.environ.get(self.ws_url_env, self.ws_url)
        LOGGER.info(
            "WebSocket client configured exchange=%s url=%s tracked_coins=%d "
            "override_env=%s overridden=%s",
            self.name,
            self.connection_url,
            len(self.coins),
            self.ws_url_env,
            bool(os.environ.get(self.ws_url_env)),
        )

    @property
    def connection_url(self) -> str:
        return self.ws_url

    async def run_forever(self) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                LOGGER.info(
                    "WebSocket connecting exchange=%s attempt=%d url=%s",
                    self.name,
                    attempt,
                    self.connection_url,
                )
                await self._connect_and_consume()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning(
                    "WebSocket disconnected exchange=%s attempt=%d url=%s "
                    "error_type=%s error=%s reconnect_seconds=%d",
                    self.name,
                    attempt,
                    self.connection_url,
                    type(exc).__name__,
                    exc,
                    self.reconnect_seconds,
                    exc_info=LOGGER.isEnabledFor(logging.DEBUG),
                )
                await asyncio.sleep(self.reconnect_seconds)

    async def _connect_and_consume(self) -> None:
        async with connect(
            self.connection_url,
            open_timeout=WS_OPEN_TIMEOUT,
            close_timeout=WS_CLOSE_TIMEOUT,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
        ) as websocket:
            LOGGER.info(
                "WebSocket connected exchange=%s url=%s",
                self.name,
                self.connection_url,
            )
            await self._subscribe(websocket)
            background_tasks = [
                asyncio.create_task(coroutine)
                for coroutine in self.background_coroutines(websocket)
            ]
            try:
                async for raw_message in websocket:
                    try:
                        tick = self.normalize_message(self.decode_message(raw_message))
                        if tick is not None:
                            await self.emit(tick)
                    except Exception as exc:
                        LOGGER.warning(
                            "WebSocket message rejected exchange=%s error_type=%s "
                            "error=%s raw=%r",
                            self.name,
                            type(exc).__name__,
                            exc,
                            str(raw_message)[:500],
                            exc_info=LOGGER.isEnabledFor(logging.DEBUG),
                        )
            finally:
                for task in background_tasks:
                    task.cancel()
                if background_tasks:
                    await asyncio.gather(*background_tasks, return_exceptions=True)

    async def _subscribe(self, websocket: ClientConnection) -> None:
        messages = self.subscription_messages()
        LOGGER.info(
            "WebSocket subscribing exchange=%s messages=%d tracked_coins=%d",
            self.name,
            len(messages),
            len(self.coins),
        )
        for message in messages:
            LOGGER.debug("WebSocket subscription exchange=%s payload=%s", self.name, message)
            await websocket.send(json.dumps(message))

    @staticmethod
    def decode_message(raw_message: str | bytes) -> RawMessage:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        try:
            return json.loads(raw_message)
        except json.JSONDecodeError:
            return raw_message

    def subscription_messages(self) -> list[dict[str, Any]]:
        return []

    def background_coroutines(
        self,
        websocket: ClientConnection,
    ) -> list[Awaitable[None]]:
        return []

    def tracked_tick(self, coin: str, price: float) -> dict[str, Any] | None:
        if coin not in self.coins:
            return None
        return make_tick(self.name, coin, price, source_mode="websocket")

    @abstractmethod
    def normalize_message(self, message: RawMessage) -> dict[str, Any] | None: ...


class BinanceTickerClient(ExchangeTickerClient):
    name = "binance"
    ws_url = "wss://stream.binance.com:9443/stream"
    ws_url_env = "BINANCE_WS_URL"

    @property
    def connection_url(self) -> str:
        streams = "/".join(f"{coin.lower()}usdt@ticker" for coin in self.coins)
        if "{streams}" in self.ws_url:
            return self.ws_url.format(streams=streams)
        separator = "&" if "?" in self.ws_url else "?"
        return f"{self.ws_url}{separator}streams={streams}"

    def normalize_message(self, message: RawMessage) -> dict[str, Any] | None:
        if not isinstance(message, dict) or not message.get("data"):
            return None
        ticker = message["data"]
        coin = ticker["s"].removesuffix("USDT")
        return self.tracked_tick(coin, float(ticker["c"]))


class KrakenTickerClient(ExchangeTickerClient):
    name = "kraken"
    ws_url = "wss://ws.kraken.com"
    ws_url_env = "KRAKEN_WS_URL"

    def subscription_messages(self) -> list[dict[str, Any]]:
        pairs = [f"{KRAKEN_SYMBOL_MAP.get(coin, coin)}/USD" for coin in self.coins]
        return [
            {
                "event": "subscribe",
                "pair": pairs,
                "subscription": {"name": "ticker"},
            }
        ]

    def normalize_message(self, message: RawMessage) -> dict[str, Any] | None:
        if not (
            isinstance(message, list)
            and len(message) == 4
            and message[2] == "ticker"
        ):
            return None
        kraken_coin = message[3].split("/")[0]
        coin = KRAKEN_TO_STD.get(kraken_coin, kraken_coin)
        return self.tracked_tick(coin, float(message[1]["c"][0]))


class CoinbaseTickerClient(ExchangeTickerClient):
    name = "coinbase"
    ws_url = "wss://ws-feed.exchange.coinbase.com"
    ws_url_env = "COINBASE_WS_URL"

    def subscription_messages(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "subscribe",
                "product_ids": [f"{coin}-USD" for coin in self.coins],
                "channels": ["ticker"],
            }
        ]

    def normalize_message(self, message: RawMessage) -> dict[str, Any] | None:
        if (
            not isinstance(message, dict)
            or message.get("type") != "ticker"
            or "price" not in message
        ):
            return None
        coin = message["product_id"].removesuffix("-USD")
        return self.tracked_tick(coin, float(message["price"]))


class BybitTickerClient(ExchangeTickerClient):
    name = "bybit"
    ws_url = "wss://stream.bybit.com/v5/public/spot"
    ws_url_env = "BYBIT_WS_URL"

    def subscription_messages(self) -> list[dict[str, Any]]:
        return [
            {
                "op": "subscribe",
                "args": [f"tickers.{coin}USDT" for coin in self.coins],
            }
        ]

    def background_coroutines(
        self,
        websocket: ClientConnection,
    ) -> list[Awaitable[None]]:
        return [self._keep_alive(websocket)]

    async def _keep_alive(self, websocket: ClientConnection) -> None:
        while True:
            await asyncio.sleep(20)
            await websocket.send(json.dumps({"op": "ping"}))

    def normalize_message(self, message: RawMessage) -> dict[str, Any] | None:
        if (
            not isinstance(message, dict)
            or not message.get("topic", "").startswith("tickers.")
            or "data" not in message
        ):
            return None
        coin = message["data"]["symbol"].removesuffix("USDT")
        price = message["data"].get("lastPrice")
        if not price:
            return None
        return self.tracked_tick(coin, float(price))


class TickBatchProcessor:
    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        interval: int = BATCH_INTERVAL,
        kinesis_stream: str = KINESIS_STREAM,
    ) -> None:
        self.queue = queue
        self.interval = interval
        self.kinesis_stream = kinesis_stream

    async def run_forever(self) -> None:
        if not self.kinesis_stream:
            self._init_database()
        while True:
            await asyncio.sleep(self.interval)
            ticks = self._drain_queue()
            if not ticks:
                continue

            timestamp = datetime.now().strftime("%H:%M:%S")
            LOGGER.info(
                "WebSocket batch ready timestamp=%s ticks=%d interval_seconds=%d "
                "destination=%s",
                timestamp,
                len(ticks),
                self.interval,
                self.kinesis_stream or "local",
            )
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._handle_ticks, ticks),
                    timeout=BATCH_PROCESSING_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.error(
                    "WebSocket batch failed ticks=%d destination=%s error_type=%s "
                    "error=%s; collector continues",
                    len(ticks),
                    self.kinesis_stream or "local",
                    type(exc).__name__,
                    exc,
                    exc_info=LOGGER.isEnabledFor(logging.DEBUG),
                )

    def _handle_ticks(self, ticks: list[dict[str, Any]]) -> None:
        if self.kinesis_stream:
            self._publish_to_kinesis(ticks)
        else:
            self._process_locally(ticks)

    def _drain_queue(self) -> list[dict[str, Any]]:
        ticks = []
        while not self.queue.empty():
            ticks.append(self.queue.get_nowait())
        return ticks

    @staticmethod
    def _init_database() -> None:
        from .processor import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.close()

    def _publish_to_kinesis(self, ticks: list[dict[str, Any]]) -> None:
        kinesis = self._get_kinesis_client()
        publish_ticks(kinesis, self.kinesis_stream, ticks)
        LOGGER.info(
            "WebSocket batch published stream=%s ticks=%d",
            self.kinesis_stream,
            len(ticks),
        )

    @staticmethod
    def _get_kinesis_client():
        import boto3

        return boto3.client("kinesis")

    @staticmethod
    def _process_locally(ticks: list[dict[str, Any]]) -> None:
        from .processor import (
            ARBITRAGE_THRESHOLD_PCT,
            get_connection,
            process_persistent_tick_batch,
            save_raw_ticks,
        )

        save_raw_ticks(ticks)
        conn = get_connection()
        try:
            opportunities = process_persistent_tick_batch(ticks, conn)
        finally:
            conn.close()
        if not opportunities:
            print(f"no spreads above {ARBITRAGE_THRESHOLD_PCT}%")
            return

        print(f"{len(opportunities)} opportunity/ies detected!")
        for opportunity in opportunities:
            print(
                f"  {opportunity['coin']:<6} "
                f"{opportunity['exchange_low']} ${opportunity['price_low']:,.4f}"
                f" → {opportunity['exchange_high']} ${opportunity['price_high']:,.4f}"
                f"  |  spread: {opportunity['spread_pct']:.4f}%"
            )


def build_clients(
    coins: list[str],
    emit: TickEmitter,
) -> list[ExchangeTickerClient]:
    clients = [
        BinanceTickerClient(coins, emit),
        KrakenTickerClient(coins, emit),
        CoinbaseTickerClient(coins, emit),
        BybitTickerClient(coins, emit),
    ]
    enabled_value = os.environ.get("ENABLED_EXCHANGES")
    if not enabled_value:
        return clients
    enabled = {
        exchange.strip().lower()
        for exchange in enabled_value.split(",")
        if exchange.strip()
    }
    selected = [client for client in clients if client.name in enabled]
    LOGGER.info(
        "WebSocket exchanges selected enabled=%s selected=%s",
        sorted(enabled),
        [client.name for client in selected],
    )
    return selected


async def collect() -> None:
    LOGGER.info("Fetching WebSocket coin universe")
    coins = get_tradeable_coins(get_top30_symbols())
    if not coins:
        raise RuntimeError("No tradeable coins available for WebSocket collection")
    LOGGER.info("WebSocket tracking coins count=%d coins=%s", len(coins), coins)

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    clients = build_clients(coins, queue.put)
    if not clients:
        raise RuntimeError("No WebSocket exchanges enabled")
    processor = TickBatchProcessor(queue)

    mode = f"Kinesis stream '{KINESIS_STREAM}'" if KINESIS_STREAM else "local mode"
    LOGGER.info(
        "Starting WebSocket collector exchanges=%s output=%s",
        [client.name for client in clients],
        mode,
    )

    await asyncio.gather(
        *(client.run_forever() for client in clients),
        processor.run_forever(),
    )
