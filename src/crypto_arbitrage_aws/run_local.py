"""
Simulates the AWS pipeline locally:
  EventBridge + Lambda Poller + Lambda Processor

Fetches the coin universe once at startup, then polls every POLL_INTERVAL seconds.
"""
import os
import time
from datetime import datetime

from .contracts import make_tick
from .poller import fetch_all_prices, get_top30_symbols, get_tradeable_coins
from .processor import (
    ARBITRAGE_THRESHOLD_PCT,
    get_connection,
    init_db,
    process_persistent_tick_batch,
    save_raw_ticks,
)

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))  # match this to EventBridge in AWS


def build_ticks(prices: dict) -> list[dict]:
    return [
        make_tick(exchange, coin, price, source_mode="rest")
        for coin, exchange_prices in prices.items()
        for exchange, price in exchange_prices.items()
        if price is not None
    ]


def run() -> None:
    # --- One-time setup ---
    print("=== Fetching coin universe (once) ===")
    top    = get_top30_symbols()
    coins  = get_tradeable_coins(top)
    print(f"Tracking {len(coins)} coins: {coins}\n")

    conn = get_connection()
    init_db(conn)
    conn.close()

    print(f"=== Poll loop started — every {POLL_INTERVAL}s | threshold: {ARBITRAGE_THRESHOLD_PCT}% ===")
    print("Press Ctrl+C to stop\n")

    iteration = 0
    while True:
        iteration += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] #{iteration} — fetching prices...", end=" ", flush=True)

        prices       = fetch_all_prices(coins)
        ticks        = build_ticks(prices)
        save_raw_ticks(ticks)
        conn = get_connection()
        try:
            opportunities = process_persistent_tick_batch(ticks, conn)
        finally:
            conn.close()

        if opportunities:
            print(f"{len(opportunities)} opportunity/ies detected!")
            for opp in opportunities:
                print(
                    f"  {opp['coin']:<6} "
                    f"{opp['exchange_low']} ${opp['price_low']:,.4f}"
                    f" → {opp['exchange_high']} ${opp['price_high']:,.4f}"
                    f"  |  spread: {opp['spread_pct']:.4f}%"
                )
        else:
            print(f"no spreads above {ARBITRAGE_THRESHOLD_PCT}%")

        time.sleep(POLL_INTERVAL)
