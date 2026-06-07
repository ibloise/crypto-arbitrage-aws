from crypto_arbitrage_aws.poller import (
    build_ticks,
    fetch_all_prices,
    get_top30_symbols,
    get_tradeable_coins,
)
from crypto_arbitrage_aws.processor import (
    ARBITRAGE_THRESHOLD_PCT,
    DB_DSN,
    DB_PATH,
    DB_TYPE,
    get_connection,
    process_persistent_tick_batch,
    save_raw_ticks,
)


def main() -> None:
    print("=== Step 1: Fetching prices via poller ===")
    top = get_top30_symbols()
    coins = get_tradeable_coins(top)
    prices = fetch_all_prices(coins)

    ticks = build_ticks(prices)

    print(f"\n=== Step 2: Saving {len(ticks)} raw ticks ===")
    save_raw_ticks(ticks)

    conn = get_connection()
    try:
        opportunities = process_persistent_tick_batch(ticks, conn)
    finally:
        conn.close()

    print(f"\n=== Step 3: {len(opportunities)} arbitrage opportunities detected ===")
    if not opportunities:
        print(f"  No spreads above {ARBITRAGE_THRESHOLD_PCT}% detected.")
        return

    for opportunity in opportunities:
        print(
            f"  {opportunity['coin']:<6}"
            f"  {opportunity['exchange_low']:<10} ${opportunity['price_low']:>12.4f}"
            f"  ->  {opportunity['exchange_high']:<10} ${opportunity['price_high']:>12.4f}"
            f"  |  spread: {opportunity['spread_pct']:.4f}%"
        )

    destination = DB_PATH if DB_TYPE == "sqlite" else DB_DSN
    print(f"\n  Saved to DB ({DB_TYPE}: {destination})")
