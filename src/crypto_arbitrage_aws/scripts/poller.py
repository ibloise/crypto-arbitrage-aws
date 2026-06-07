from crypto_arbitrage_aws.observability import configure_logging
from crypto_arbitrage_aws.poller import (
    fetch_all_prices,
    get_top30_symbols,
    get_tradeable_coins,
)


def main() -> None:
    configure_logging()
    print("=== Step 1: Top 30 coins by market cap ===")
    top30 = get_top30_symbols()
    print(top30)

    print("\n=== Step 2: Exchange availability + intersection ===")
    coins = get_tradeable_coins(top30)

    if not coins:
        print("No coins found on all exchanges - exiting.")
        raise SystemExit(1)

    print(f"\n=== Step 3: Fetching prices for {len(coins)} coins ===")
    prices = fetch_all_prices(coins)

    print()
    for coin, exchange_prices in prices.items():
        values = "  |  ".join(
            f"{exchange}: ${price:>12.4f}" if price else f"{exchange}: N/A"
            for exchange, price in exchange_prices.items()
        )
        print(f"{coin:<6} {values}")
