import asyncio

from crypto_arbitrage_aws.ws_collector import collect


def main() -> None:
    try:
        asyncio.run(collect())
    except KeyboardInterrupt:
        print("\nStopped.")
