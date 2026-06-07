import asyncio

from crypto_arbitrage_aws.observability import configure_logging
from crypto_arbitrage_aws.ws_collector import collect


def main() -> None:
    configure_logging()
    try:
        asyncio.run(collect())
    except KeyboardInterrupt:
        print("\nStopped.")
