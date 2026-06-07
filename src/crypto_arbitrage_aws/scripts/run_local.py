from crypto_arbitrage_aws.run_local import run


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopped.")
