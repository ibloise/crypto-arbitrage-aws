import sys
from pathlib import Path


def main() -> None:
    from streamlit.web import cli as streamlit_cli

    dashboard = Path(__file__).parents[1] / "dashboard.py"
    sys.argv = ["streamlit", "run", str(dashboard), *sys.argv[1:]]
    streamlit_cli.main()
