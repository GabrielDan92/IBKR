"""
Dashboard launcher — reads port from constants so nothing is hardcoded.

Used as the dashboard container's entrypoint.
"""

from __future__ import annotations

import sys

from streamlit.web.cli import main as st_main

from config.constants import DASHBOARD_PORT


def main() -> None:
    sys.argv = [
        "streamlit",
        "run",
        "/opt/app/src/dashboard/app.py",
        f"--server.port={DASHBOARD_PORT}",
        "--server.address=0.0.0.0",
    ]
    st_main()


if __name__ == "__main__":
    main()
