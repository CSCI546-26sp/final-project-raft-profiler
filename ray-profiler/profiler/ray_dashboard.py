import os


def dashboard_port(default: int = 52367) -> int:
    v = os.environ.get("RAY_DASHBOARD_PORT")
    return int(v) if v else default
