"""Background worker for refreshing leaderboard materialized views."""

from __future__ import annotations

import logging
import threading
from datetime import timedelta

from ..database import refresh_leaderboard_views

__all__ = ["ensure_leaderboard_refresh_scheduler", "LEADERBOARD_REFRESH_INTERVAL"]


LEADERBOARD_REFRESH_INTERVAL = timedelta(hours=12)

_LOGGER = logging.getLogger(__name__)

_refresh_thread: threading.Thread | None = None
_refresh_stop_event: threading.Event | None = None
_refresh_lock = threading.Lock()


def _refresh_worker(stop_event: threading.Event) -> None:
    interval_seconds = max(int(LEADERBOARD_REFRESH_INTERVAL.total_seconds()), 1)
    while not stop_event.is_set():
        try:
            refresh_leaderboard_views()
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Failed to refresh leaderboard materialized views")
        if stop_event.wait(interval_seconds):
            break


def ensure_leaderboard_refresh_scheduler() -> None:
    """Start the background worker that refreshes leaderboard views."""

    global _refresh_thread, _refresh_stop_event
    with _refresh_lock:
        if _refresh_thread and _refresh_thread.is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_refresh_worker,
            args=(stop_event,),
            name="leaderboard-refresh",
            daemon=True,
        )
        thread.start()
        _refresh_thread = thread
        _refresh_stop_event = stop_event
    try:
        refresh_leaderboard_views()
    except Exception:  # pragma: no cover - best effort logging
        _LOGGER.exception("Initial leaderboard refresh failed")
