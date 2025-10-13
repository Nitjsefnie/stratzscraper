"""Progress reporting helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping

from ..database import db_connection, retryable_execute

__all__ = [
    "fetch_progress",
    "list_progress_snapshots",
    "record_progress_snapshot",
]


def fetch_progress() -> dict:
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE hero_done=TRUE) AS hero_done,
                COUNT(*) FILTER (WHERE discover_done=TRUE) AS discover_done
            FROM players
            """
        ).fetchone()
        if row is None:
            return {"players_total": 0, "hero_done": 0, "discover_done": 0}
        total = row["total"] or 0
        hero_done = row["hero_done"] or 0
        discover_done = row["discover_done"] or 0
    return {
        "players_total": total,
        "hero_done": hero_done,
        "discover_done": discover_done,
    }


def _normalize_captured_at(captured_at: datetime | None) -> datetime:
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    elif captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    return captured_at.replace(minute=0, second=0, microsecond=0)


def record_progress_snapshot(
    progress: Mapping[str, int] | None = None,
    *,
    captured_at: datetime | None = None,
) -> dict:
    """Persist a snapshot of current progress metrics.

    Parameters
    ----------
    progress:
        Optional mapping containing ``players_total``, ``hero_done`` and
        ``discover_done`` counters. When omitted the values are pulled from the
        live ``/progress`` view.
    captured_at:
        Optional timestamp indicating when the snapshot was captured. The value
        is normalized to the start of the hour in UTC to keep a single row per
        hour.
    """

    captured_at = _normalize_captured_at(captured_at)
    if progress is None:
        progress = fetch_progress()
    else:
        progress = dict(progress)
    required_keys = ("players_total", "hero_done", "discover_done")
    normalized: dict[str, int] = {}
    for key in required_keys:
        value = int(progress.get(key, 0))
        normalized[key] = value

    with db_connection(write=True) as conn:
        cur = conn.cursor()
        retryable_execute(
            cur,
            """
            INSERT INTO progress_snapshots (
                captured_at,
                players_total,
                hero_done,
                discover_done
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (captured_at) DO UPDATE
            SET
                players_total=EXCLUDED.players_total,
                hero_done=EXCLUDED.hero_done,
                discover_done=EXCLUDED.discover_done
            """,
            (
                captured_at,
                normalized["players_total"],
                normalized["hero_done"],
                normalized["discover_done"],
            ),
        )

    snapshot = {
        "captured_at": captured_at,
        **normalized,
    }
    return snapshot


def list_progress_snapshots(*, limit: int | None = None) -> list[dict]:
    """Return stored progress snapshots ordered chronologically."""

    sql = (
        """
        SELECT captured_at, players_total, hero_done, discover_done
        FROM progress_snapshots
        ORDER BY captured_at ASC
        """
    )
    parameters = ()
    if limit is not None:
        if limit <= 0:
            return []
        sql += " LIMIT %s"
        parameters = (limit,)

    with db_connection() as conn:
        rows = conn.execute(sql, parameters).fetchall()

    return [
        {
            "captured_at": row["captured_at"],
            "players_total": row["players_total"],
            "hero_done": row["hero_done"],
            "discover_done": row["discover_done"],
        }
        for row in rows
    ]
