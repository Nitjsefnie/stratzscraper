"""Progress reporting helpers."""

from __future__ import annotations

from ..database import db_connection

__all__ = ["fetch_progress"]


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
