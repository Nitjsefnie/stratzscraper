"""Leaderboard helpers."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from ..database import db_connection, row_value
from ..heroes import HEROES, HERO_SLUGS, hero_slug

__all__ = ["fetch_best_payload", "fetch_hero_leaderboard", "fetch_overall_leaderboard"]


def fetch_hero_leaderboard(slug: str) -> Optional[Tuple[str, str, List[dict]]]:
    normalized = slug.strip().replace(" ", "_").lower()
    hero_entry = HERO_SLUGS.get(normalized)
    if not hero_entry:
        return None
    hero_id, hero_name = hero_entry
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT steamAccountId, matches, wins
            FROM hero_top100
            WHERE heroId=%s
            ORDER BY matches DESC, wins DESC, steamAccountId ASC
            LIMIT 100
            """,
            (hero_id,),
        ).fetchall()
    players = [
        {
            "steamAccountId": row_value(row, "steamAccountId"),
            "matches": row["matches"],
            "wins": row["wins"],
        }
        for row in rows
    ]
    return hero_name, normalized, players


def fetch_overall_leaderboard() -> List[Dict[str, object]]:
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT heroId, steamAccountId, matches, wins
            FROM hero_top100
            ORDER BY matches DESC, wins DESC, steamAccountId ASC
            LIMIT 100
            """
        ).fetchall()
    players: List[Dict[str, object]] = []
    for row in rows:
        hero_id = row_value(row, "heroId")
        hero_name = HEROES.get(hero_id)
        hero_slug_value = hero_slug(hero_name) if isinstance(hero_name, str) else None
        players.append(
            {
                "steamAccountId": row_value(row, "steamAccountId"),
                "matches": row["matches"] or 0,
                "wins": row["wins"] or 0,
                "heroName": hero_name,
                "heroSlug": hero_slug_value,
            }
        )
    return players


def fetch_best_payload() -> List[Dict]:
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT heroId, steamAccountId, matches, wins
            FROM (
                SELECT
                    heroId,
                    steamAccountId,
                    matches,
                    wins,
                    ROW_NUMBER() OVER (
                        PARTITION BY heroId
                        ORDER BY matches DESC, wins DESC, steamAccountId
                    ) AS rn
                FROM hero_top100
            ) ranked
            WHERE rn = 1
            ORDER BY matches DESC, wins DESC, steamAccountId ASC
            """
        ).fetchall()
    payload: List[Dict] = []
    for row in rows:
        hero_id = row_value(row, "heroId")
        hero_name = HEROES.get(hero_id)
        payload.append(
            {
                "hero_id": hero_id,
                "hero_name": hero_name,
                "player_id": row_value(row, "steamAccountId"),
                "matches": row["matches"],
                "wins": row["wins"],
                "hero_slug": hero_slug(hero_name) if isinstance(hero_name, str) else None,
            }
        )
    return payload
