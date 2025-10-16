"""Background submission helpers."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Iterator, List

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    retryable_executemany,
    row_value,
)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DISCOVERY_SUBMISSION_LOCK_ID = 0x646973636f766572  # "discover"
_DISCOVERY_BATCH_SIZE = 500

__all__ = [
    "BACKGROUND_EXECUTOR",
    "process_discover_submission",
    "process_hero_submission",
    "submit_discover_submission",
    "submit_hero_submission",
]


def _submit_background(func, /, *args, **kwargs) -> None:
    def _runner() -> None:
        try:
            func(*args, **kwargs)
        finally:
            close_cached_connections()

    BACKGROUND_EXECUTOR.submit(_runner)


def _unmark_hero_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET hero_done=FALSE,
                    hero_refreshed_at=NULL,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        traceback.print_exc()


def _unmark_discover_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET discover_done=FALSE,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        traceback.print_exc()


def _extract_hero_rows(
    steam_account_id: int, heroes_payload: Iterable[dict] | None
) -> tuple[List[tuple[int, int, int, int]], List[int]]:
    hero_stats_rows: List[tuple[int, int, int, int]] = []
    hero_ids: List[int] = []
    seen: set[int] = set()
    if heroes_payload is None:
        return hero_stats_rows, hero_ids
    for hero in heroes_payload:
        try:
            hero_id = int(hero["heroId"])
            matches_value = hero.get("matches", hero.get("games"))
            if matches_value is None:
                continue
            matches = int(matches_value)
            wins = int(hero.get("wins", 0))
        except (KeyError, TypeError, ValueError):
            continue
        hero_stats_rows.append((steam_account_id, hero_id, matches, wins))
        if hero_id not in seen:
            hero_ids.append(hero_id)
            seen.add(hero_id)
    return hero_stats_rows, hero_ids


def _iter_discovered_counts(
    values: Iterable[object] | None,
) -> Iterator[tuple[int, int]]:
    if values is None:
        return
    for value in values:
        candidate_id: object | None
        count_value: int | None = 1
        if isinstance(value, dict):
            candidate_id = value.get("steamAccountId")
            if candidate_id is None:
                candidate_id = value.get("id")
            count_raw = value.get("count")
            if count_raw is None:
                count_raw = value.get("seenCount")
            if count_raw is not None:
                try:
                    count_value = int(count_raw)
                except (TypeError, ValueError):
                    count_value = 0
        else:
            candidate_id = value
        try:
            normalized_id = int(candidate_id)
        except (TypeError, ValueError):
            continue
        if normalized_id <= 0:
            continue
        if count_value is None:
            continue
        if not isinstance(count_value, int):
            try:
                count_value = int(count_value)
            except (TypeError, ValueError):
                continue
        if count_value <= 0:
            continue
        yield normalized_id, count_value


def _iter_discovered_child_rows(
    discovered_payload: Iterable[object] | None,
    *,
    parent_id: int,
    next_depth: int,
    batch_size: int,
) -> Iterator[List[tuple[int, int, int]]]:
    effective_batch_size = max(1, batch_size)
    pending: OrderedDict[int, int] = OrderedDict()
    for candidate_id, count in _iter_discovered_counts(discovered_payload):
        if candidate_id == parent_id:
            continue
        existing = pending.get(candidate_id)
        if existing is None:
            pending[candidate_id] = count
        else:
            pending[candidate_id] = existing + count
        if len(pending) >= effective_batch_size:
            batch = [
                (pid, next_depth, total)
                for pid, total in pending.items()
                if total > 0
            ]
            if batch:
                yield batch
            pending = OrderedDict()
    if pending:
        batch = [
            (pid, next_depth, total)
            for pid, total in pending.items()
            if total > 0
        ]
        if batch:
            yield batch


def _resolve_next_depth(
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> int:
    if provided_next_depth is not None:
        return provided_next_depth
    parent_depth_value = provided_depth
    if parent_depth_value is None:
        if assignment_depth is not None:
            parent_depth_value = assignment_depth
        else:
            parent_depth_value = 0
    return parent_depth_value + 1


def _coerce_optional_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def process_hero_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
) -> None:
    hero_stats_rows, hero_ids = _extract_hero_rows(steam_account_id, heroes_payload)
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if hero_stats_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                        matches = CASE
                            WHEN excluded.matches > hero_stats.matches
                            THEN excluded.matches
                            ELSE hero_stats.matches
                        END,
                        wins = CASE
                            WHEN excluded.matches > hero_stats.matches
                            THEN excluded.wins
                            ELSE hero_stats.wins
                        END
                    """,
                    hero_stats_rows,
                )
            if hero_ids:
                stats_rows = retryable_execute(
                    cur,
                    """
                    SELECT heroId, matches, wins
                    FROM hero_stats
                    WHERE steamAccountId=%s AND heroId = ANY(%s)
                    """,
                    (steam_account_id, list(hero_ids)),
                ).fetchall()
                stats_by_hero = {
                    int(row_value(row, "heroId")): (
                        int(row_value(row, "matches") or 0),
                        int(row_value(row, "wins") or 0),
                    )
                    for row in stats_rows
                }
                if not stats_by_hero:
                    return
                hero_keys = list(stats_by_hero.keys())
                existing_rows = retryable_execute(
                    cur,
                    """
                    SELECT heroId, matches, wins
                    FROM hero_top100
                    WHERE steamAccountId=%s AND heroId = ANY(%s)
                    """,
                    (steam_account_id, hero_keys),
                ).fetchall()
                existing_by_hero = {
                    int(row_value(row, "heroId")): (
                        int(row_value(row, "matches") or 0),
                        int(row_value(row, "wins") or 0),
                    )
                    for row in existing_rows
                }
                count_rows = retryable_execute(
                    cur,
                    """
                    SELECT heroId, COUNT(*) AS total
                    FROM hero_top100
                    WHERE heroId = ANY(%s)
                    GROUP BY heroId
                    """,
                    (hero_keys,),
                ).fetchall()
                counts_by_hero = {
                    int(row_value(row, "heroId")): int(row_value(row, "total") or 0)
                    for row in count_rows
                }
                threshold_rows = retryable_execute(
                    cur,
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
                                ORDER BY matches DESC, wins DESC, steamAccountId ASC
                            ) AS rn
                        FROM hero_top100
                        WHERE heroId = ANY(%s)
                    ) ranked
                    WHERE rn = 100
                    """,
                    (hero_keys,),
                ).fetchall()
                thresholds_by_hero = {
                    int(row_value(row, "heroId")): (
                        int(row_value(row, "steamAccountId")),
                        int(row_value(row, "matches") or 0),
                        int(row_value(row, "wins") or 0),
                    )
                    for row in threshold_rows
                }
                for hero_id, stats in stats_by_hero.items():
                    matches, wins = stats
                    existing_stats = existing_by_hero.get(hero_id)
                    if existing_stats is not None:
                        existing_matches, existing_wins = existing_stats
                        if existing_matches != matches or existing_wins != wins:
                            retryable_execute(
                                cur,
                                """
                                UPDATE hero_top100
                                SET matches=%s, wins=%s
                                WHERE heroId=%s AND steamAccountId=%s
                                """,
                                (matches, wins, hero_id, steam_account_id),
                            )
                        continue
                    hero_count = counts_by_hero.get(hero_id, 0)
                    if hero_count < 100:
                        retryable_execute(
                            cur,
                            """
                            INSERT INTO hero_top100 (heroId, steamAccountId, matches, wins)
                            VALUES (%s,%s,%s,%s)
                            """,
                            (hero_id, steam_account_id, matches, wins),
                        )
                        continue
                    threshold_row = thresholds_by_hero.get(hero_id)
                    if threshold_row is None:
                        continue
                    threshold_account, threshold_matches, threshold_wins = threshold_row
                    if matches < threshold_matches:
                        continue
                    if matches == threshold_matches and wins <= threshold_wins:
                        continue
                    retryable_execute(
                        cur,
                        """
                        INSERT INTO hero_top100 (heroId, steamAccountId, matches, wins)
                        VALUES (%s,%s,%s,%s)
                        """,
                        (hero_id, steam_account_id, matches, wins),
                    )
                    retryable_execute(
                        cur,
                        """
                        DELETE FROM hero_top100
                        WHERE ctid = (
                            SELECT ctid
                            FROM hero_top100
                            WHERE heroId=%s
                            ORDER BY matches ASC, wins ASC, steamAccountId DESC
                            LIMIT 1
                        )
                        """,
                        (hero_id,),
                    )
    except Exception:
        import traceback

        print(
            f"[submit-background] failed to process hero stats for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_hero_task(steam_account_id)


def process_discover_submission(
    steam_account_id: int,
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    parsed_next_depth = _coerce_optional_int(provided_next_depth)
    parsed_depth = _coerce_optional_int(provided_depth)
    parsed_assignment_depth = _coerce_optional_int(assignment_depth)
    next_depth_value = _resolve_next_depth(
        parsed_next_depth,
        parsed_depth,
        parsed_assignment_depth,
    )
    try:
        with db_connection(write=True) as conn:
            for child_rows in _iter_discovered_child_rows(
                discovered_payload,
                parent_id=steam_account_id,
                next_depth=next_depth_value,
                batch_size=_DISCOVERY_BATCH_SIZE,
            ):
                retryable_executemany(
                    conn,
                    """
                    INSERT INTO players (
                        steamAccountId,
                        depth,
                        hero_done,
                        discover_done,
                        seen_count
                    )
                    VALUES (%s, %s, FALSE, FALSE, %s)
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET
                        depth = LEAST(players.depth, excluded.depth),
                        seen_count = CASE
                            WHEN players.discover_done = FALSE
                                THEN players.seen_count + excluded.seen_count
                            ELSE players.seen_count
                        END
                    WHERE players.discover_done = FALSE
                        OR excluded.depth < players.depth
                    """,
                    child_rows,
                    reacquire_advisory_lock=_DISCOVERY_SUBMISSION_LOCK_ID,
                )
                conn.commit()
            with conn.cursor() as cur:
                retryable_execute(
                    cur,
                    """
                    UPDATE meta
                    SET value = '-1'
                    WHERE key = 'hero_assignment_cursor';
                    """
                )
    except Exception:
        import traceback

        print(
            f"[submit-background] failed to process discovery for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_discover_task(steam_account_id)


def submit_hero_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
) -> None:
    _submit_background(
        process_hero_submission,
        steam_account_id,
        heroes_payload,
    )


def submit_discover_submission(
    steam_account_id: int,
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    _submit_background(
        process_discover_submission,
        steam_account_id,
        discovered_payload,
        provided_next_depth,
        provided_depth,
        assignment_depth,
    )
