"""Validate collected SIP bars and retry recent regular-session gaps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from data_collection.collect_sip_1min import (
    fetch_range,
    load_local_data,
    merge_frames,
    save_local_data,
)
from data_filtering.filter_regular_session import DEFAULT_CALENDAR
from data_validation.audit_regular_session import audit_dataframe


ONE_MINUTE = pd.Timedelta(minutes=1)
REQUIRED_OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class QualityResult:
    success: bool
    summary: dict[str, object]
    missing_intervals: list[dict[str, object]]
    invalid_rows: list[dict[str, object]]
    repaired_rows: int = 0
    changed_from_utc: pd.Timestamp | None = None
    error: str = ""


def invalid_market_rows(
    dataframe: pd.DataFrame,
    symbol: str,
) -> list[dict[str, object]]:
    """Return structurally invalid OHLCV rows without modifying source data."""
    missing_columns = set(REQUIRED_OHLCV_COLUMNS).difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "missing required columns: " + ", ".join(sorted(missing_columns))
        )
    if dataframe.empty:
        return []

    prices = dataframe[["open", "high", "low", "close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    volume = pd.to_numeric(dataframe["volume"], errors="coerce")
    reasons = pd.DataFrame(index=dataframe.index)
    reasons["missing_or_non_numeric_price"] = prices.isna().any(axis=1)
    reasons["non_positive_price"] = prices.le(0).any(axis=1)
    reasons["high_below_ohlc"] = prices["high"] < prices.max(axis=1)
    reasons["low_above_ohlc"] = prices["low"] > prices.min(axis=1)
    reasons["negative_or_invalid_volume"] = volume.isna() | volume.lt(0)

    rows: list[dict[str, object]] = []
    for index, flags in reasons[reasons.any(axis=1)].iterrows():
        timestamp = index
        if isinstance(index, tuple):
            timestamp = index[dataframe.index.names.index("timestamp")]
        rows.append(
            {
                "symbol": symbol,
                "timestamp_utc": pd.Timestamp(timestamp).tz_convert("UTC").isoformat(),
                "reasons": ",".join(flags.index[flags].tolist()),
            }
        )
    return rows


def quality_snapshot(
    dataframe: pd.DataFrame,
    symbol: str,
    calendar_name: str = DEFAULT_CALENDAR,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    """Build coverage and OHLCV-quality metrics for one collected symbol."""
    summary, intervals = audit_dataframe(
        dataframe,
        symbol,
        calendar_name,
        bar_frequency=ONE_MINUTE,
    )
    invalid_rows = invalid_market_rows(dataframe, symbol)
    zero_volume_rows = 0
    if "volume" in dataframe.columns:
        volume = pd.to_numeric(dataframe["volume"], errors="coerce")
        zero_volume_rows = int(volume.eq(0).sum())
    summary["invalid_ohlcv_rows"] = len(invalid_rows)
    summary["zero_volume_rows"] = zero_volume_rows
    summary["status"] = (
        "warning"
        if summary.get("missing_bars", 0)
        or summary.get("duplicate_timestamps", 0)
        or invalid_rows
        else "ok"
    )
    return summary, intervals, invalid_rows


def repair_windows(
    missing_intervals: list[dict[str, object]],
    repair_start: datetime,
) -> list[tuple[datetime, datetime]]:
    """Coalesce missing bars into at most one API request window per session."""
    start_limit = pd.Timestamp(repair_start).tz_convert("UTC")
    by_session: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    for interval in missing_intervals:
        start = pd.Timestamp(str(interval["missing_start_utc"])).tz_convert("UTC")
        end = pd.Timestamp(str(interval["missing_end_utc"])).tz_convert("UTC")
        if end < start_limit:
            continue
        start = max(start, start_limit)
        by_session.setdefault(str(interval["session_date"]), []).append((start, end))

    windows: list[tuple[datetime, datetime]] = []
    for session_intervals in by_session.values():
        window_start = min(value[0] for value in session_intervals)
        window_end = max(value[1] for value in session_intervals) + ONE_MINUTE
        windows.append((window_start.to_pydatetime(), window_end.to_pydatetime()))
    return sorted(windows)


def repair_symbol_file(
    client: object,
    symbol: str,
    file_path: Path,
    storage_format: str,
    data_type: str,
    start_time: datetime,
    end_time: datetime,
    repair_start: datetime,
    chunk_days: int,
    request_delay: float,
    calendar_name: str = DEFAULT_CALENDAR,
) -> QualityResult:
    """Audit one source file, retry recent gaps, and return its final quality state."""
    try:
        dataframe = load_local_data(file_path, storage_format)
        _, before_intervals, _ = quality_snapshot(dataframe, symbol, calendar_name)
        windows = repair_windows(before_intervals, repair_start)
        fetched_frames: list[pd.DataFrame] = []
        for window_start, window_end in windows:
            frames = fetch_range(
                client,
                symbol,
                window_start,
                window_end,
                data_type,
                chunk_days,
                request_delay,
            )
            if frames is None:
                return QualityResult(
                    False,
                    {},
                    before_intervals,
                    [],
                    error=f"gap repair request failed: {window_start} ~ {window_end}",
                )
            fetched_frames.extend(frames)

        repaired_rows = 0
        changed_from: pd.Timestamp | None = None
        if fetched_frames:
            before_index = dataframe.index
            combined = merge_frames(dataframe, fetched_frames, start_time, end_time)
            added_index = combined.index.difference(before_index)
            repaired_rows = len(added_index)
            if repaired_rows:
                first = added_index[0]
                if isinstance(first, tuple):
                    first = first[combined.index.names.index("timestamp")]
                changed_from = pd.Timestamp(first).tz_convert("UTC")
                save_local_data(combined, file_path, storage_format)
                dataframe = combined

        summary, intervals, invalid_rows = quality_snapshot(
            dataframe, symbol, calendar_name
        )
        summary["repair_windows"] = len(windows)
        summary["repaired_rows"] = repaired_rows
        return QualityResult(
            True,
            summary,
            intervals,
            invalid_rows,
            repaired_rows,
            changed_from,
        )
    except (OSError, ValueError, ImportError) as exc:
        return QualityResult(False, {}, [], [], error=str(exc))
