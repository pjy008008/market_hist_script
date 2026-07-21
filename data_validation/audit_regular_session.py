"""Audit filtered regular-session files for date coverage and missing bars."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    choose_data_type,
    choose_storage_format,
    load_market_data,
)


BAR_FREQUENCY = pd.Timedelta(minutes=5)
MISSING_INTERVAL_COLUMNS = [
    "symbol",
    "session_date",
    "missing_start_utc",
    "missing_end_utc",
    "missing_bars",
    "missing_minutes",
    "previous_bar_utc",
    "next_bar_utc",
]


def expected_regular_timestamps(
    first_timestamp: pd.Timestamp,
    last_timestamp: pd.Timestamp,
    calendar_name: str = DEFAULT_CALENDAR,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
) -> pd.DatetimeIndex:
    """Return expected bar starts within the observed boundaries."""
    calendar = mcal.get_calendar(calendar_name)
    first_timestamp = pd.Timestamp(first_timestamp).tz_convert("UTC")
    last_timestamp = pd.Timestamp(last_timestamp).tz_convert("UTC")
    local_dates = pd.DatetimeIndex([first_timestamp, last_timestamp]).tz_convert(
        calendar.tz
    )
    schedule = calendar.schedule(
        start_date=local_dates.min().date(),
        end_date=local_dates.max().date(),
        tz="UTC",
    )

    expected_values = [
        timestamp
        for market_open, market_close in schedule[
            ["market_open", "market_close"]
        ].itertuples(index=False, name=None)
        for timestamp in pd.date_range(
            market_open,
            market_close,
            freq=bar_frequency,
            inclusive="left",
        )
    ]
    if not expected_values:
        return pd.DatetimeIndex([], tz="UTC")

    expected = pd.DatetimeIndex(expected_values)
    return expected[(expected >= first_timestamp) & (expected <= last_timestamp)]


def build_missing_intervals(
    missing: pd.DatetimeIndex,
    expected: pd.DatetimeIndex,
    observed: pd.DatetimeIndex,
    symbol: str,
    calendar_name: str,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
) -> list[dict[str, object]]:
    """Group adjacent missing timestamps without joining separate sessions."""
    if missing.empty:
        return []

    calendar = mcal.get_calendar(calendar_name)
    missing_local_dates = missing.tz_convert(calendar.tz).normalize()
    intervals: list[tuple[int, int]] = []
    interval_start = 0

    for position in range(1, len(missing)):
        same_session = missing_local_dates[position] == missing_local_dates[position - 1]
        consecutive = missing[position] - missing[position - 1] == bar_frequency
        if not (same_session and consecutive):
            intervals.append((interval_start, position - 1))
            interval_start = position
    intervals.append((interval_start, len(missing) - 1))

    expected_set = set(expected)
    observed_set = set(observed)
    rows: list[dict[str, object]] = []
    for start_position, end_position in intervals:
        missing_start = missing[start_position]
        missing_end = missing[end_position]
        previous_timestamp = missing_start - bar_frequency
        next_timestamp = missing_end + bar_frequency
        missing_bars = end_position - start_position + 1
        missing_minutes = int(
            missing_bars * bar_frequency / pd.Timedelta(minutes=1)
        )

        rows.append(
            {
                "symbol": symbol,
                "session_date": missing_start.tz_convert(calendar.tz).date().isoformat(),
                "missing_start_utc": missing_start.isoformat(),
                "missing_end_utc": missing_end.isoformat(),
                "missing_bars": missing_bars,
                "missing_minutes": missing_minutes,
                "previous_bar_utc": (
                    previous_timestamp.isoformat()
                    if previous_timestamp in expected_set
                    and previous_timestamp in observed_set
                    else ""
                ),
                "next_bar_utc": (
                    next_timestamp.isoformat()
                    if next_timestamp in expected_set and next_timestamp in observed_set
                    else ""
                ),
            }
        )
    return rows


def count_missing_intervals(
    missing: pd.DatetimeIndex,
    calendar_name: str,
    bar_frequency: pd.Timedelta,
) -> int:
    """Count contiguous gaps without materializing the detailed report rows."""
    if missing.empty:
        return 0

    calendar = mcal.get_calendar(calendar_name)
    local_dates = missing.tz_convert(calendar.tz).normalize()
    breaks = (local_dates[1:] != local_dates[:-1]) | (
        missing[1:] - missing[:-1] != bar_frequency
    )
    return 1 + int(breaks.sum())


def audit_dataframe(
    dataframe: pd.DataFrame,
    symbol: str,
    calendar_name: str = DEFAULT_CALENDAR,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
    include_intervals: bool = True,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Calculate observed coverage and missing regular-session intervals."""
    if dataframe.empty:
        return (
            {
                "symbol": symbol,
                "status": "empty",
                "first_timestamp_utc": "",
                "last_timestamp_utc": "",
                "first_session_date": "",
                "last_session_date": "",
                "observed_rows": 0,
                "unique_timestamps": 0,
                "expected_bars": 0,
                "missing_bars": 0,
                "coverage_pct": 0.0,
                "missing_intervals": 0,
                "duplicate_timestamps": 0,
                "unexpected_timestamps": 0,
            },
            [],
        )

    timestamps = pd.DatetimeIndex(
        pd.to_datetime(
            dataframe.index.get_level_values("timestamp"),
            errors="raise",
            utc=True,
        )
    )
    observed = timestamps.unique().sort_values()
    expected = expected_regular_timestamps(
        observed.min(), observed.max(), calendar_name, bar_frequency
    )
    missing = expected.difference(observed)
    unexpected = observed.difference(expected)
    missing_interval_count = count_missing_intervals(
        missing, calendar_name, bar_frequency
    )
    intervals = (
        build_missing_intervals(
            missing, expected, observed, symbol, calendar_name, bar_frequency
        )
        if include_intervals
        else []
    )
    covered_bars = len(expected.intersection(observed))
    coverage_pct = covered_bars / len(expected) * 100 if len(expected) else 0.0
    calendar = mcal.get_calendar(calendar_name)
    observed_local = observed.tz_convert(calendar.tz)

    summary = {
        "symbol": symbol,
        "status": "ok",
        "first_timestamp_utc": observed.min().isoformat(),
        "last_timestamp_utc": observed.max().isoformat(),
        "first_session_date": observed_local.min().date().isoformat(),
        "last_session_date": observed_local.max().date().isoformat(),
        "observed_rows": len(dataframe),
        "unique_timestamps": len(observed),
        "expected_bars": len(expected),
        "missing_bars": len(missing),
        "coverage_pct": round(coverage_pct, 6),
        "missing_intervals": missing_interval_count,
        "duplicate_timestamps": len(timestamps) - len(observed),
        "unexpected_timestamps": len(unexpected),
    }
    return summary, intervals


def save_report(dataframe: pd.DataFrame, destination: Path) -> None:
    """Atomically save a report CSV."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f".{destination.name}.tmp")
    try:
        dataframe.to_csv(temporary_path, index=False, encoding="utf-8-sig")
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def selected_sources(
    project_root: Path,
    data_type: str,
    storage_format: str,
    dataset: str = "standard",
) -> list[tuple[str, str, Path]]:
    data_types = ("raw", "adjusted") if data_type == "all" else (data_type,)
    formats = ("csv", "parquet") if storage_format == "all" else (storage_format,)
    root_name = (
        "regular_sip_1min_market_data"
        if dataset == "sip"
        else "regular_market_data"
    )
    roots = {
        "raw": project_root / root_name / "raw",
        "adjusted": project_root / root_name / "adjusted",
    }
    return [
        (selected_type, selected_format, roots[selected_type] / selected_format)
        for selected_type in data_types
        for selected_format in formats
    ]


def audit_source(
    source_dir: Path,
    storage_format: str,
    calendar_name: str,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
    include_intervals: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    input_files = sorted(source_dir.glob(f"*.{storage_format}"))
    summaries: list[dict[str, object]] = []
    intervals: list[dict[str, object]] = []

    for index, input_path in enumerate(input_files, 1):
        symbol = input_path.name
        sip_suffix = f"_sip_historical.{storage_format}"
        if symbol.endswith(sip_suffix):
            symbol_with_interval = symbol[: -len(sip_suffix)]
            symbol = symbol_with_interval.rsplit("_", 1)[0]
        else:
            legacy_suffix = f"_5min_historical.{storage_format}"
            if symbol.endswith(legacy_suffix):
                symbol = symbol[: -len(legacy_suffix)]
        try:
            dataframe = load_market_data(input_path, storage_format)
            summary, missing_intervals = audit_dataframe(
                dataframe,
                symbol,
                calendar_name,
                bar_frequency,
                include_intervals,
            )
            summary["file"] = input_path.name
            summaries.append(summary)
            intervals.extend(missing_intervals)
            print(
                f"[{index}/{len(input_files)}] {symbol}: "
                f"{summary['first_timestamp_utc']} ~ {summary['last_timestamp_utc']}, "
                f"누락 {summary['missing_bars']:,}개 "
                f"(커버리지 {summary['coverage_pct']:.4f}%)"
            )
        except (OSError, ValueError, ImportError) as exc:
            summaries.append(
                {
                    "symbol": symbol,
                    "status": f"error: {exc}",
                    "file": input_path.name,
                }
            )
            print(f"[{index}/{len(input_files)}] {symbol}: 오류 - {exc}")

    return pd.DataFrame(summaries), pd.DataFrame(
        intervals, columns=MISSING_INTERVAL_COLUMNS
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="정규장 데이터의 종목별 기간과 누락된 봉 구간을 검사합니다."
    )
    parser.add_argument(
        "--dataset",
        choices=("standard", "sip"),
        default="standard",
        help="검사할 데이터셋 (기본값: standard 5분봉)",
    )
    parser.add_argument(
        "--data-type",
        choices=("raw", "adjusted", "all"),
        help="검사할 데이터 타입 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet", "all"),
        help="검사할 파일 형식 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--calendar",
        default=DEFAULT_CALENDAR,
        help=f"거래소 캘린더 이름 (기본값: {DEFAULT_CALENDAR})",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="보고서 저장 폴더 (미지정 시 데이터셋별 기본 폴더)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_type = args.data_type or choose_data_type()
    storage_format = args.storage_format or choose_storage_format()
    report_dir = (
        args.report_dir.expanduser().resolve()
        if args.report_dir
        else PROJECT_ROOT
        / "report"
        / (
            "regular_sip_session_audit"
            if args.dataset == "sip"
            else "regular_session_audit"
        )
    )
    bar_frequency = pd.Timedelta(minutes=1 if args.dataset == "sip" else 5)

    try:
        mcal.get_calendar(args.calendar)
    except RuntimeError as exc:
        print(f"[오류] 지원하지 않는 캘린더입니다: {args.calendar}\n{exc}", file=sys.stderr)
        return 2
    processed_files = 0
    for selected_type, selected_format, source_dir in selected_sources(
        PROJECT_ROOT, data_type, storage_format, args.dataset
    ):
        if not source_dir.is_dir():
            print(f"[건너뜀] 입력 폴더 없음: {source_dir}")
            continue

        summary, intervals = audit_source(
            source_dir, selected_format, args.calendar, bar_frequency
        )
        if summary.empty:
            print(f"[건너뜀] 입력 파일 없음: {source_dir}")
            continue

        prefix = f"{selected_type}_{selected_format}"
        summary_path = report_dir / f"{prefix}_summary.csv"
        intervals_path = report_dir / f"{prefix}_missing_intervals.csv"
        save_report(summary, summary_path)
        save_report(intervals, intervals_path)
        processed_files += len(summary)
        print(f"요약 보고서: {summary_path}")
        print(f"누락 구간 보고서: {intervals_path}")

    if processed_files == 0:
        print("[오류] 검사할 정규장 데이터 파일이 없습니다.", file=sys.stderr)
        return 1

    print(f"완료: 총 {processed_files}개 파일 검사")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
