"""Filter collected intraday bars to official exchange regular sessions."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal


DEFAULT_CALENDAR = "XNYS"


@dataclass(frozen=True)
class DataSource:
    data_type: str
    storage_format: str
    source_dir: Path
    destination_dir: Path


def load_market_data(file_path: Path, storage_format: str) -> pd.DataFrame:
    """Load a collected file while restoring the common MultiIndex for CSV."""
    if storage_format == "parquet":
        dataframe = pd.read_parquet(file_path)
    else:
        dataframe = pd.read_csv(file_path)
        required_columns = {"symbol", "timestamp"}
        missing_columns = required_columns.difference(dataframe.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"필수 CSV 컬럼이 없습니다: {missing}")

        dataframe["timestamp"] = pd.to_datetime(
            dataframe["timestamp"], errors="raise", utc=True
        )
        dataframe = dataframe.set_index(["symbol", "timestamp"])

    if "timestamp" not in dataframe.index.names:
        raise ValueError("인덱스에 timestamp 레벨이 없습니다.")

    return dataframe


def filter_regular_session(
    dataframe: pd.DataFrame,
    calendar_name: str = DEFAULT_CALENDAR,
) -> pd.DataFrame:
    """Keep bars whose start time is inside an official regular session.

    The exchange schedule supplies UTC-aware opens and closes, so holidays,
    special closes, and daylight-saving transitions are handled by the calendar.
    A bar at the exact close timestamp is excluded because its interval starts
    after the regular session has ended.
    """
    if dataframe.empty:
        return dataframe.copy()

    timestamps = pd.DatetimeIndex(
        pd.to_datetime(
            dataframe.index.get_level_values("timestamp"),
            errors="raise",
            utc=True,
        )
    )
    calendar = mcal.get_calendar(calendar_name)

    local_dates = (
        timestamps.tz_convert(calendar.tz)
        .tz_localize(None)
        .normalize()
    )
    schedule = calendar.schedule(
        start_date=local_dates.min(),
        end_date=local_dates.max(),
        tz="UTC",
    )

    session_dates = pd.Series(local_dates)
    market_opens = session_dates.map(schedule["market_open"])
    market_closes = session_dates.map(schedule["market_close"])
    bar_times = pd.Series(timestamps)

    regular_mask = (
        market_opens.notna()
        & market_closes.notna()
        & bar_times.ge(market_opens)
        & bar_times.lt(market_closes)
    )
    return dataframe.loc[regular_mask.to_numpy()].copy()


def save_market_data(
    dataframe: pd.DataFrame,
    file_path: Path,
    storage_format: str,
) -> None:
    """Atomically replace an output file after a successful write."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = file_path.with_name(f".{file_path.name}.tmp")

    try:
        if storage_format == "parquet":
            dataframe.to_parquet(temporary_path)
        else:
            dataframe.to_csv(temporary_path, index=True)
        os.replace(temporary_path, file_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def build_sources(
    project_root: Path,
    output_root: Path,
    data_type: str,
    storage_format: str,
) -> list[DataSource]:
    """Build selected source/destination pairs from the project layout."""
    data_types = ("raw", "adjusted") if data_type == "all" else (data_type,)
    formats = ("csv", "parquet") if storage_format == "all" else (storage_format,)
    input_roots = {
        "raw": project_root / "market_data",
        "adjusted": project_root / "adjust_market_data",
    }

    return [
        DataSource(
            data_type=selected_type,
            storage_format=selected_format,
            source_dir=input_roots[selected_type] / selected_format,
            destination_dir=output_root / selected_type / selected_format,
        )
        for selected_type in data_types
        for selected_format in formats
    ]


def process_source(source: DataSource, calendar_name: str) -> tuple[int, int, int]:
    """Filter every market-data file in one source directory."""
    if not source.source_dir.is_dir():
        print(f"[건너뜀] 입력 폴더 없음: {source.source_dir}")
        return 0, 0, 0

    input_files = sorted(source.source_dir.glob(f"*.{source.storage_format}"))
    if not input_files:
        print(f"[건너뜀] 입력 파일 없음: {source.source_dir}")
        return 0, 0, 0

    total_rows = 0
    kept_rows = 0
    processed_files = 0
    for index, input_path in enumerate(input_files, 1):
        output_path = source.destination_dir / input_path.name
        dataframe = load_market_data(input_path, source.storage_format)
        filtered = filter_regular_session(dataframe, calendar_name)
        save_market_data(filtered, output_path, source.storage_format)

        processed_files += 1
        total_rows += len(dataframe)
        kept_rows += len(filtered)
        print(
            f"[{index}/{len(input_files)}] {source.data_type}/{source.storage_format} "
            f"{input_path.name}: {len(dataframe):,} -> {len(filtered):,}행"
        )

    return processed_files, total_rows, kept_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    project_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description=(
            "거래소 공식 일정에 따라 미국 주식 정규장 봉만 별도 폴더에 저장합니다."
        )
    )
    parser.add_argument(
        "--data-type",
        choices=("raw", "adjusted", "all"),
        default="all",
        help="처리할 데이터 타입 (기본값: all)",
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet", "all"),
        default="all",
        help="처리할 파일 형식 (기본값: all)",
    )
    parser.add_argument(
        "--calendar",
        default=DEFAULT_CALENDAR,
        help=f"pandas-market-calendars 캘린더 이름 (기본값: {DEFAULT_CALENDAR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "regular_market_data",
        help="결과 루트 폴더 (기본값: 프로젝트의 regular_market_data)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parent
    output_root = args.output_dir.expanduser().resolve()

    try:
        mcal.get_calendar(args.calendar)
    except RuntimeError as exc:
        print(f"[오류] 지원하지 않는 캘린더입니다: {args.calendar}\n{exc}", file=sys.stderr)
        return 2

    print(f"캘린더: {args.calendar}")
    print(f"결과 폴더: {output_root}")

    totals = [0, 0, 0]
    try:
        sources = build_sources(
            project_root,
            output_root,
            args.data_type,
            args.storage_format,
        )
        for source in sources:
            source_totals = process_source(source, args.calendar)
            totals = [left + right for left, right in zip(totals, source_totals)]
    except (OSError, ValueError, ImportError) as exc:
        print(f"[오류] 처리 중단: {exc}", file=sys.stderr)
        return 1

    processed_files, total_rows, kept_rows = totals
    print("=" * 60)
    print(f"완료: {processed_files}개 파일, {total_rows:,}행 -> {kept_rows:,}행")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
