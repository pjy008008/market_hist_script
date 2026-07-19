"""Filter collected intraday bars to official exchange regular sessions."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from pandas.api.types import is_datetime64_any_dtype


DEFAULT_CALENDAR = "XNYS"


@dataclass(frozen=True)
class DataSource:
    dataset: str
    data_type: str
    storage_format: str
    source_dir: Path
    destination_dir: Path


def parse_timestamp_values(values: Iterable[object]) -> pd.DatetimeIndex:
    """Parse ISO timestamps or numeric Unix epochs into UTC timestamps.

    Numeric timestamps are accepted in seconds, milliseconds, microseconds, or
    nanoseconds. Alpaca JSON representations commonly expose timestamps as Unix
    milliseconds even when the original Parquet column has a datetime type.
    """
    value_series = pd.Series(values, copy=False)
    if is_datetime64_any_dtype(value_series.dtype):
        return pd.DatetimeIndex(
            pd.to_datetime(value_series, errors="raise", utc=True)
        )

    non_null = value_series.dropna()
    numeric = pd.to_numeric(value_series, errors="coerce")
    if not non_null.empty and numeric.loc[non_null.index].notna().all():
        magnitude = float(numeric.loc[non_null.index].abs().median())
        if magnitude < 1e11:
            unit = "s"
        elif magnitude < 1e14:
            unit = "ms"
        elif magnitude < 1e17:
            unit = "us"
        else:
            unit = "ns"
        return pd.DatetimeIndex(
            pd.to_datetime(numeric, unit=unit, errors="raise", utc=True)
        )

    return pd.DatetimeIndex(pd.to_datetime(value_series, errors="raise", utc=True))


def normalize_timestamp_index(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a copy whose timestamp index level is normalized to UTC."""
    if "timestamp" not in dataframe.index.names:
        raise ValueError("인덱스에 timestamp 레벨이 없습니다.")

    normalized = dataframe.copy()
    timestamps = parse_timestamp_values(
        normalized.index.get_level_values("timestamp")
    )
    if isinstance(normalized.index, pd.MultiIndex):
        arrays = [
            timestamps if name == "timestamp" else normalized.index.get_level_values(i)
            for i, name in enumerate(normalized.index.names)
        ]
        normalized.index = pd.MultiIndex.from_arrays(
            arrays,
            names=normalized.index.names,
        )
    else:
        normalized.index = timestamps
        normalized.index.name = "timestamp"
    return normalized


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

        dataframe = dataframe.set_index(["symbol", "timestamp"])

    return normalize_timestamp_index(dataframe)


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
    normalized = normalize_timestamp_index(dataframe)
    if normalized.empty:
        return normalized

    timestamps = pd.DatetimeIndex(normalized.index.get_level_values("timestamp"))
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
    if "volume" in normalized.columns:
        volume = pd.to_numeric(normalized["volume"], errors="coerce").reset_index(
            drop=True
        )
        regular_mask &= volume.gt(0)
    return normalized.loc[regular_mask.to_numpy()].copy()


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


def slice_timestamp_range(
    dataframe: pd.DataFrame,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> pd.DataFrame:
    """Keep rows inside a half-open UTC timestamp range."""
    if dataframe.empty:
        return dataframe.copy()
    timestamps = pd.DatetimeIndex(dataframe.index.get_level_values("timestamp"))
    mask = (timestamps >= start_time) & (timestamps < end_time)
    return dataframe.loc[mask].copy()


def session_open_for_timestamp(
    timestamp: pd.Timestamp,
    calendar_name: str = DEFAULT_CALENDAR,
) -> pd.Timestamp:
    """Return the official session open containing a regular-session timestamp."""
    calendar = mcal.get_calendar(calendar_name)
    local_date = pd.Timestamp(timestamp).tz_convert(calendar.tz).date()
    schedule = calendar.schedule(local_date, local_date, tz="UTC")
    if schedule.empty:
        raise ValueError(f"거래 세션을 찾을 수 없습니다: {local_date}")
    return pd.Timestamp(schedule.iloc[0]["market_open"])


def process_file_incrementally(
    input_path: Path,
    output_path: Path,
    storage_format: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    calendar_name: str = DEFAULT_CALENDAR,
    recompute_from: pd.Timestamp | None = None,
) -> tuple[int, int, int]:
    """Filter only the last output session and newly collected source rows.

    The preceding output session is recalculated to make reruns and partially
    completed files safe. Older filtered rows are retained, while rows outside
    the rolling collection window are removed.
    """
    start = pd.Timestamp(start_time).tz_convert("UTC")
    end = pd.Timestamp(end_time).tz_convert("UTC")
    source = slice_timestamp_range(
        load_market_data(input_path, storage_format),
        start,
        end,
    )

    existing = pd.DataFrame()
    if output_path.is_file():
        existing = slice_timestamp_range(
            load_market_data(output_path, storage_format),
            start,
            end,
        )

    if existing.empty:
        retained = existing
        candidate = source
    else:
        last_output = pd.Timestamp(
            existing.index.get_level_values("timestamp").max()
        )
        recompute_start = session_open_for_timestamp(last_output, calendar_name)
        if recompute_from is not None:
            requested_start = session_open_for_timestamp(
                pd.Timestamp(recompute_from), calendar_name
            )
            recompute_start = min(recompute_start, requested_start)
        existing_timestamps = pd.DatetimeIndex(
            existing.index.get_level_values("timestamp")
        )
        source_timestamps = pd.DatetimeIndex(source.index.get_level_values("timestamp"))
        retained = existing.loc[existing_timestamps < recompute_start].copy()
        candidate = source.loc[source_timestamps >= recompute_start].copy()

    newly_filtered = filter_regular_session(candidate, calendar_name)
    frames = [frame for frame in (retained, newly_filtered) if not frame.empty]
    if frames:
        combined = pd.concat(frames)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined = slice_timestamp_range(combined, start, end)
    else:
        combined = source.iloc[0:0].copy()
    save_market_data(combined, output_path, storage_format)
    return len(candidate), len(newly_filtered), len(combined)


def build_sources(
    project_root: Path,
    output_root: Path,
    data_type: str,
    storage_format: str,
    dataset: str = "standard",
) -> list[DataSource]:
    """Build selected source/destination pairs from the project layout."""
    data_types = ("raw", "adjusted") if data_type == "all" else (data_type,)
    formats = ("csv", "parquet") if storage_format == "all" else (storage_format,)
    if dataset == "sip":
        input_roots = {
            "raw": project_root / "sip_market_data" / "raw",
            "adjusted": project_root / "sip_market_data" / "adjusted",
        }
    else:
        input_roots = {
            "raw": project_root / "market_data",
            "adjusted": project_root / "adjust_market_data",
        }

    return [
        DataSource(
            dataset=dataset,
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
            f"[{index}/{len(input_files)}] {source.dataset}/"
            f"{source.data_type}/{source.storage_format} "
            f"{input_path.name}: {len(dataframe):,} -> {len(filtered):,}행"
        )

    return processed_files, total_rows, kept_rows


def choose_dataset() -> str:
    """Prompt for the collected dataset to filter."""
    choices = {
        "": "standard",
        "1": "standard",
        "standard": "standard",
        "5min": "standard",
        "2": "sip",
        "sip": "sip",
        "1min": "sip",
    }

    print("=" * 54)
    print(" [선택 1] 필터링할 원본 데이터셋을 선택해 주세요.")
    print("  1. 기존 수집 데이터/5분봉 (기본값)")
    print("  2. SIP 1분봉 데이터")
    print("=" * 54)
    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""
        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(기존 데이터) 또는 2(SIP 1분봉)를 입력해 주세요.")


def default_output_root(project_root: Path, dataset: str) -> Path:
    """Return a separate output root so different feeds never mix."""
    folder_name = "regular_sip_market_data" if dataset == "sip" else "regular_market_data"
    return project_root / folder_name


def choose_data_type(step_number: int = 1) -> str:
    """Prompt for the source price type when no CLI option was supplied."""
    choices = {
        "": "raw",
        "1": "raw",
        "raw": "raw",
        "2": "adjusted",
        "adjusted": "adjusted",
        "adjust": "adjusted",
    }

    print("=" * 50)
    print(f" [선택 {step_number}] 처리할 데이터 타입을 선택해 주세요.")
    print("  1. Raw 데이터 (기본값)")
    print("  2. Adjusted 데이터 (배당/분할 수정주가 반영)")
    print("=" * 50)

    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""

        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(Raw) 또는 2(Adjusted)를 입력해 주세요.")


def choose_storage_format(step_number: int = 2) -> str:
    """Prompt for the file format when no CLI option was supplied."""
    choices = {
        "": "csv",
        "1": "csv",
        "csv": "csv",
        ".csv": "csv",
        "2": "parquet",
        "parquet": "parquet",
        ".parquet": "parquet",
    }

    print("\n" + "=" * 50)
    print(f" [선택 {step_number}] 처리할 파일 형식을 선택해 주세요.")
    print("  1. CSV (기본값)")
    print("  2. Parquet")
    print("=" * 50)

    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""

        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(CSV) 또는 2(Parquet)를 입력해 주세요.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "거래소 공식 일정에 따라 미국 주식 정규장 봉만 별도 폴더에 저장합니다."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=("standard", "sip"),
        help="원본 데이터셋 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--data-type",
        choices=("raw", "adjusted", "all"),
        help="처리할 데이터 타입 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet", "all"),
        help="처리할 파일 형식 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--calendar",
        default=DEFAULT_CALENDAR,
        help=f"pandas-market-calendars 캘린더 이름 (기본값: {DEFAULT_CALENDAR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="결과 루트 폴더 (미지정 시 데이터셋별 기본 폴더)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    dataset = args.dataset or choose_dataset()
    output_root = (
        args.output_dir.expanduser().resolve()
        if args.output_dir
        else default_output_root(project_root, dataset)
    )
    data_type = args.data_type or choose_data_type(step_number=2)
    storage_format = args.storage_format or choose_storage_format(step_number=3)

    try:
        mcal.get_calendar(args.calendar)
    except RuntimeError as exc:
        print(f"[오류] 지원하지 않는 캘린더입니다: {args.calendar}\n{exc}", file=sys.stderr)
        return 2

    print(f"캘린더: {args.calendar}")
    print(f"원본 데이터셋: {dataset}")
    print(f"데이터 타입: {data_type}")
    print(f"파일 형식: {storage_format}")
    print(f"결과 폴더: {output_root}")

    totals = [0, 0, 0]
    try:
        sources = build_sources(
            project_root,
            output_root,
            data_type,
            storage_format,
            dataset,
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
