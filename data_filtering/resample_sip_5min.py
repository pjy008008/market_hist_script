"""Build regular-session SIP five-minute bars from filtered one-minute bars."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    load_market_data,
    normalize_timestamp_index,
    save_market_data,
    session_open_for_timestamp,
    slice_timestamp_range,
)


SOURCE_ROOT = PROJECT_ROOT / "regular_sip_market_data"
DESTINATION_ROOT = PROJECT_ROOT / "regular_sip_5min_market_data"
DATA_TYPE = "adjusted"
FIVE_MINUTES = "5min"
REQUIRED_COLUMNS = {"open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class ResampleSource:
    storage_format: str
    source_dir: Path
    destination_dir: Path


def _resample_session(session: pd.DataFrame) -> pd.DataFrame:
    """Aggregate one symbol and one regular session into five-minute bars."""
    rule = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "trade_count" in session.columns:
        rule["trade_count"] = "sum"

    resampler = session.resample(
        FIVE_MINUTES,
        closed="left",
        label="left",
        origin="start_day",
    )
    aggregated = resampler.agg(rule)
    source_minutes = resampler["close"].count()
    aggregated = aggregated.loc[source_minutes > 0].copy()
    aggregated["source_minutes"] = source_minutes.loc[aggregated.index].astype("int64")

    if "vwap" in session.columns:
        valid_vwap = session["vwap"].notna() & session["volume"].notna()
        weighted_value = (
            (session["vwap"] * session["volume"])
            .where(valid_vwap)
            .resample(
                FIVE_MINUTES,
                closed="left",
                label="left",
                origin="start_day",
            )
            .sum(min_count=1)
        )
        vwap_volume = (
            session["volume"]
            .where(valid_vwap)
            .resample(
                FIVE_MINUTES,
                closed="left",
                label="left",
                origin="start_day",
            )
            .sum(min_count=1)
        )
        aggregated["vwap"] = (
            weighted_value.loc[aggregated.index]
            / vwap_volume.loc[aggregated.index].replace(0, pd.NA)
        )

    column_order = [
        column
        for column in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
            "source_minutes",
        )
        if column in aggregated.columns
    ]
    return aggregated[column_order]


def resample_sip_five_minutes(
    dataframe: pd.DataFrame,
    calendar_name: str = DEFAULT_CALENDAR,
) -> pd.DataFrame:
    """Aggregate filtered SIP one-minute bars by symbol and exchange session."""
    normalized = normalize_timestamp_index(dataframe)
    missing_columns = REQUIRED_COLUMNS.difference(normalized.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"5분봉 생성에 필요한 컬럼이 없습니다: {missing}")
    if normalized.empty:
        return normalized.copy()

    calendar = mcal.get_calendar(calendar_name)
    frames: list[pd.DataFrame] = []
    symbols = normalized.index.get_level_values("symbol").unique()
    for symbol in symbols:
        symbol_frame = normalized.xs(symbol, level="symbol").sort_index()
        local_session_dates = symbol_frame.index.tz_convert(calendar.tz).normalize()
        for session_date in local_session_dates.unique():
            session = symbol_frame.loc[local_session_dates == session_date]
            aggregated = _resample_session(session)
            if not aggregated.empty:
                aggregated["symbol"] = symbol
                frames.append(aggregated.reset_index().set_index(["symbol", "timestamp"]))

    if not frames:
        return normalized.iloc[0:0].copy()
    return pd.concat(frames).sort_index()


def build_resample_source(
    storage_format: str,
    source_root: Path = SOURCE_ROOT,
    destination_root: Path = DESTINATION_ROOT,
) -> ResampleSource:
    return ResampleSource(
        storage_format=storage_format,
        source_dir=source_root / DATA_TYPE / storage_format,
        destination_dir=destination_root / DATA_TYPE / storage_format,
    )


def output_file_name(input_path: Path) -> str:
    return input_path.name.replace("_1min_sip_historical", "_5min_sip_historical")


def process_resample_file_incrementally(
    input_path: Path,
    output_path: Path,
    storage_format: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    calendar_name: str = DEFAULT_CALENDAR,
    recompute_from: pd.Timestamp | None = None,
) -> tuple[int, int, int]:
    """Rebuild the last output session and append newly filtered sessions."""
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

    new_bars = resample_sip_five_minutes(candidate, calendar_name)
    frames = [frame for frame in (retained, new_bars) if not frame.empty]
    if frames:
        combined = pd.concat(frames)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined = slice_timestamp_range(combined, start, end)
    else:
        combined = source.iloc[0:0].copy()
    save_market_data(combined, output_path, storage_format)
    return len(candidate), len(new_bars), len(combined)


def process_resample_source(
    source: ResampleSource,
    calendar_name: str = DEFAULT_CALENDAR,
) -> tuple[int, int, int]:
    """Resample every one-minute file in a selected source directory."""
    if not source.source_dir.is_dir():
        print(f"[건너뜀] 입력 폴더 없음: {source.source_dir}")
        return 0, 0, 0

    input_files = sorted(source.source_dir.glob(f"*.{source.storage_format}"))
    if not input_files:
        print(f"[건너뜀] 입력 파일 없음: {source.source_dir}")
        return 0, 0, 0

    total_rows = 0
    resampled_rows = 0
    for index, input_path in enumerate(input_files, 1):
        dataframe = load_market_data(input_path, source.storage_format)
        resampled = resample_sip_five_minutes(dataframe, calendar_name)
        output_path = source.destination_dir / output_file_name(input_path)
        save_market_data(resampled, output_path, source.storage_format)
        total_rows += len(dataframe)
        resampled_rows += len(resampled)
        print(
            f"[{index}/{len(input_files)}] {input_path.name}: "
            f"{len(dataframe):,}개 1분봉 -> {len(resampled):,}개 5분봉"
        )
    return len(input_files), total_rows, resampled_rows


def choose_storage_format() -> str:
    choices = {
        "": "csv",
        "1": "csv",
        "csv": "csv",
        "2": "parquet",
        "parquet": "parquet",
    }
    print("1. CSV (기본값)\n2. Parquet")
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
        description="정규장 SIP Adjusted 1분봉을 5분봉으로 집계합니다."
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet"),
        help="입출력 형식 (미지정 시 실행 중 선택)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage_format = args.storage_format or choose_storage_format()
    source = build_resample_source(storage_format)
    try:
        processed_files, source_rows, output_rows = process_resample_source(source)
    except (OSError, ValueError, ImportError) as exc:
        print(f"[오류] 5분봉 생성 실패: {exc}", file=sys.stderr)
        return 1
    if processed_files == 0:
        return 1
    print(
        f"완료: {processed_files}개 파일, "
        f"{source_rows:,}개 1분봉 -> {output_rows:,}개 5분봉"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
