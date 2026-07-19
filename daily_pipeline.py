"""Run the daily SIP one-minute adjusted-data pipeline end to end."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from alpaca.data.historical import StockHistoricalDataClient
from dotenv import load_dotenv

from data_collection.collect_sip_1min import (
    DEFAULT_CHUNK_DAYS,
    DEFAULT_REQUEST_DELAY_SECONDS,
    END_DELAY_MINUTES,
    YEARS_TO_COLLECT,
    process_symbol,
)
from data_collection.get_ticker import get_historical_sp500_tickers
from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    build_sources,
    process_source,
)
from data_validation.audit_regular_session import audit_source, save_report


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_TYPE = "adjusted"
DATASET = "sip"
BAR_FREQUENCY = pd.Timedelta(minutes=1)
MIN_EXPECTED_TICKERS = 400
TICKER_FILE = PROJECT_ROOT / "ticker_info" / "sp500_tickers_3years.txt"
COLLECTION_ROOT = PROJECT_ROOT / "sip_market_data"
FILTERED_ROOT = PROJECT_ROOT / "regular_sip_market_data"
REPORT_ROOT = PROJECT_ROOT / "report" / "regular_sip_session_audit"


def choose_storage_format() -> str:
    """Prompt for the pipeline's only user-selectable setting."""
    choices = {
        "": "csv",
        "1": "csv",
        "csv": "csv",
        ".csv": "csv",
        "2": "parquet",
        "parquet": "parquet",
        ".parquet": "parquet",
    }
    print("=" * 54)
    print(" 저장 형식을 선택해 주세요.")
    print("  1. CSV (기본값)")
    print("  2. Parquet")
    print("=" * 54)
    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""
        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(CSV) 또는 2(Parquet)를 입력해 주세요.")


def completed_collection_window(
    now: datetime | None = None,
    calendar_name: str = DEFAULT_CALENDAR,
) -> tuple[datetime, datetime]:
    """Return a rolling three-year window ending at the last completed session.

    A session is considered complete only after the configured market-data delay,
    so a run before today's data is available safely falls back to the preceding
    trading session. Exchange holidays, early closes, and DST come from XNYS.
    """
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    available_until = pd.Timestamp(current - timedelta(minutes=END_DELAY_MINUTES))

    calendar = mcal.get_calendar(calendar_name)
    local_date = available_until.tz_convert(calendar.tz).date()
    schedule = calendar.schedule(
        start_date=local_date - timedelta(days=45),
        end_date=local_date,
        tz="UTC",
    )
    completed = schedule[schedule["market_close"] <= available_until]
    if completed.empty:
        raise RuntimeError("최근 완료된 거래 세션을 찾지 못했습니다.")

    end_time = pd.Timestamp(completed.iloc[-1]["market_close"]).to_pydatetime()
    start_time = (
        pd.Timestamp(end_time) - pd.DateOffset(years=YEARS_TO_COLLECT)
    ).to_pydatetime()
    return start_time, end_time


def read_ticker_file(file_path: Path = TICKER_FILE) -> list[str]:
    if not file_path.is_file():
        return []
    return sorted(
        {
            line.strip().upper()
            for line in file_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    )


def save_ticker_file(symbols: list[str], file_path: Path = TICKER_FILE) -> None:
    """Atomically save the refreshed ticker universe."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = file_path.with_name(f".{file_path.name}.tmp")
    try:
        temporary_path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
        os.replace(temporary_path, file_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def load_pipeline_symbols(
    file_path: Path = TICKER_FILE,
    minimum_count: int = MIN_EXPECTED_TICKERS,
) -> list[str]:
    """Refresh tickers, preserving a valid cached universe on crawl failure."""
    fetched = get_historical_sp500_tickers(years=YEARS_TO_COLLECT)
    if len(fetched) >= minimum_count:
        symbols = sorted(set(fetched))
        save_ticker_file(symbols, file_path)
        return symbols

    cached = read_ticker_file(file_path)
    if len(cached) >= minimum_count:
        print(
            f"[경고] 티커 갱신 결과가 {len(fetched)}개뿐이어서 "
            f"기존 {len(cached)}개 목록을 사용합니다."
        )
        return cached
    raise RuntimeError(
        "S&P 500 티커 목록을 정상적으로 가져오지 못했고 사용할 기존 목록도 없습니다."
    )


def run_collection(
    client: StockHistoricalDataClient,
    symbols: list[str],
    storage_format: str,
    start_time: datetime,
    end_time: datetime,
) -> list[str]:
    failed_symbols: list[str] = []
    for index, symbol in enumerate(symbols, 1):
        print(f"\n[{index}/{len(symbols)}] {symbol} 수집 또는 갱신")
        succeeded = process_symbol(
            client,
            symbol,
            DATA_TYPE,
            storage_format,
            COLLECTION_ROOT,
            start_time,
            end_time,
            DEFAULT_CHUNK_DAYS,
            DEFAULT_REQUEST_DELAY_SECONDS,
        )
        if not succeeded:
            failed_symbols.append(symbol)
    return failed_symbols


def run_filter(storage_format: str) -> tuple[int, int, int]:
    source = build_sources(
        PROJECT_ROOT,
        FILTERED_ROOT,
        DATA_TYPE,
        storage_format,
        DATASET,
    )[0]
    return process_source(source, DEFAULT_CALENDAR)


def run_validation(storage_format: str) -> tuple[int, int]:
    source_dir = FILTERED_ROOT / DATA_TYPE / storage_format
    summary, intervals = audit_source(
        source_dir,
        storage_format,
        DEFAULT_CALENDAR,
        BAR_FREQUENCY,
    )
    if summary.empty:
        raise RuntimeError("검사할 SIP 정규장 데이터 파일이 없습니다.")

    prefix = f"{DATA_TYPE}_{storage_format}"
    summary_path = REPORT_ROOT / f"{prefix}_summary.csv"
    intervals_path = REPORT_ROOT / f"{prefix}_missing_intervals.csv"
    save_report(summary, summary_path)
    save_report(intervals, intervals_path)
    error_count = int(summary["status"].astype(str).str.startswith("error:").sum())
    print(f"요약 보고서: {summary_path}")
    print(f"누락 구간 보고서: {intervals_path}")
    return len(summary), error_count


def run_pipeline(storage_format: str, now: datetime | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        print("[오류] Alpaca API 키와 Secret 키를 설정해 주세요.", file=sys.stderr)
        return 1

    try:
        start_time, end_time = completed_collection_window(now)
        print("Wikipedia에서 최근 3년 S&P 500 관련 티커를 갱신합니다...")
        symbols = load_pipeline_symbols()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[오류] 실행 준비 실패: {exc}", file=sys.stderr)
        return 1

    print("=" * 72)
    print("일일 통합 파이프라인")
    print("피드/간격   : SIP / 1분")
    print("가격 조정   : Adjusted")
    print(f"저장 형식   : {storage_format}")
    print(f"수집 기간   : {start_time.isoformat()} ~ {end_time.isoformat()}")
    print(f"마지막 세션 : {end_time.astimezone(timezone.utc).isoformat()}")
    print(f"대상 종목   : {len(symbols)}개")
    print("=" * 72)

    client = StockHistoricalDataClient(api_key, secret_key)
    print("\n[1/3] SIP Adjusted 1분봉 수집·증분 갱신")
    failed_symbols = run_collection(
        client,
        symbols,
        storage_format,
        start_time,
        end_time,
    )

    print("\n[2/3] XNYS 정규장 필터링")
    try:
        processed_files, total_rows, kept_rows = run_filter(storage_format)
        if processed_files == 0:
            raise RuntimeError("필터링할 수집 파일이 없습니다.")

        print("\n[3/3] SIP 1분봉 기간·누락 구간 검사")
        audited_files, audit_errors = run_validation(storage_format)
    except (OSError, RuntimeError, ValueError, ImportError) as exc:
        print(f"[오류] 후처리 실패: {exc}", file=sys.stderr)
        return 1

    print("=" * 72)
    print(
        f"완료: 수집 성공 {len(symbols) - len(failed_symbols)}/{len(symbols)}개, "
        f"필터 {processed_files}개 파일 ({total_rows:,} -> {kept_rows:,}행), "
        f"검사 {audited_files}개 파일"
    )
    if failed_symbols:
        print("수집 실패 종목: " + ", ".join(failed_symbols))
    if audit_errors:
        print(f"검사 오류 파일: {audit_errors}개")
    return 1 if failed_symbols or audit_errors else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SIP Adjusted 1분봉을 갱신하고 정규장 필터링과 데이터 검사를 수행합니다."
        )
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet"),
        help="저장 형식 (미지정 시 실행 중 선택)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage_format = args.storage_format or choose_storage_format()
    return run_pipeline(storage_format)


if __name__ == "__main__":
    raise SystemExit(main())
