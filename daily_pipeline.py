"""Run the daily SIP one-minute adjusted-data pipeline end to end."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
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
    storage_path,
)
from data_collection.get_ticker import get_historical_sp500_tickers
from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    build_sources,
    process_file_incrementally,
)
from data_filtering.resample_sip_5min import (
    DESTINATION_ROOT as RESAMPLED_ROOT,
    build_resample_source,
    output_file_name,
    process_resample_file_incrementally,
)
from data_validation.audit_regular_session import audit_source, save_report
from pipeline_state import PipelineStateStore


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_TYPE = "adjusted"
DATASET = "sip"
ONE_MINUTE_FREQUENCY = pd.Timedelta(minutes=1)
FIVE_MINUTE_FREQUENCY = pd.Timedelta(minutes=5)
MIN_EXPECTED_TICKERS = 400
TICKER_FILE = PROJECT_ROOT / "ticker_info" / "sp500_tickers_3years.txt"
COLLECTION_ROOT = PROJECT_ROOT / "sip_market_data"
FILTERED_ROOT = PROJECT_ROOT / "regular_sip_market_data"
REPORT_ROOT = PROJECT_ROOT / "report" / "regular_sip_session_audit"
FAILURE_REPORT_PATH = PROJECT_ROOT / "report" / "pipeline_failures.json"
MAX_SYMBOL_ATTEMPTS = 3
SYMBOL_RETRY_DELAY_SECONDS = 5


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
    state: PipelineStateStore,
    target_session_utc: str,
    retry_delay: float = SYMBOL_RETRY_DELAY_SECONDS,
) -> list[dict[str, object]]:
    pending = list(symbols)
    last_attempts: dict[str, int] = {}
    for attempt in range(1, MAX_SYMBOL_ATTEMPTS + 1):
        if not pending:
            break
        if attempt > 1:
            print(f"\n수집 실패 종목 재시도 {attempt}/{MAX_SYMBOL_ATTEMPTS}: {len(pending)}개")
            if retry_delay > 0:
                time.sleep(retry_delay)

        failed_this_round: list[str] = []
        for index, symbol in enumerate(pending, 1):
            file_path = storage_path(
                symbol,
                DATA_TYPE,
                storage_format,
                COLLECTION_ROOT,
            )
            if state.is_complete(
                storage_format,
                symbol,
                "collection",
                target_session_utc,
                file_path,
            ):
                print(f"[{index}/{len(pending)}] {symbol} 수집 체크포인트 완료 - 건너뜀")
                continue

            print(f"\n[{index}/{len(pending)}] {symbol} 수집 또는 갱신 (시도 {attempt})")
            last_attempts[symbol] = attempt
            try:
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
                error = "" if succeeded else "종목 수집 처리 실패"
            except Exception as exc:
                succeeded = False
                error = str(exc)

            state.mark_stage(
                storage_format,
                symbol,
                "collection",
                "success" if succeeded else "failed",
                target_session_utc,
                attempt,
                error,
            )
            if not succeeded:
                failed_this_round.append(symbol)
        pending = failed_this_round

    return [
        {
            "stage": "collection",
            "symbol": symbol,
            "attempts": last_attempts.get(symbol, MAX_SYMBOL_ATTEMPTS),
            "error": "자동 재시도 후에도 수집 실패",
        }
        for symbol in pending
    ]


def run_filter(
    storage_format: str,
    symbols: list[str],
    state: PipelineStateStore,
    target_session_utc: str,
    start_time: datetime,
    end_time: datetime,
) -> tuple[int, int, int, list[dict[str, object]]]:
    source = build_sources(
        PROJECT_ROOT,
        FILTERED_ROOT,
        DATA_TYPE,
        storage_format,
        DATASET,
    )[0]
    completed_files = 0
    candidate_rows = 0
    filtered_rows = 0
    failures: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, 1):
        input_path = storage_path(
            symbol, DATA_TYPE, storage_format, COLLECTION_ROOT
        )
        output_path = source.destination_dir / input_path.name
        if state.is_complete(
            storage_format,
            symbol,
            "filter",
            target_session_utc,
            output_path,
        ):
            print(f"[{index}/{len(symbols)}] {symbol} 필터 체크포인트 완료 - 건너뜀")
            completed_files += 1
            continue
        try:
            processed, kept, total = process_file_incrementally(
                input_path,
                output_path,
                storage_format,
                pd.Timestamp(start_time),
                pd.Timestamp(end_time),
                DEFAULT_CALENDAR,
            )
            state.mark_stage(
                storage_format,
                symbol,
                "filter",
                "success",
                target_session_utc,
                1,
                details={"output_rows": total},
            )
            completed_files += 1
            candidate_rows += processed
            filtered_rows += kept
            print(f"[{index}/{len(symbols)}] {symbol}: 증분 {processed:,} -> 정규장 {kept:,}행")
        except (OSError, ValueError, ImportError) as exc:
            state.mark_stage(
                storage_format, symbol, "filter", "failed", target_session_utc, 1, str(exc)
            )
            failures.append({"stage": "filter", "symbol": symbol, "attempts": 1, "error": str(exc)})
    return completed_files, candidate_rows, filtered_rows, failures


def run_resample(
    storage_format: str,
    symbols: list[str],
    state: PipelineStateStore,
    target_session_utc: str,
    start_time: datetime,
    end_time: datetime,
) -> tuple[int, int, int, list[dict[str, object]]]:
    source = build_resample_source(storage_format)
    completed_files = 0
    candidate_rows = 0
    five_minute_rows = 0
    failures: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, 1):
        input_name = f"{symbol.replace('/', '-')}_1min_sip_historical.{storage_format}"
        input_path = source.source_dir / input_name
        output_path = source.destination_dir / output_file_name(input_path)
        if state.is_complete(
            storage_format,
            symbol,
            "resample_5min",
            target_session_utc,
            output_path,
        ):
            print(f"[{index}/{len(symbols)}] {symbol} 5분봉 체크포인트 완료 - 건너뜀")
            completed_files += 1
            continue
        try:
            processed, created, total = process_resample_file_incrementally(
                input_path,
                output_path,
                storage_format,
                pd.Timestamp(start_time),
                pd.Timestamp(end_time),
                DEFAULT_CALENDAR,
            )
            state.mark_stage(
                storage_format,
                symbol,
                "resample_5min",
                "success",
                target_session_utc,
                1,
                details={"output_rows": total},
            )
            completed_files += 1
            candidate_rows += processed
            five_minute_rows += created
            print(f"[{index}/{len(symbols)}] {symbol}: 증분 1분봉 {processed:,} -> 5분봉 {created:,}행")
        except (OSError, ValueError, ImportError) as exc:
            state.mark_stage(
                storage_format,
                symbol,
                "resample_5min",
                "failed",
                target_session_utc,
                1,
                str(exc),
            )
            failures.append(
                {"stage": "resample_5min", "symbol": symbol, "attempts": 1, "error": str(exc)}
            )
    return completed_files, candidate_rows, five_minute_rows, failures


def run_validation(storage_format: str) -> tuple[int, int]:
    total_files = 0
    total_errors = 0
    sources = (
        ("1min", FILTERED_ROOT, ONE_MINUTE_FREQUENCY),
        ("5min", RESAMPLED_ROOT, FIVE_MINUTE_FREQUENCY),
    )
    for label, source_root, bar_frequency in sources:
        source_dir = source_root / DATA_TYPE / storage_format
        summary, intervals = audit_source(
            source_dir,
            storage_format,
            DEFAULT_CALENDAR,
            bar_frequency,
        )
        if summary.empty:
            raise RuntimeError(f"검사할 SIP {label} 정규장 데이터 파일이 없습니다.")

        prefix = f"{DATA_TYPE}_{storage_format}"
        report_dir = REPORT_ROOT / label
        summary_path = report_dir / f"{prefix}_summary.csv"
        intervals_path = report_dir / f"{prefix}_missing_intervals.csv"
        save_report(summary, summary_path)
        save_report(intervals, intervals_path)
        error_count = int(
            summary["status"].astype(str).str.startswith("error:").sum()
        )
        total_files += len(summary)
        total_errors += error_count
        print(f"[{label}] 요약 보고서: {summary_path}")
        print(f"[{label}] 누락 구간 보고서: {intervals_path}")
    return total_files, total_errors


def save_failure_report(
    failures: list[dict[str, object]],
    storage_format: str,
    target_session_utc: str,
) -> None:
    """Atomically write the latest failed-symbol report for server monitoring."""
    FAILURE_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = FAILURE_REPORT_PATH.with_name(
        f".{FAILURE_REPORT_PATH.name}.tmp"
    )
    payload = {
        "storage_format": storage_format,
        "target_session_utc": target_session_utc,
        "failure_count": len(failures),
        "failures": failures,
    }
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, FAILURE_REPORT_PATH)
    finally:
        temporary_path.unlink(missing_ok=True)


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
        state = PipelineStateStore()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[오류] 실행 준비 실패: {exc}", file=sys.stderr)
        return 1

    target_session_utc = pd.Timestamp(end_time).isoformat()
    state.begin_run(storage_format, target_session_utc)

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
    print("\n[1/4] SIP Adjusted 1분봉 수집·증분 갱신")
    failures = run_collection(
        client,
        symbols,
        storage_format,
        start_time,
        end_time,
        state,
        target_session_utc,
    )
    failed_collection_symbols = {
        str(failure["symbol"]) for failure in failures
    }
    collected_symbols = [
        symbol for symbol in symbols if symbol not in failed_collection_symbols
    ]

    print("\n[2/4] XNYS 정규장 1분봉 필터링")
    try:
        processed_files, total_rows, kept_rows, filter_failures = run_filter(
            storage_format,
            collected_symbols,
            state,
            target_session_utc,
            start_time,
            end_time,
        )
        failures.extend(filter_failures)
        if processed_files == 0:
            raise RuntimeError("필터링할 수집 파일이 없습니다.")

        print("\n[3/4] 정규장 1분봉에서 SIP 5분봉 생성")
        failed_filter_symbols = {
            str(failure["symbol"]) for failure in filter_failures
        }
        filtered_symbols = [
            symbol
            for symbol in collected_symbols
            if symbol not in failed_filter_symbols
        ]
        (
            resampled_files,
            one_minute_rows,
            five_minute_rows,
            resample_failures,
        ) = run_resample(
            storage_format,
            filtered_symbols,
            state,
            target_session_utc,
            start_time,
            end_time,
        )
        failures.extend(resample_failures)
        if resampled_files == 0:
            raise RuntimeError("5분봉으로 변환할 정규장 1분봉 파일이 없습니다.")

        print("\n[4/4] SIP 1분봉·5분봉 기간 및 누락 구간 검사")
        audited_files, audit_errors = run_validation(storage_format)
    except (OSError, RuntimeError, ValueError, ImportError) as exc:
        failures.append(
            {"stage": "postprocess", "symbol": "*", "attempts": 1, "error": str(exc)}
        )
        save_failure_report(failures, storage_format, target_session_utc)
        state.finish_run("failed", failures)
        print(f"[오류] 후처리 실패: {exc}", file=sys.stderr)
        return 1


    if audit_errors:
        failures.append(
            {
                "stage": "validation",
                "symbol": "*",
                "attempts": 1,
                "error": f"검사 오류 파일 {audit_errors}개",
            }
        )
    save_failure_report(failures, storage_format, target_session_utc)
    state.finish_run("failed" if failures else "success", failures)

    print("=" * 72)
    print(
        f"완료: 수집 성공 {len(symbols) - len(failed_collection_symbols)}/{len(symbols)}개, "
        f"1분봉 필터 {processed_files}개 파일 ({total_rows:,} -> {kept_rows:,}행), "
        f"5분봉 {resampled_files}개 파일 "
        f"({one_minute_rows:,} -> {five_minute_rows:,}행), "
        f"검사 {audited_files}개 파일"
    )
    if failures:
        print(f"최종 실패 {len(failures)}건: {FAILURE_REPORT_PATH}")
    if audit_errors:
        print(f"검사 오류 파일: {audit_errors}개")
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SIP Adjusted 1분봉을 갱신하고 정규장 필터링, 5분봉 생성과 "
            "데이터 검사를 수행합니다."
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
