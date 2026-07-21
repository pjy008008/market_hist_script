"""Run the daily SIP one-minute adjusted-data pipeline end to end."""

from __future__ import annotations

import argparse
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
    CollectionResult,
    DEFAULT_CHUNK_DAYS,
    DEFAULT_REQUEST_DELAY_SECONDS,
    END_DELAY_MINUTES,
    YEARS_TO_COLLECT,
    storage_path,
    update_symbol_data,
)
from data_collection.get_ticker import get_historical_sp500_tickers
from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    build_sources,
    process_file_incrementally,
)
from data_filtering.resample_sip_5min import (
    BAR_INTERVALS,
    DESTINATION_ROOTS as RESAMPLED_ROOTS,
    build_resample_source,
    output_file_name,
    process_resample_file_incrementally,
)
from data_validation.audit_regular_session import (
    MISSING_INTERVAL_COLUMNS,
    audit_source,
)
from data_validation.quality_control import QualityResult, repair_symbol_file
from pipeline_reporting import DailyReportStore, replace_report_rows
from pipeline_state import PipelineStateStore


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_TYPES = ("adjusted", "raw")
DEFAULT_DATA_TYPE = "adjusted"
DATASET = "sip"
ONE_MINUTE_FREQUENCY = pd.Timedelta(minutes=1)
MIN_EXPECTED_TICKERS = 400
TICKER_FILE = PROJECT_ROOT / "ticker_info" / "sp500_tickers_3years.txt"
COLLECTION_ROOT = PROJECT_ROOT / "sip_market_data"
FILTERED_ROOT = PROJECT_ROOT / "regular_sip_1min_market_data"
MAX_SYMBOL_ATTEMPTS = 3
SYMBOL_RETRY_DELAY_SECONDS = 5
ADJUSTED_REFRESH_SESSIONS = 10


def stage_name(stage: str, data_type: str) -> str:
    """Keep existing adjusted checkpoints and isolate raw checkpoints."""
    return stage if data_type == DEFAULT_DATA_TYPE else f"{data_type}_{stage}"


def report_name(filename: str, data_type: str) -> str:
    """Keep existing adjusted report names and prefix the added raw reports."""
    return filename if data_type == DEFAULT_DATA_TYPE else f"{data_type}_{filename}"


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


def adjusted_refresh_start(
    end_time: datetime,
    sessions: int = ADJUSTED_REFRESH_SESSIONS,
    calendar_name: str = DEFAULT_CALENDAR,
) -> datetime:
    """Return the open of the recent sessions re-fetched for adjustment changes."""
    if sessions <= 0:
        raise ValueError("sessions must be positive")
    calendar = mcal.get_calendar(calendar_name)
    end = pd.Timestamp(end_time).tz_convert("UTC")
    local_end_date = end.tz_convert(calendar.tz).date()
    schedule = calendar.schedule(
        start_date=local_end_date - timedelta(days=max(45, sessions * 3)),
        end_date=local_end_date,
        tz="UTC",
    )
    completed = schedule[schedule["market_close"] <= end]
    if len(completed) < sessions:
        raise RuntimeError(
            f"최근 {sessions}개 완료 거래 세션을 찾지 못했습니다."
        )
    return pd.Timestamp(completed.iloc[-sessions]["market_open"]).to_pydatetime()


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
    refresh_start: datetime | None,
    retry_delay: float = SYMBOL_RETRY_DELAY_SECONDS,
    data_type: str = DEFAULT_DATA_TYPE,
) -> tuple[list[dict[str, object]], dict[str, pd.Timestamp]]:
    pending = list(symbols)
    last_attempts: dict[str, int] = {}
    changed_from_by_symbol: dict[str, pd.Timestamp] = {}
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
                data_type,
                storage_format,
                COLLECTION_ROOT,
            )
            if state.is_complete(
                storage_format,
                symbol,
                stage_name("collection", data_type),
                target_session_utc,
                file_path,
            ):
                print(f"[{index}/{len(pending)}] {symbol} 수집 체크포인트 완료 - 건너뜀")
                continue

            print(f"\n[{index}/{len(pending)}] {symbol} 수집 또는 갱신 (시도 {attempt})")
            last_attempts[symbol] = attempt
            succeeded = False
            try:
                result = update_symbol_data(
                    client,
                    symbol,
                    data_type,
                    storage_format,
                    COLLECTION_ROOT,
                    start_time,
                    end_time,
                    DEFAULT_CHUNK_DAYS,
                    DEFAULT_REQUEST_DELAY_SECONDS,
                    refresh_start,
                )
                succeeded = bool(result)
                error = "" if succeeded else "종목 수집 처리 실패"
                if result.changed_from_utc is not None:
                    changed_from_by_symbol[symbol] = result.changed_from_utc
            except Exception as exc:
                result = CollectionResult(False)
                succeeded = False
                error = str(exc)

            state.mark_stage(
                storage_format,
                symbol,
                stage_name("collection", data_type),
                "success" if succeeded else "failed",
                target_session_utc,
                attempt,
                error,
                details={
                    "changed_from_utc": (
                        result.changed_from_utc.isoformat()
                        if result.changed_from_utc is not None
                        else ""
                    ),
                    "adjustment_revision": result.adjustment_revision,
                    "added_rows": result.added_rows,
                },
            )
            if not succeeded:
                failed_this_round.append(symbol)
        pending = failed_this_round

    failures = [
        {
            "stage": "collection",
            "data_type": data_type,
            "symbol": symbol,
            "attempts": last_attempts.get(symbol, MAX_SYMBOL_ATTEMPTS),
            "error": "자동 재시도 후에도 수집 실패",
        }
        for symbol in pending
    ]
    return failures, changed_from_by_symbol


def run_quality_control(
    client: StockHistoricalDataClient,
    storage_format: str,
    symbols: list[str],
    state: PipelineStateStore,
    target_session_utc: str,
    start_time: datetime,
    end_time: datetime,
    repair_start: datetime,
    changed_from_by_symbol: dict[str, pd.Timestamp],
    deep_quality: bool = False,
    reports: DailyReportStore | None = None,
    data_type: str = DEFAULT_DATA_TYPE,
) -> tuple[list[str], list[dict[str, object]], dict[str, pd.Timestamp], int]:
    """Audit source bars, retry recent gaps, and block structurally invalid symbols."""
    reports = reports or DailyReportStore.for_target_session(
        target_session_utc,
        storage_format,
        DEFAULT_CALENDAR,
    )
    summary_filename = report_name("quality_summary.csv", data_type)
    invalid_filename = report_name("quality_invalid_rows.csv", data_type)
    missing_filename = report_name("quality_missing_intervals.csv", data_type)
    previous_summaries = reports.load_history_dataframe(summary_filename)
    previous_invalid_rows = reports.load_history_dataframe(
        invalid_filename
    )
    previous_missing_intervals = (
        reports.load_history_dataframe(
            missing_filename,
            detailed=True,
        )
        if deep_quality
        else pd.DataFrame()
    )
    has_prior_summary = (
        reports.history_root / summary_filename
    ).is_file()
    validated_symbols: list[str] = []
    failures: list[dict[str, object]] = []
    downstream_changes = dict(changed_from_by_symbol)
    summaries: list[dict[str, object]] = []
    missing_intervals: list[dict[str, object]] = []
    invalid_rows: list[dict[str, object]] = []
    repaired_rows = 0
    quality_stage = "quality_deep" if deep_quality else "quality_fast"
    checked_symbols: set[str] = set()

    for index, symbol in enumerate(symbols, 1):
        file_path = storage_path(symbol, data_type, storage_format, COLLECTION_ROOT)
        force_check = symbol in changed_from_by_symbol
        if has_prior_summary and not force_check and state.is_complete(
            storage_format,
            symbol,
            stage_name(quality_stage, data_type),
            target_session_utc,
            file_path,
        ):
            print(f"[{index}/{len(symbols)}] {symbol} 품질 검사 체크포인트 완료 - 건너뜀")
            validated_symbols.append(symbol)
            continue

        checked_symbols.add(symbol)
        result: QualityResult = repair_symbol_file(
            client,
            symbol,
            file_path,
            storage_format,
            data_type,
            start_time,
            end_time,
            repair_start,
            DEFAULT_CHUNK_DAYS,
            DEFAULT_REQUEST_DELAY_SECONDS,
            DEFAULT_CALENDAR,
            deep_quality,
        )
        if not result.success:
            error = result.error or "quality control failed"
            state.mark_stage(
                storage_format,
                symbol,
                stage_name(quality_stage, data_type),
                "failed",
                target_session_utc,
                1,
                error,
            )
            failures.append(
                {
                    "stage": "quality",
                    "data_type": data_type,
                    "symbol": symbol,
                    "attempts": 1,
                    "error": error,
                }
            )
            summaries.append({"symbol": symbol, "status": f"error: {error}"})
            continue

        summaries.append(result.summary)
        missing_intervals.extend(result.missing_intervals)
        invalid_rows.extend(result.invalid_rows)
        repaired_rows += result.repaired_rows
        if result.changed_from_utc is not None:
            previous = downstream_changes.get(symbol)
            downstream_changes[symbol] = (
                min(previous, result.changed_from_utc)
                if previous is not None
                else result.changed_from_utc
            )

        invalid_count = len(result.invalid_rows)
        duplicate_count = int(result.summary.get("duplicate_timestamps", 0))
        empty_source = int(result.summary.get("observed_rows", 0)) == 0
        structural_errors: list[str] = []
        if empty_source:
            structural_errors.append("empty source")
        if duplicate_count:
            structural_errors.append(f"duplicate timestamps: {duplicate_count}")
        if invalid_count:
            structural_errors.append(f"invalid OHLCV rows: {invalid_count}")
        status = "failed" if structural_errors else "success"
        error = "; ".join(structural_errors)
        state.mark_stage(
            storage_format,
            symbol,
            stage_name(quality_stage, data_type),
            status,
            target_session_utc,
            1,
            error,
            details={
                "missing_bars": int(result.summary.get("missing_bars", 0)),
                "repair_windows": int(result.summary.get("repair_windows", 0)),
                "repaired_rows": result.repaired_rows,
                "quality_mode": "deep" if deep_quality else "fast",
            },
        )
        if structural_errors:
            failures.append(
                {
                    "stage": "quality",
                    "data_type": data_type,
                    "symbol": symbol,
                    "attempts": 1,
                    "error": error,
                }
            )
        else:
            validated_symbols.append(symbol)

    summary_report = replace_report_rows(
        previous_summaries,
        pd.DataFrame(summaries),
        "symbol",
        checked_symbols,
    )
    invalid_report = replace_report_rows(
        previous_invalid_rows,
        pd.DataFrame(invalid_rows),
        "symbol",
        checked_symbols,
    )
    reports.save_dataframe(summary_report, summary_filename)
    reports.save_dataframe(invalid_report, invalid_filename)
    if deep_quality:
        missing_report = replace_report_rows(
            previous_missing_intervals,
            pd.DataFrame(missing_intervals, columns=MISSING_INTERVAL_COLUMNS),
            "symbol",
            checked_symbols,
        )
        reports.save_dataframe(
            missing_report,
            missing_filename,
            detailed=True,
        )
    return validated_symbols, failures, downstream_changes, repaired_rows


def run_filter(
    storage_format: str,
    symbols: list[str],
    state: PipelineStateStore,
    target_session_utc: str,
    start_time: datetime,
    end_time: datetime,
    changed_from_by_symbol: dict[str, pd.Timestamp] | None = None,
    data_type: str = DEFAULT_DATA_TYPE,
) -> tuple[int, int, int, list[dict[str, object]]]:
    changed_from_by_symbol = changed_from_by_symbol or {}
    source = build_sources(
        PROJECT_ROOT,
        FILTERED_ROOT,
        data_type,
        storage_format,
        DATASET,
    )[0]
    completed_files = 0
    candidate_rows = 0
    filtered_rows = 0
    failures: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, 1):
        input_path = storage_path(
            symbol, data_type, storage_format, COLLECTION_ROOT
        )
        output_path = source.destination_dir / input_path.name
        if symbol not in changed_from_by_symbol and state.is_complete(
            storage_format,
            symbol,
            stage_name("filter", data_type),
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
                changed_from_by_symbol.get(symbol),
            )
            state.mark_stage(
                storage_format,
                symbol,
                stage_name("filter", data_type),
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
                storage_format,
                symbol,
                stage_name("filter", data_type),
                "failed",
                target_session_utc,
                1,
                str(exc),
            )
            failures.append(
                {
                    "stage": "filter",
                    "data_type": data_type,
                    "symbol": symbol,
                    "attempts": 1,
                    "error": str(exc),
                }
            )
    return completed_files, candidate_rows, filtered_rows, failures


def run_resample(
    storage_format: str,
    symbols: list[str],
    state: PipelineStateStore,
    target_session_utc: str,
    start_time: datetime,
    end_time: datetime,
    changed_from_by_symbol: dict[str, pd.Timestamp] | None = None,
    data_type: str = DEFAULT_DATA_TYPE,
    bar_interval: str = "5min",
) -> tuple[int, int, int, list[dict[str, object]]]:
    if bar_interval not in BAR_INTERVALS:
        raise ValueError(f"지원하지 않는 봉 간격입니다: {bar_interval}")
    changed_from_by_symbol = changed_from_by_symbol or {}
    source = build_resample_source(
        storage_format,
        source_root=FILTERED_ROOT,
        destination_root=RESAMPLED_ROOTS[bar_interval],
        data_type=data_type,
    )
    completed_files = 0
    candidate_rows = 0
    output_rows = 0
    failures: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, 1):
        input_name = f"{symbol.replace('/', '-')}_1min_sip_historical.{storage_format}"
        input_path = source.source_dir / input_name
        output_path = source.destination_dir / output_file_name(
            input_path,
            bar_interval,
        )
        checkpoint_stage = f"resample_{bar_interval}"
        if symbol not in changed_from_by_symbol and state.is_complete(
            storage_format,
            symbol,
            stage_name(checkpoint_stage, data_type),
            target_session_utc,
            output_path,
        ):
            print(
                f"[{index}/{len(symbols)}] {symbol} "
                f"{bar_interval}봉 체크포인트 완료 - 건너뜀"
            )
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
                changed_from_by_symbol.get(symbol),
                bar_interval,
            )
            state.mark_stage(
                storage_format,
                symbol,
                stage_name(checkpoint_stage, data_type),
                "success",
                target_session_utc,
                1,
                details={"output_rows": total},
            )
            completed_files += 1
            candidate_rows += processed
            output_rows += created
            print(
                f"[{index}/{len(symbols)}] {symbol}: 증분 1분봉 "
                f"{processed:,} -> {bar_interval}봉 {created:,}행"
            )
        except (OSError, ValueError, ImportError) as exc:
            state.mark_stage(
                storage_format,
                symbol,
                stage_name(checkpoint_stage, data_type),
                "failed",
                target_session_utc,
                1,
                str(exc),
            )
            failures.append(
                {
                    "stage": checkpoint_stage,
                    "data_type": data_type,
                    "symbol": symbol,
                    "attempts": 1,
                    "error": str(exc),
                }
            )
    return completed_files, candidate_rows, output_rows, failures


def run_validation(
    storage_format: str,
    detailed: bool = False,
    reports: DailyReportStore | None = None,
    target_session_utc: str | None = None,
    data_type: str = DEFAULT_DATA_TYPE,
) -> tuple[int, int]:
    if reports is None:
        if target_session_utc is None:
            raise ValueError("target_session_utc is required without a report store")
        reports = DailyReportStore.for_target_session(
            target_session_utc,
            storage_format,
            DEFAULT_CALENDAR,
        )
    total_files = 0
    total_errors = 0
    sources = [
        ("1min", FILTERED_ROOT, ONE_MINUTE_FREQUENCY),
        *[
            (label, RESAMPLED_ROOTS[label], frequency)
            for label, frequency in BAR_INTERVALS.items()
        ],
    ]
    for label, source_root, bar_frequency in sources:
        source_dir = source_root / data_type / storage_format
        summary, intervals = audit_source(
            source_dir,
            storage_format,
            DEFAULT_CALENDAR,
            bar_frequency,
            include_intervals=detailed,
        )
        if summary.empty:
            raise RuntimeError(f"검사할 SIP {label} 정규장 데이터 파일이 없습니다.")

        summary_path, _ = reports.save_dataframe(
            summary,
            report_name(f"{label}_summary.csv", data_type),
        )
        intervals_path = (
            reports.latest_root
            / "deep_quality"
            / report_name(f"{label}_missing_intervals.csv", data_type)
        )
        if detailed:
            intervals_path, _ = reports.save_dataframe(
                intervals,
                report_name(f"{label}_missing_intervals.csv", data_type),
                detailed=True,
            )
        error_count = int(
            summary["status"].astype(str).str.startswith("error:").sum()
        )
        total_files += len(summary)
        total_errors += error_count
        print(f"[{label}] 요약 보고서: {summary_path}")
        if detailed:
            print(f"[{label}] 누락 구간 상세 보고서: {intervals_path}")
        else:
            print(f"[{label}] 누락 구간 상세 생성 생략 (빠른 모드)")
    return total_files, total_errors


def save_failure_report(
    failures: list[dict[str, object]],
    storage_format: str,
    target_session_utc: str,
    reports: DailyReportStore,
) -> Path:
    """Write current and session-dated failed-symbol reports."""
    payload = {
        "storage_format": storage_format,
        "target_session_utc": target_session_utc,
        "session_date": reports.session_date,
        "failure_count": len(failures),
        "failures": failures,
    }
    latest_path, _ = reports.save_json(payload, "pipeline_failures.json")
    return latest_path


def save_run_summary(
    reports: DailyReportStore,
    *,
    status: str,
    storage_format: str,
    target_session_utc: str,
    started_at_utc: str,
    failures: list[dict[str, object]],
    metrics: dict[str, object],
) -> Path:
    """Write a compact operational summary for the current session."""
    payload = {
        "status": status,
        "storage_format": storage_format,
        "target_session_utc": target_session_utc,
        "session_date": reports.session_date,
        "started_at_utc": started_at_utc,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        **metrics,
        "failure_count": len(failures),
    }
    latest_path, _ = reports.save_json(payload, "run_summary.json")
    return latest_path


def run_pipeline(
    storage_format: str,
    now: datetime | None = None,
    deep_quality: bool = False,
) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        print("[오류] Alpaca API 키와 Secret 키를 설정해 주세요.", file=sys.stderr)
        return 1

    try:
        start_time, end_time = completed_collection_window(now)
        refresh_start = adjusted_refresh_start(end_time)
        print("Wikipedia에서 최근 3년 S&P 500 관련 티커를 갱신합니다...")
        symbols = load_pipeline_symbols()
        state = PipelineStateStore()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[오류] 실행 준비 실패: {exc}", file=sys.stderr)
        return 1

    target_session_utc = pd.Timestamp(end_time).isoformat()
    started_at_utc = datetime.now(timezone.utc).isoformat()
    try:
        reports = DailyReportStore.for_target_session(
            target_session_utc,
            storage_format,
            DEFAULT_CALENDAR,
        )
        reports.prune_history()
    except (OSError, ValueError) as exc:
        print(f"[오류] 보고서 저장소 준비 실패: {exc}", file=sys.stderr)
        return 1
    state.begin_run(storage_format, target_session_utc)

    print("=" * 72)
    print("일일 통합 파이프라인")
    print("피드/간격   : SIP / 1분")
    print("가격 타입   : Adjusted + Raw")
    print(f"저장 형식   : {storage_format}")
    print(f"수집 기간   : {start_time.isoformat()} ~ {end_time.isoformat()}")
    print(f"마지막 세션 : {end_time.astimezone(timezone.utc).isoformat()}")
    print(f"대상 종목   : {len(symbols)}개")
    print(f"품질 모드   : {'상세 검사·누락 복구' if deep_quality else '빠른 구조 검사'}")
    print("=" * 72)

    client = StockHistoricalDataClient(api_key, secret_key)
    failures: list[dict[str, object]] = []
    changed_by_type: dict[str, dict[str, pd.Timestamp]] = {}
    collected_by_type: dict[str, list[str]] = {}
    quality_by_type: dict[str, list[str]] = {}
    filtered_by_type: dict[str, list[str]] = {}
    metrics_by_type: dict[str, dict[str, object]] = {
        data_type: {
            "collection_success_symbols": 0,
            "quality_success_symbols": 0,
            "repaired_rows": 0,
            "filtered_files": 0,
            "filtered_source_rows": 0,
            "regular_session_rows": 0,
            "resampled_files": 0,
            "resampled_source_rows": 0,
            "five_minute_rows": 0,
            "audited_files": 0,
            "audit_errors": 0,
            "bars_by_interval": {
                interval: {"files": 0, "source_rows": 0, "output_rows": 0}
                for interval in BAR_INTERVALS
            },
        }
        for data_type in DATA_TYPES
    }

    print("\n[1/5] SIP Adjusted·Raw 1분봉 수집")
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} 수집 ---")
        collection_failures, changes = run_collection(
            client,
            symbols,
            storage_format,
            start_time,
            end_time,
            state,
            target_session_utc,
            refresh_start if data_type == "adjusted" else None,
            data_type=data_type,
        )
        failures.extend(collection_failures)
        failed_symbols = {
            str(failure["symbol"]) for failure in collection_failures
        }
        collected = [symbol for symbol in symbols if symbol not in failed_symbols]
        collected_by_type[data_type] = collected
        changed_by_type[data_type] = changes
        metrics_by_type[data_type]["collection_success_symbols"] = len(collected)

    print(
        "\n[2/5] "
        + (
            "Adjusted·Raw 데이터 품질 상세 검사 및 최근 누락 구간 복구"
            if deep_quality
            else "Adjusted·Raw 빠른 데이터 구조 검사"
        )
    )
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} 품질 검사 ---")
        (
            quality_symbols,
            quality_failures,
            downstream_changes,
            repaired_rows,
        ) = run_quality_control(
            client,
            storage_format,
            collected_by_type[data_type],
            state,
            target_session_utc,
            start_time,
            end_time,
            refresh_start,
            changed_by_type[data_type],
            deep_quality,
            reports,
            data_type=data_type,
        )
        failures.extend(quality_failures)
        quality_by_type[data_type] = quality_symbols
        changed_by_type[data_type] = downstream_changes
        metrics_by_type[data_type]["quality_success_symbols"] = len(quality_symbols)
        metrics_by_type[data_type]["repaired_rows"] = repaired_rows

    print("\n[3/5] Adjusted·Raw XNYS 정규장 1분봉 필터링")
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} 정규장 필터 ---")
        try:
            processed, source_rows, kept_rows, filter_failures = run_filter(
                storage_format,
                quality_by_type[data_type],
                state,
                target_session_utc,
                start_time,
                end_time,
                changed_by_type[data_type],
                data_type=data_type,
            )
            failures.extend(filter_failures)
            if processed == 0:
                raise RuntimeError("필터링할 수집 파일이 없습니다.")
            failed_symbols = {
                str(failure["symbol"]) for failure in filter_failures
            }
            filtered_by_type[data_type] = [
                symbol
                for symbol in quality_by_type[data_type]
                if symbol not in failed_symbols
            ]
            metrics_by_type[data_type]["filtered_files"] = processed
            metrics_by_type[data_type]["filtered_source_rows"] = source_rows
            metrics_by_type[data_type]["regular_session_rows"] = kept_rows
        except (OSError, RuntimeError, ValueError, ImportError) as exc:
            filtered_by_type[data_type] = []
            failures.append(
                {
                    "stage": "filter",
                    "data_type": data_type,
                    "symbol": "*",
                    "attempts": 1,
                    "error": str(exc),
                }
            )

    print("\n[4/5] Adjusted·Raw 정규장 1분봉에서 다중 주기 봉 생성")
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} 다중 주기 봉 생성 ---")
        for bar_interval in BAR_INTERVALS:
            print(f"\n[{data_type.upper()}] {bar_interval}봉")
            try:
                resampled, source_rows, output_rows, resample_failures = run_resample(
                    storage_format,
                    filtered_by_type[data_type],
                    state,
                    target_session_utc,
                    start_time,
                    end_time,
                    changed_by_type[data_type],
                    data_type=data_type,
                    bar_interval=bar_interval,
                )
                failures.extend(resample_failures)
                if resampled == 0:
                    raise RuntimeError(
                        f"{bar_interval}봉으로 변환할 정규장 1분봉 파일이 없습니다."
                    )
                interval_metrics = metrics_by_type[data_type]["bars_by_interval"]
                interval_metrics[bar_interval] = {
                    "files": resampled,
                    "source_rows": source_rows,
                    "output_rows": output_rows,
                }
                if bar_interval == "5min":
                    metrics_by_type[data_type]["resampled_files"] = resampled
                    metrics_by_type[data_type]["resampled_source_rows"] = source_rows
                    metrics_by_type[data_type]["five_minute_rows"] = output_rows
            except (OSError, RuntimeError, ValueError, ImportError) as exc:
                failures.append(
                    {
                        "stage": f"resample_{bar_interval}",
                        "data_type": data_type,
                        "symbol": "*",
                        "attempts": 1,
                        "error": str(exc),
                    }
                )

    print(
        "\n[5/5] "
        + (
            "Adjusted·Raw SIP 전체 주기 기간 및 누락 구간 상세 검사"
            if deep_quality
            else "Adjusted·Raw SIP 전체 주기 기간·커버리지 요약 검사"
        )
    )
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} 보고서 생성 ---")
        try:
            audited_files, audit_errors = run_validation(
                storage_format,
                detailed=deep_quality,
                reports=reports,
                data_type=data_type,
            )
            metrics_by_type[data_type]["audited_files"] = audited_files
            metrics_by_type[data_type]["audit_errors"] = audit_errors
            if audit_errors:
                failures.append(
                    {
                        "stage": "validation",
                        "data_type": data_type,
                        "symbol": "*",
                        "attempts": 1,
                        "error": f"검사 오류 파일 {audit_errors}개",
                    }
                )
        except (OSError, RuntimeError, ValueError, ImportError) as exc:
            failures.append(
                {
                    "stage": "validation",
                    "data_type": data_type,
                    "symbol": "*",
                    "attempts": 1,
                    "error": str(exc),
                }
            )

    numeric_metric_keys = (
        "collection_success_symbols",
        "quality_success_symbols",
        "repaired_rows",
        "filtered_files",
        "filtered_source_rows",
        "regular_session_rows",
        "resampled_files",
        "resampled_source_rows",
        "five_minute_rows",
        "audited_files",
        "audit_errors",
    )
    totals = {
        key: sum(int(values[key]) for values in metrics_by_type.values())
        for key in numeric_metric_keys
    }
    bar_totals = {
        interval: {
            metric: sum(
                int(values["bars_by_interval"][interval][metric])
                for values in metrics_by_type.values()
            )
            for metric in ("files", "source_rows", "output_rows")
        }
        for interval in BAR_INTERVALS
    }
    final_status = "failed" if failures else "success"
    failure_report_path = save_failure_report(
        failures,
        storage_format,
        target_session_utc,
        reports,
    )
    run_summary_path = save_run_summary(
        reports,
        status=final_status,
        storage_format=storage_format,
        target_session_utc=target_session_utc,
        started_at_utc=started_at_utc,
        failures=failures,
        metrics={
            "quality_mode": "deep" if deep_quality else "fast",
            "data_types": list(DATA_TYPES),
            "symbols_total": len(symbols),
            **totals,
            "bars_by_interval": bar_totals,
            "by_data_type": metrics_by_type,
        },
    )
    state.finish_run(final_status, failures)

    print("=" * 72)
    for data_type in DATA_TYPES:
        metrics = metrics_by_type[data_type]
        quality_result_text = (
            f"최근 누락 복구 {metrics['repaired_rows']:,}행"
            if deep_quality
            else "누락 재요청 생략"
        )
        interval_text = ", ".join(
            f"{interval} {metrics['bars_by_interval'][interval]['files']}개"
            for interval in BAR_INTERVALS
        )
        print(
            f"{data_type.upper()}: 수집 {metrics['collection_success_symbols']}/{len(symbols)}개, "
            f"품질 {metrics['quality_success_symbols']}개, "
            f"1분봉 필터 {metrics['filtered_files']}개 파일, "
            f"생성 파일 ({interval_text}), "
            f"검사 {metrics['audited_files']}개 파일, {quality_result_text}"
        )
    if failures:
        print(f"최종 실패 {len(failures)}건: {failure_report_path}")
    if totals["audit_errors"]:
        print(f"검사 오류 파일: {totals['audit_errors']}개")
    print(f"실행 요약: {run_summary_path}")
    print(f"거래일별 보고서: {reports.history_root}")
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SIP Adjusted·Raw 1분봉을 갱신하고 정규장 필터링, "
            "다중 주기 봉 생성과 데이터 검사를 수행합니다."
        )
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet"),
        help="저장 형식 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--deep-quality",
        action="store_true",
        help=(
            "전체 누락 구간 상세 보고서와 최근 10거래일 누락 재요청을 실행합니다. "
            "기본값은 빠른 구조 검사입니다."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage_format = args.storage_format or choose_storage_format()
    return run_pipeline(storage_format, deep_quality=args.deep_quality)


if __name__ == "__main__":
    raise SystemExit(main())
