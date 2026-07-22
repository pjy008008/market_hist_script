"""Run the ten-year regular-session SIP 1h/4h/1d pipeline."""

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
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv

from data_collection.collect_sip_1min import CollectionResult, InactiveSymbolCache
from data_collection.collect_sip_long_term import (
    DEFAULT_CHUNK_DAYS,
    DEFAULT_REQUEST_DELAY_SECONDS,
    END_DELAY_MINUTES,
    HISTORY_YEARS,
    OUTPUT_INTERVALS,
    OUTPUT_ROOTS,
    output_paths,
    rolling_window,
    update_symbol_data,
)
from data_collection.etf_universe import load_etf_symbols
from data_collection.get_ticker import get_historical_sp500_tickers
from data_filtering.filter_regular_session import DEFAULT_CALENDAR, load_market_data
from data_filtering.resample_sip_5min import BAR_INTERVALS
from data_validation.audit_regular_session import (
    MISSING_INTERVAL_COLUMNS,
    audit_dataframe,
)
from data_validation.quality_control import invalid_market_rows
from pipeline_reporting import DailyReportStore
from pipeline_state import PipelineStateStore


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_TYPES = ("adjusted", "raw")
DEFAULT_DATA_TYPE = "adjusted"
TICKER_LOOKBACK_YEARS = 10
MIN_EXPECTED_TICKERS = 400
TICKER_FILE = PROJECT_ROOT / "ticker_info" / "sp500_tickers_10years.txt"
MAX_SYMBOL_ATTEMPTS = 3
SYMBOL_RETRY_DELAY_SECONDS = 5
ADJUSTED_REFRESH_SESSIONS = 10


def stage_name(stage: str, data_type: str) -> str:
    return stage if data_type == DEFAULT_DATA_TYPE else f"{data_type}_{stage}"


def report_name(filename: str, data_type: str) -> str:
    return filename if data_type == DEFAULT_DATA_TYPE else f"{data_type}_{filename}"


def choose_storage_format() -> str:
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
    """Return a ten-year window ending at the latest completed XNYS session."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    available_until = pd.Timestamp(
        current.astimezone(timezone.utc) - timedelta(minutes=END_DELAY_MINUTES)
    )
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
    return rolling_window(end_time, HISTORY_YEARS)


def adjusted_refresh_start(
    end_time: datetime,
    sessions: int = ADJUSTED_REFRESH_SESSIONS,
    calendar_name: str = DEFAULT_CALENDAR,
) -> datetime:
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
        raise RuntimeError(f"최근 {sessions}개 완료 거래 세션을 찾지 못했습니다.")
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
    additional_symbols: list[str] | None = None,
) -> list[str]:
    """Build the current and trailing ten-year S&P membership universe."""
    fetched = get_historical_sp500_tickers(years=TICKER_LOOKBACK_YEARS)
    if len(fetched) >= minimum_count:
        sp500_symbols = sorted(set(fetched))
        save_ticker_file(sp500_symbols, file_path)
    else:
        cached = read_ticker_file(file_path)
        if len(cached) < minimum_count:
            raise RuntimeError(
                "S&P 500 티커 목록을 정상적으로 가져오지 못했고 사용할 기존 목록도 없습니다."
            )
        print(
            f"[경고] 티커 갱신 결과가 {len(fetched)}개뿐이어서 "
            f"기존 {len(cached)}개 목록을 사용합니다."
        )
        sp500_symbols = cached
    extras = {
        symbol.strip().upper()
        for symbol in (additional_symbols or [])
        if symbol.strip()
    }
    combined = sorted(set(sp500_symbols).union(extras))
    print(
        f"종목 유니버스: S&P 500 관련 {len(sp500_symbols)}개 + "
        f"ETF {len(extras)}개 = 중복 제거 총 {len(combined)}개"
    )
    return combined


def _checkpoint_complete(
    state: PipelineStateStore,
    storage_format: str,
    symbol: str,
    data_type: str,
    target_session_utc: str,
) -> bool:
    paths = output_paths(symbol, data_type, storage_format)
    return state.is_complete_outputs(
        storage_format,
        symbol,
        stage_name("long_term_collection", data_type),
        target_session_utc,
        list(paths.values()),
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
    data_type: str,
    asset_client: TradingClient,
    inactive_cache: InactiveSymbolCache,
    retry_delay: float = SYMBOL_RETRY_DELAY_SECONDS,
) -> tuple[list[dict[str, object]], int]:
    pending = list(symbols)
    last_errors: dict[str, str] = {}
    added_rows = 0
    checkpoint_stage = stage_name("long_term_collection", data_type)
    for attempt in range(1, MAX_SYMBOL_ATTEMPTS + 1):
        if not pending:
            break
        if attempt > 1:
            print(f"\n수집 실패 종목 재시도 {attempt}/{MAX_SYMBOL_ATTEMPTS}: {len(pending)}개")
            if retry_delay > 0:
                time.sleep(retry_delay)
        failed_this_round: list[str] = []
        for index, symbol in enumerate(pending, 1):
            if _checkpoint_complete(
                state,
                storage_format,
                symbol,
                data_type,
                target_session_utc,
            ):
                print(f"[{index}/{len(pending)}] {symbol} 장기봉 체크포인트 완료 - 건너뜀")
                continue
            print(f"\n[{index}/{len(pending)}] {symbol} 10년 장기봉 갱신 (시도 {attempt})")
            try:
                result = update_symbol_data(
                    client,
                    symbol,
                    data_type,
                    storage_format,
                    start_time,
                    end_time,
                    refresh_start=refresh_start,
                    chunk_days=DEFAULT_CHUNK_DAYS,
                    request_delay=DEFAULT_REQUEST_DELAY_SECONDS,
                    asset_client=asset_client,
                    inactive_cache=inactive_cache,
                )
                error = "" if result else "장기봉 수집 처리 실패"
            except Exception as exc:
                result = CollectionResult(False)
                error = str(exc)
            state.mark_stage(
                storage_format,
                symbol,
                checkpoint_stage,
                "success" if result else "failed",
                target_session_utc,
                attempt,
                error,
                details={
                    "history_years": HISTORY_YEARS,
                    "intervals": list(OUTPUT_INTERVALS),
                    "changed_from_utc": (
                        result.changed_from_utc.isoformat()
                        if result.changed_from_utc is not None
                        else ""
                    ),
                    "adjustment_revision": result.adjustment_revision,
                    "added_rows": result.added_rows,
                    "inactive": result.inactive,
                },
            )
            if result:
                added_rows += result.added_rows
            else:
                last_errors[symbol] = error
                failed_this_round.append(symbol)
        pending = failed_this_round
    failures = [
        {
            "stage": "long_term_collection",
            "data_type": data_type,
            "symbol": symbol,
            "attempts": MAX_SYMBOL_ATTEMPTS,
            "error": last_errors.get(symbol, "자동 재시도 후에도 수집 실패"),
        }
        for symbol in pending
    ]
    return failures, added_rows


def run_validation(
    storage_format: str,
    symbols: list[str],
    reports: DailyReportStore,
    data_type: str,
    detailed: bool,
) -> tuple[int, int]:
    total_files = 0
    total_errors = 0
    for interval in OUTPUT_INTERVALS:
        summaries: list[dict[str, object]] = []
        intervals: list[dict[str, object]] = []
        frequency = BAR_INTERVALS[interval]
        for symbol in symbols:
            path = output_paths(symbol, data_type, storage_format)[interval]
            if not path.is_file():
                summaries.append(
                    {
                        "symbol": symbol,
                        "status": "error: output file missing",
                        "file": path.name,
                    }
                )
                total_errors += 1
                continue
            try:
                dataframe = load_market_data(path, storage_format)
                summary, missing = audit_dataframe(
                    dataframe,
                    symbol,
                    DEFAULT_CALENDAR,
                    frequency,
                    include_intervals=detailed,
                )
                invalid_rows = invalid_market_rows(dataframe, symbol)
                structural_errors: list[str] = []
                if dataframe.empty:
                    structural_errors.append("empty output")
                if int(summary.get("duplicate_timestamps", 0)):
                    structural_errors.append("duplicate timestamps")
                if int(summary.get("unexpected_timestamps", 0)):
                    structural_errors.append("unexpected timestamps")
                if invalid_rows:
                    structural_errors.append(f"invalid OHLCV rows: {len(invalid_rows)}")
                summary["invalid_ohlcv_rows"] = len(invalid_rows)
                summary["status"] = (
                    "error: " + "; ".join(structural_errors)
                    if structural_errors
                    else "ok"
                )
                summary["file"] = path.name
                summaries.append(summary)
                intervals.extend(missing)
                if structural_errors:
                    total_errors += 1
            except (OSError, ValueError, ImportError) as exc:
                summaries.append(
                    {
                        "symbol": symbol,
                        "status": f"error: {exc}",
                        "file": path.name,
                    }
                )
                total_errors += 1
        summary_path, _ = reports.save_dataframe(
            pd.DataFrame(summaries),
            report_name(f"{interval}_summary.csv", data_type),
        )
        if detailed:
            reports.save_dataframe(
                pd.DataFrame(intervals, columns=MISSING_INTERVAL_COLUMNS),
                report_name(f"{interval}_missing_intervals.csv", data_type),
                detailed=True,
            )
        total_files += len(summaries)
        print(f"[{data_type}/{interval}] 검증 보고서: {summary_path}")
    return total_files, total_errors


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
        print("Wikipedia에서 최근 10년 S&P 500 관련 티커를 갱신합니다...")
        etf_symbols = load_etf_symbols(as_of=end_time)
        symbols = load_pipeline_symbols(additional_symbols=etf_symbols)
        state = PipelineStateStore()
        target_session_utc = pd.Timestamp(end_time).isoformat()
        reports = DailyReportStore.for_target_session(
            target_session_utc,
            storage_format,
            DEFAULT_CALENDAR,
        )
        reports.prune_history()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[오류] 실행 준비 실패: {exc}", file=sys.stderr)
        return 1

    started_at_utc = datetime.now(timezone.utc).isoformat()
    state.begin_run(storage_format, target_session_utc)
    print("=" * 72)
    print("10년 장기봉 통합 파이프라인")
    print("피드/입력   : SIP / 임시 30분봉")
    print("저장 출력   : 정규장 1시간봉, 4시간봉, 일봉")
    print("가격 타입   : Adjusted + Raw")
    print(f"저장 형식   : {storage_format}")
    print(f"수집 기간   : {start_time.isoformat()} ~ {end_time.isoformat()}")
    print(f"대상 종목   : {len(symbols)}개 (ETF {len(etf_symbols)}개 포함)")
    print("=" * 72)

    client = StockHistoricalDataClient(api_key, secret_key)
    asset_client = TradingClient(api_key, secret_key)
    inactive_cache = InactiveSymbolCache()
    failures: list[dict[str, object]] = []
    metrics: dict[str, dict[str, int]] = {}

    print("\n[1/2] Adjusted·Raw 10년 1시간·4시간·일봉 수집")
    for data_type in DATA_TYPES:
        print(f"\n--- {data_type.upper()} ---")
        collection_failures, added_rows = run_collection(
            client,
            symbols,
            storage_format,
            start_time,
            end_time,
            state,
            target_session_utc,
            refresh_start if data_type == "adjusted" else None,
            data_type,
            asset_client,
            inactive_cache,
        )
        failures.extend(collection_failures)
        metrics[data_type] = {
            "collection_success_symbols": len(symbols) - len(collection_failures),
            "added_rows": added_rows,
            "audited_files": 0,
            "audit_errors": 0,
        }

    print("\n[2/2] 1시간·4시간·일봉 구조 및 커버리지 검사")
    for data_type in DATA_TYPES:
        audited_files, audit_errors = run_validation(
            storage_format,
            symbols,
            reports,
            data_type,
            deep_quality,
        )
        metrics[data_type]["audited_files"] = audited_files
        metrics[data_type]["audit_errors"] = audit_errors
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

    final_status = "failed" if failures else "success"
    failure_payload = {
        "storage_format": storage_format,
        "target_session_utc": target_session_utc,
        "session_date": reports.session_date,
        "failure_count": len(failures),
        "failures": failures,
    }
    failure_path, _ = reports.save_json(failure_payload, "pipeline_failures.json")
    summary_payload = {
        "status": final_status,
        "storage_format": storage_format,
        "target_session_utc": target_session_utc,
        "session_date": reports.session_date,
        "started_at_utc": started_at_utc,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "history_years": HISTORY_YEARS,
        "source_timeframe": "30Min (not persisted)",
        "output_intervals": list(OUTPUT_INTERVALS),
        "data_types": list(DATA_TYPES),
        "symbols_total": len(symbols),
        "sp500_related_symbols_total": len(set(symbols).difference(etf_symbols)),
        "etf_symbols_total": len(etf_symbols),
        "etf_symbols": etf_symbols,
        "by_data_type": metrics,
        "failure_count": len(failures),
    }
    summary_path, _ = reports.save_json(summary_payload, "run_summary.json")
    state.finish_run(final_status, failures)
    print("=" * 72)
    for data_type in DATA_TYPES:
        values = metrics[data_type]
        print(
            f"{data_type.upper()}: 수집 "
            f"{values['collection_success_symbols']}/{len(symbols)}개, "
            f"추가 {values['added_rows']:,}행, "
            f"검사 {values['audited_files']}개 파일, "
            f"오류 {values['audit_errors']}개"
        )
    if failures:
        print(f"최종 실패 {len(failures)}건: {failure_path}")
    print(f"실행 요약: {summary_path}")
    print(f"거래일별 보고서: {reports.history_root}")
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "기존 종목 유니버스의 최근 10년 SIP 정규장 "
            "1시간봉·4시간봉·일봉을 갱신합니다."
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
        help="종목별 누락 구간 상세 CSV도 생성합니다.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage_format = args.storage_format or choose_storage_format()
    return run_pipeline(storage_format, deep_quality=args.deep_quality)


if __name__ == "__main__":
    raise SystemExit(main())
