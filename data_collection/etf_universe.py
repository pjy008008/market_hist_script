"""Load the curated ETF universe used by the integrated SIP pipeline."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ETF_UNIVERSE_FILE = PROJECT_ROOT / "ticker_info" / "etf_universe.csv"
MINIMUM_HISTORY_YEARS = 3
ALLOWED_STRUCTURES = {"open_end", "uit", "grantor_trust", "commodity_pool"}
REQUIRED_COLUMNS = {
    "ticker",
    "name",
    "asset_class",
    "category",
    "benchmark",
    "issuer",
    "structure",
    "inception_date",
    "leveraged",
    "inverse",
    "enabled",
    "reviewed_at",
    "source_url",
}
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
BOOLEAN_VALUES = {
    "true": True,
    "false": False,
    "1": True,
    "0": False,
    "yes": True,
    "no": False,
}


def _parse_boolean(value: str, column: str, ticker: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized not in BOOLEAN_VALUES:
        raise ValueError(f"{ticker}: {column} 값이 올바른 불리언이 아닙니다: {value}")
    return BOOLEAN_VALUES[normalized]


def load_etf_symbols(
    file_path: Path = DEFAULT_ETF_UNIVERSE_FILE,
    as_of: datetime | date | pd.Timestamp | None = None,
    minimum_history_years: int = MINIMUM_HISTORY_YEARS,
) -> list[str]:
    """Return enabled, approved ETPs with enough history for the collection window."""
    if minimum_history_years < 0:
        raise ValueError("minimum_history_years는 0 이상이어야 합니다.")
    if not file_path.is_file():
        raise FileNotFoundError(f"ETF 유니버스 파일이 없습니다: {file_path}")

    dataframe = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    missing_columns = REQUIRED_COLUMNS.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "ETF 유니버스 필수 컬럼이 없습니다: "
            + ", ".join(sorted(missing_columns))
        )
    if dataframe.empty:
        raise ValueError("ETF 유니버스가 비어 있습니다.")

    dataframe = dataframe.copy()
    dataframe["ticker"] = dataframe["ticker"].str.strip().str.upper()
    metadata_columns = (
        "name",
        "asset_class",
        "category",
        "benchmark",
        "issuer",
        "reviewed_at",
        "source_url",
    )
    empty_metadata = dataframe.loc[
        dataframe[list(metadata_columns)].apply(
            lambda column: column.str.strip().eq("")
        ).any(axis=1),
        "ticker",
    ].tolist()
    if empty_metadata:
        raise ValueError(
            "ETF 메타데이터에 빈 값이 있습니다: " + ", ".join(empty_metadata)
        )
    invalid_tickers = [
        ticker
        for ticker in dataframe["ticker"]
        if not TICKER_PATTERN.fullmatch(ticker)
    ]
    if invalid_tickers:
        raise ValueError("ETF 티커 형식이 올바르지 않습니다: " + ", ".join(invalid_tickers))
    duplicate_tickers = sorted(
        dataframe.loc[dataframe["ticker"].duplicated(keep=False), "ticker"].unique()
    )
    if duplicate_tickers:
        raise ValueError("ETF 티커가 중복되었습니다: " + ", ".join(duplicate_tickers))

    for column in ("leveraged", "inverse", "enabled"):
        dataframe[column] = [
            _parse_boolean(value, column, ticker)
            for value, ticker in zip(dataframe[column], dataframe["ticker"])
        ]

    dataframe["structure"] = dataframe["structure"].str.strip().str.lower()
    enabled = dataframe["enabled"]
    invalid_structures = sorted(
        dataframe.loc[
            enabled & ~dataframe["structure"].isin(ALLOWED_STRUCTURES), "ticker"
        ].unique()
    )
    if invalid_structures:
        raise ValueError(
            "활성 ETF에 허용되지 않은 상품 구조가 있습니다: "
            + ", ".join(invalid_structures)
        )

    inception_dates = pd.to_datetime(dataframe["inception_date"], errors="coerce")
    invalid_dates = dataframe.loc[inception_dates.isna(), "ticker"].tolist()
    if invalid_dates:
        raise ValueError(
            "ETF 상장일 형식이 올바르지 않습니다: " + ", ".join(invalid_dates)
        )
    reviewed_dates = pd.to_datetime(dataframe["reviewed_at"], errors="coerce")
    invalid_review_dates = dataframe.loc[reviewed_dates.isna(), "ticker"].tolist()
    if invalid_review_dates:
        raise ValueError(
            "ETF 검토일 형식이 올바르지 않습니다: "
            + ", ".join(invalid_review_dates)
        )

    invalid_sources = dataframe.loc[
        enabled & ~dataframe["source_url"].str.strip().str.startswith("https://"),
        "ticker",
    ].tolist()
    if invalid_sources:
        raise ValueError(
            "활성 ETF의 공식 출처 URL이 올바르지 않습니다: "
            + ", ".join(invalid_sources)
        )

    reference_time = pd.Timestamp(as_of or datetime.now(timezone.utc))
    if reference_time.tzinfo is not None:
        reference_time = reference_time.tz_convert("UTC").tz_localize(None)
    history_cutoff = reference_time - pd.DateOffset(years=minimum_history_years)
    stale_reviews = dataframe.loc[
        enabled & (reviewed_dates < reference_time - pd.Timedelta(days=120)),
        "ticker",
    ].tolist()
    if stale_reviews:
        print(
            "[경고] ETF 유니버스 검토 후 120일이 지났습니다: "
            + ", ".join(stale_reviews)
        )
    eligible = (
        enabled
        & ~dataframe["leveraged"]
        & ~dataframe["inverse"]
        & dataframe["structure"].isin(ALLOWED_STRUCTURES)
        & (inception_dates <= history_cutoff)
    )
    symbols = sorted(dataframe.loc[eligible, "ticker"].tolist())
    if not symbols:
        raise ValueError("활성화된 적격 ETF가 없습니다.")
    return symbols
