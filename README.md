# 미국 주식 10년 장기봉 파이프라인

기존 종목 유니버스 정책을 유지하면서 Alpaca SIP의 최근 10년 데이터를 정규장 기준 `1시간봉`, `4시간봉`, `일봉`으로 수집·갱신하는 프로젝트입니다.

## 현재 통합 파이프라인

`daily_pipeline.py`는 다음 순서로 실행됩니다.

1. Wikipedia에서 현재 및 최근 10년 S&P 500 편출입 관련 티커를 갱신
2. `ticker_info/etf_universe.csv`의 검토된 ETF·ETP 27개를 병합
3. 각 종목의 Alpaca SIP 30분봉을 Raw와 Adjusted로 조회
4. XNYS 캘린더로 정규장 30분봉만 선택
5. 개장 시각에 맞춘 1시간봉·4시간봉·일봉을 생성
6. 결과 파일과 종목별 체크포인트를 저장
7. OHLCV 구조와 기간·커버리지를 검사하고 보고서 생성

30분봉은 집계를 위한 임시 입력입니다. 디스크에는 저장하지 않으며 1분봉·5분봉·15분봉도 새 통합 파이프라인에서 만들지 않습니다.

## 데이터 범위

- 가격 데이터: 최근 10년의 롤링 윈도우
- 피드: Alpaca SIP
- 가격 타입: Adjusted와 Raw 모두
- 출력 주기: 1시간, 4시간, 1일
- 거래 세션: XNYS 정규장
- 종목 정책: 현재·최근 10년 S&P 500 관련 종목 + ETF·ETP 27개

Alpaca 미국 주식 과거 데이터는 2016년부터 제공됩니다. 따라서 실행일 기준 10년 시작점이 2016년보다 빠르면 실제 결과는 Alpaca에 존재하는 최초 데이터부터 시작합니다. 신규 상장 종목은 상장일 이후 데이터만 생성됩니다.

## 출력 구조

```text
market_hist_script/
├── regular_sip_1hour_market_data/
│   ├── adjusted/{csv,parquet}/
│   └── raw/{csv,parquet}/
├── regular_sip_4hour_market_data/
│   ├── adjusted/{csv,parquet}/
│   └── raw/{csv,parquet}/
├── regular_sip_1day_market_data/
│   ├── adjusted/{csv,parquet}/
│   └── raw/{csv,parquet}/
├── ticker_info/
│   ├── sp500_tickers_10years.txt
│   └── etf_universe.csv
├── pipeline_state/
│   ├── daily_pipeline_state.json
│   └── inactive_symbols.json
└── report/
    ├── latest/{format}/
    └── history/{XNYS-session-date}/{format}/
```

파일명 예시:

```text
AAPL_1hour_sip_historical.parquet
AAPL_4hour_sip_historical.parquet
AAPL_1day_sip_historical.parquet
```

각 파일은 `symbol`, `timestamp` MultiIndex와 OHLCV, `trade_count`, `vwap`, `source_minutes`를 사용합니다. `source_minutes`는 결과 봉에 실제로 포함된 정규장 분량입니다.

정상 거래일의 예상 분량:

- 1시간봉: `60, 60, 60, 60, 60, 60, 30`
- 4시간봉: `240, 150`
- 일봉: `390`

조기 폐장일은 실제 세션 길이에 맞게 짧아지며 무거래 원본 봉은 임의로 채우지 않습니다.

## 설치

Python 3.10 이상을 권장합니다.

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

`.env.example`을 `.env`로 복사하고 Alpaca 키를 설정합니다.

## 실행

```powershell
python daily_pipeline.py --format parquet
```

```powershell
python daily_pipeline.py --format csv
```

전체 기간의 누락 구간 상세 CSV까지 생성:

```powershell
python daily_pipeline.py --format parquet --deep-quality
```

기본 검사는 빠른 요약 모드입니다. `--deep-quality`는 누락 구간을 보고하지만 무거래 구간을 인위적으로 채우지는 않습니다.

## 증분 갱신과 중단 재개

- 최초 실행은 종목별 최근 10년을 180일 단위로 조회합니다.
- 이후 실행은 마지막 출력 세션부터 다시 조회해 최신 세션을 교체합니다.
- Adjusted 데이터는 최근 10거래일을 항상 재조회합니다.
- 수정주가 이력 변경이 발견되면 해당 종목의 최근 10년 전체를 다시 계산합니다.
- 종목 하나가 실패하면 최대 3회 재시도합니다.
- 종목별 성공 상태는 `pipeline_state/daily_pipeline_state.json`에 즉시 저장됩니다.
- 재실행 시 같은 대상 세션에 대해 세 출력 파일이 모두 존재하는 완료 종목은 건너뜁니다.
- 상태 파일과 결과 파일은 임시 파일을 완성한 뒤 교체합니다.

## 보고서

`report/latest/{format}/`과 거래일별 이력 폴더에 다음 파일이 저장됩니다.

```text
run_summary.json
pipeline_failures.json
1hour_summary.csv
4hour_summary.csv
1day_summary.csv
raw_1hour_summary.csv
raw_4hour_summary.csv
raw_1day_summary.csv
```

`--deep-quality` 사용 시 `deep_quality/` 아래에 주기별 누락 구간 CSV가 추가됩니다.

## 코드 구조

| 파일 | 역할 |
| --- | --- |
| `daily_pipeline.py` | 10년 장기봉 통합 실행과 체크포인트·보고서 관리 |
| `data_collection/collect_sip_long_term.py` | 임시 30분봉 조회와 1시간·4시간·일봉 갱신 |
| `data_collection/get_ticker.py` | 현재 및 최근 10년 S&P 500 종목 정책 |
| `data_collection/etf_universe.py` | ETF·ETP 유니버스 검증 |
| `data_filtering/filter_regular_session.py` | XNYS 정규장 필터링 |
| `data_filtering/resample_sip_5min.py` | 세션 개장 기준 공통 봉 집계 함수 |
| `data_validation/audit_regular_session.py` | 기간·커버리지·누락 구간 계산 |
| `pipeline_state.py` | 원자적 종목별 체크포인트 |
| `pipeline_reporting.py` | 최신 및 거래일별 보고서 저장 |

## 기존 데이터와 보조 스크립트

기존 `sip_market_data`, `regular_sip_1min_market_data`, `regular_sip_5min_market_data` 데이터는 자동으로 삭제하지 않습니다. 새 통합 파이프라인은 해당 폴더를 읽거나 갱신하지 않습니다.

기존 1분봉 수집기와 5분봉 필터·집계 스크립트도 독립 실행용으로 남겨 두었지만 `daily_pipeline.py`의 실행 경로에는 포함되지 않습니다.

## 테스트

```powershell
python -m unittest discover -s tests -v
```
