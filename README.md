# 미국 주식 데이터 파이프라인

Wikipedia와 Alpaca에서 최근 3년의 S&P 500 관련 종목을 수집하고, 정규장 데이터 분리와 데이터 품질 검사를 수행하는 프로젝트입니다.

## 프로젝트 구조

스크립트는 역할에 따라 세 폴더로 분류합니다. 각 폴더의 자세한 실행법은 해당 README에서 확인할 수 있습니다.

```text
market_hist_script/
├── daily_pipeline.py             # SIP 1분봉 일일 통합 파이프라인
├── pipeline_state.py             # 종목별 실행 체크포인트 관리
├── data_collection/              # API 호출 및 크롤링
│   ├── script.py
│   ├── collect_sip_1min.py
│   ├── get_ticker.py
│   └── README.md
├── data_filtering/               # 정규장 데이터 필터링
│   ├── filter_regular_session.py
│   ├── resample_sip_5min.py
│   └── README.md
├── data_validation/              # 데이터 검사 및 보고서 생성
│   ├── audit_regular_session.py
│   ├── quality_control.py
│   ├── check_data.py
│   ├── data_report.py
│   └── README.md
├── tests/                        # 자동 테스트
├── .env.example                  # Alpaca 인증정보 예시
├── requirements.txt              # Python 의존성
└── README.md
```

| 폴더 | 역할 | 상세 설명 |
| --- | --- | --- |
| `data_collection/` | 티커 크롤링과 Alpaca 5분봉 수집·갱신 | [수집 스크립트 안내](data_collection/README.md) |
| `data_filtering/` | XNYS 일정 기반 정규장 데이터 분리 | [필터링 스크립트 안내](data_filtering/README.md) |
| `data_validation/` | 수집 결과와 정규장 누락 구간 검사 | [검사 스크립트 안내](data_validation/README.md) |

## 실행으로 생성되는 구조

스크립트 위치만 분류했으며 기존 데이터와 보고서의 출력 구조는 유지합니다.

```text
market_hist_script/
├── market_data/                  # Raw 데이터
│   ├── csv/
│   └── parquet/
├── adjust_market_data/           # 배당·분할 반영 데이터
│   ├── csv/
│   └── parquet/
├── sip_market_data/              # SIP 최근 3년 1분봉
│   ├── raw/{csv,parquet}/
│   └── adjusted/{csv,parquet}/
├── regular_market_data/          # 정규장 필터 결과
│   ├── raw/{csv,parquet}/
│   └── adjusted/{csv,parquet}/
├── regular_sip_1min_market_data/ # SIP 1분봉 정규장 필터 결과
│   ├── raw/{csv,parquet}/
│   └── adjusted/{csv,parquet}/
├── regular_sip_5min_market_data/ # 정규장 SIP 1분봉에서 생성한 5분봉
│   ├── raw/{csv,parquet}/
│   └── adjusted/{csv,parquet}/
├── ticker_info/
│   └── sp500_tickers_3years.txt
├── pipeline_state/
│   └── daily_pipeline_state.json # 종목·형식·가격 타입·단계별 체크포인트
└── report/
    ├── data_audit_report.txt
    ├── latest/{format}/          # 마지막 일일 실행 보고서
    │   ├── run_summary.json
    │   ├── pipeline_failures.json
    │   ├── quality_summary.csv
    │   ├── quality_invalid_rows.csv
    │   ├── raw_quality_summary.csv
    │   ├── raw_quality_invalid_rows.csv
    │   ├── 1min_summary.csv
    │   ├── 5min_summary.csv
    │   ├── raw_1min_summary.csv
    │   ├── raw_5min_summary.csv
    │   └── deep_quality/         # 상세 모드에서만 갱신
    ├── history/{session}/{format}/ # 미국 거래일별 일일 실행 보고서
    └── regular_session_audit/
        ├── {type}_{format}_summary.csv
        └── {type}_{format}_missing_intervals.csv
```

| 생성 경로 | 생성 스크립트 | 내용 |
| --- | --- | --- |
| `market_data/` | `data_collection/script.py` | Raw 5분봉 CSV 또는 Parquet |
| `adjust_market_data/` | `data_collection/script.py` | 수정주가 5분봉 CSV 또는 Parquet |
| `ticker_info/` | `data_collection/script.py` | 수집 대상 티커 목록 |
| `sip_market_data/` | `data_collection/collect_sip_1min.py` | SIP 최근 3년 1분봉 Raw/Adjusted 데이터 |
| `regular_market_data/` | `data_filtering/filter_regular_session.py` | 휴장·조기 폐장·서머타임을 반영한 정규장 데이터 |
| `regular_sip_1min_market_data/` | `data_filtering/filter_regular_session.py` | SIP 1분봉의 정규장 필터 결과 |
| `regular_sip_5min_market_data/` | `data_filtering/resample_sip_5min.py` | Raw·Adjusted 정규장 SIP 1분봉에서 각각 집계한 5분봉 |
| `pipeline_state/daily_pipeline_state.json` | `daily_pipeline.py` | 종목·가격 타입별 수집·품질 검사·필터·5분봉 생성 체크포인트와 최근 실행 상태 |
| `report/latest/{format}/` | `daily_pipeline.py` | 마지막 실행의 요약, 실패 목록과 품질 검사 결과 |
| `report/history/{session}/{format}/` | `daily_pipeline.py` | 미국 거래일별로 보관하는 일일 실행 보고서 |
| `report/data_audit_report.txt` | `data_validation/data_report.py` | Raw Parquet 전체 검사 보고서 |
| `report/regular_session_audit/` | `data_validation/audit_regular_session.py` | 종목별 기간·커버리지와 누락 구간 CSV |

## 사전 준비

- Python 3.10 이상
- 유효한 Alpaca Market Data API Key와 Secret Key
- 전체 수집 결과를 저장할 충분한 디스크 공간

### 1. 가상환경 만들기

Windows PowerShell:

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

macOS/Linux:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 2. 의존성 설치하기

```bash
python -m pip install -r requirements.txt
```

주요 의존성은 `alpaca-py`, `pandas`, `pandas-market-calendars`, `pyarrow`, `lxml`, `python-dotenv`입니다.

### 3. Alpaca 인증정보 설정하기

Windows PowerShell:

```powershell
$env:ALPACA_API_KEY = "발급받은_API_KEY"
$env:ALPACA_SECRET_KEY = "발급받은_SECRET_KEY"
```

macOS/Linux:

```bash
export ALPACA_API_KEY="발급받은_API_KEY"
export ALPACA_SECRET_KEY="발급받은_SECRET_KEY"
```

또는 `.env.example`을 `.env`로 복사한 뒤 실제 키를 입력할 수 있습니다. 실제 키가 들어 있는 `.env`는 커밋하지 마세요.

## SIP 1분봉 일일 통합 실행

프로젝트 최상단의 `daily_pipeline.py`는 다음 작업을 순서대로 수행합니다.

1. Wikipedia에서 현재 및 최근 3년 S&P 500 관련 티커 갱신
2. Alpaca SIP Adjusted·Raw 1분봉을 각각 수집 또는 증분 갱신
3. Adjusted·Raw 원본 1분봉의 데이터 품질 검사
4. 두 데이터 타입을 XNYS 캘린더로 각각 정규장 데이터로 분리
5. 두 정규장 1분봉을 종목·거래일별 SIP 5분봉으로 각각 집계
6. 데이터 타입별 1분봉·5분봉 커버리지와 누락 구간 보고서 생성

대화형으로 선택하는 값은 CSV 또는 Parquet 형식뿐입니다. 피드는 SIP, 봉 간격은 1분, 가격 타입은 Adjusted와 Raw 모두, 캘린더는 XNYS로 고정됩니다. 기본 품질 단계는 두 데이터 타입의 중복과 OHLCV 구조만 빠르게 검사하며 누락 구간 API 재요청과 상세 보고서는 생성하지 않습니다.

```bash
python daily_pipeline.py
```

서버나 스케줄러에서는 형식을 명시해 입력 대기 없이 실행합니다.

```bash
python daily_pipeline.py --format parquet
```

전체 3년 누락 구간 상세 보고서와 최근 10거래일 누락 재요청이 필요한 경우에만 상세 모드를 사용합니다.

```bash
python daily_pipeline.py --format parquet --deep-quality
```

종목 파일이 없으면 가장 최근에 완료된 거래 세션을 끝으로 최근 3년 전체를 수집합니다. 기존 파일이 있으면 마지막 타임스탬프 다음 1분부터 추가하고, 데이터는 계속 최근 3년 범위로 유지합니다. 정규장 필터와 5분봉 생성도 전체 3년을 다시 계산하지 않고 마지막 저장 세션을 안전하게 한 번 재계산한 뒤 새 세션만 병합합니다.

Adjusted 데이터는 매 실행마다 최근 10거래일을 다시 조회합니다. 이미 저장된 가격과 재조회 가격이 달라지면 배당·액면분할 등 수정주가 이력 변경으로 판단하여 해당 종목의 최근 3년 원본을 다시 수집하고, 변경 시작 거래일부터 정규장 1분봉과 5분봉을 다시 계산합니다. Raw 데이터는 수정주가 재산정 대상이 아니므로 마지막 저장 시각 다음 1분부터 증분 수집합니다.

기본 품질 단계는 Adjusted·Raw 전체 원본의 타임스탬프 중복, OHLC 관계, 음수 거래량과 `volume=0`을 검사합니다. 구조적으로 잘못된 행이나 중복 타임스탬프가 있는 종목은 해당 데이터 타입의 후속 처리에서 제외하고 파이프라인을 실패 상태로 기록합니다. 정규장 1분봉·5분봉의 기간, 누락 봉 수와 커버리지는 마지막 단계의 요약 보고서에 남기되 개별 누락 구간 수백만 행은 생성하지 않습니다.

`--deep-quality`를 지정하면 기존 상세 검사를 실행합니다. 전체 원본 기간의 정규장 1분 누락 구간을 계산하고 최근 10거래일만 Alpaca에 재요청하며, 복구된 원본은 후속 필터와 5분봉에 즉시 반영합니다. 재요청 후에도 없는 봉은 실제 무거래일 수 있으므로 임의 보간하지 않고 상세 CSV에 남깁니다.

일일 파이프라인 보고서는 `report/latest/{format}/`에 최신본을 덮어쓰고, 같은 내용을 `report/history/{미국 거래일}/{format}/`에도 보관합니다. 실행 날짜가 한국에서는 다음 날이더라도 폴더명은 XNYS 거래 세션 날짜를 사용합니다. 가벼운 요약 이력은 365일 유지하고, `--deep-quality`가 생성하는 대용량 누락 구간 상세 자료는 30일 후 자동 삭제합니다. 같은 거래일에 다시 실행하면 해당 날짜의 이력을 최신 결과로 갱신합니다.

Adjusted·Raw 5분봉은 별도 API 호출 없이 각 데이터 타입의 정규장 1분봉에서 생성합니다. 시가/고가/저가/종가에는 각각 첫 값/최댓값/최솟값/마지막 값을 사용하고, 거래량과 체결 수는 합산하며 VWAP은 거래량 가중 방식으로 다시 계산합니다. 거래가 없어 없는 1분봉은 채우지 않고 각 5분봉의 실제 원천 행 수를 `source_minutes`에 기록합니다.

마지막 수집 시점은 단순한 현재 시각이 아니라 `현재 UTC - 15분` 시점에 이미 폐장한 가장 최근 XNYS 세션입니다. 따라서 주말, 휴장, 조기폐장과 서머타임을 자동 반영하고 장중 실행 시에는 아직 끝나지 않은 당일 세션을 수집하지 않습니다. 서버에서는 미국 정규장 종료 15분 이후에 하루 한 번 실행하는 것을 권장합니다.

종목·형식·가격 타입·단계별 성공 상태는 `pipeline_state/daily_pipeline_state.json`에 매 종목 처리 직후 원자적으로 저장합니다. 기존 Adjusted 체크포인트 이름은 유지하고 Raw 체크포인트에는 `raw_` 접두사를 사용하므로 두 데이터 타입이 서로의 완료 상태를 덮어쓰지 않습니다. 같은 완료 세션을 다시 실행할 때 체크포인트와 결과 파일이 모두 있으면 완료 단계를 건너뜁니다. 실행이 중간에 종료되더라도 다음 실행은 미완료 종목과 단계부터 이어집니다. 상태 파일이 없어도 기존 결과 파일을 기준으로 증분 범위를 다시 계산할 수 있습니다.

Windows에서 백신이나 검색 인덱서가 상태 파일을 순간적으로 점유하는 경우에는 파일 교체를 지수 백오프로 최대 6회 재시도합니다. 각 파이프라인 프로세스는 고유한 임시 상태 파일을 사용하므로 임시 파일명 충돌도 방지합니다.

`daily_pipeline_state.json`은 실행 이력이 아니라 재시작을 위한 현재 체크포인트이므로 날짜별로 복제하지 않고 하나만 유지합니다. 날짜별 실행 이력은 `report/history/`가 담당합니다.

티커 크롤링이 일시적으로 실패하면 정상적인 기존 티커 파일을 유지합니다. 수집 실패 종목은 전체 1차 수집이 끝난 뒤 실패 종목만 최대 3회까지 자동 재시도합니다. 최종 실패는 최신·거래일별 `pipeline_failures.json`과 체크포인트에 기록합니다. 수집 실패 종목이나 검사 오류가 있으면 가능한 파일의 후속 처리는 계속 수행하지만 프로세스는 종료 코드 `1`을 반환하므로 서버 모니터링에서 실패를 감지할 수 있습니다.

## 개별 스크립트 실행 순서

명령은 프로젝트 루트에서 실행하는 것을 권장합니다.

```bash
# 1. 데이터 수집 또는 갱신
python data_collection/script.py

# 2. 정규장 데이터 분리
python data_filtering/filter_regular_session.py

# 3. 종목별 기간과 누락 구간 검사
python data_validation/audit_regular_session.py
```

보조 명령:

```bash
# API 호출 없이 티커 목록 확인
python data_collection/get_ticker.py

# Raw Parquet 간단 검사
python data_validation/check_data.py

# Raw Parquet 텍스트 보고서 생성
python data_validation/data_report.py

# 전체 자동 테스트
python -m unittest discover -s tests -v
```

모든 스크립트는 자신의 파일 위치를 기준으로 프로젝트 루트를 계산하므로, 실행 위치와 관계없이 기존 루트 출력 폴더를 사용합니다.
