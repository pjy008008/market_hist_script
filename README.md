# S&P 500 데이터 수집

## 주요 기능

- Wikipedia의 현재 S&P 500 구성 종목과 최근 편출 이력을 결합해 수집 대상을 만듭니다.
- `BRK.B`와 같은 티커를 Alpaca 형식인 `BRK/B`로 변환합니다.
- Alpaca에서 최근 3년간의 5분봉을 종목별로 수집합니다.
- 실행할 때 CSV 또는 Parquet 저장 형식을 선택할 수 있습니다.
- Raw 데이터 또는 배당·분할을 반영한 수정주가 데이터를 선택할 수 있습니다.
- 선택한 형식의 기존 파일이 있으면 마지막 타임스탬프 다음 5분봉부터 증분 갱신합니다.
- 중복 인덱스를 제거하고 시간순으로 정렬해 저장합니다.
- XNYS 공식 거래소 일정으로 휴장일, 조기 폐장, 서머타임을 반영해 정규장 데이터만 별도 저장합니다.
- 종목별 데이터 기간, 행 수, 미국 정규장 데이터 비율을 검사합니다.
- 전 종목 상세 결과와 백테스트 참고사항을 텍스트 보고서로 생성합니다.

## 처리 흐름

```text
Wikipedia S&P 500 목록/변경 이력
                │
                ▼
     최근 3년간의 티커 목록 생성
                │
                ▼
       Alpaca 5분봉 데이터 요청
                │
          ┌─────┴─────┐
          │           │
    기존 파일 없음   기존 파일 있음
      3년치 수집      마지막 시점부터 갱신
          │           │
          └─────┬─────┘
                ▼
      데이터 타입/저장 형식 선택
          ┌─────┴─────┐
          │           │
 Raw: market_data/  Adjusted: adjust_market_data/
          │           │
          └─────┬─────┘
                ▼
          csv/ 또는 parquet/
                │
                ▼
          ┌─────┴─────┐
          │           │
    정규장 필터링    데이터 검사
          │           │
          ▼           ▼
 regular_market_data/ 보고서 생성
```

## 프로젝트 구성

| 경로 | 역할 |
| --- | --- |
| `script.py` | 전체 티커 수집, Raw/수정주가 5분봉 최초 수집 및 증분 갱신 |
| `filter_regular_session.py` | XNYS 공식 일정으로 정규장 봉만 필터링해 별도 폴더에 저장 |
| `audit_regular_session.py` | 정규장 결과의 종목별 시작·종료일, 커버리지와 연속 누락 구간을 CSV로 보고 |
| `get_ticker.py` | Alpaca 호출 없이 최근 3년의 S&P 500 티커 목록만 확인 |
| `check_data.py` | `market_data/parquet/`의 로컬 Parquet 전체 검사 후 요약과 앞 20개 종목을 터미널에 출력 |
| `data_report.py` | 전 종목 검사 결과와 백테스트 가이드를 `report/data_audit_report.txt`로 생성 |
| `.env.example` | Alpaca 인증정보 형식 예시 |
| `requirements.txt` | 머신별 가상환경 경로 없이 정리한 프로젝트 런타임 의존성 |

## 사전 준비

- Python 3.10 권장
- 유효한 Alpaca 계정과 Market Data API Key/Secret Key
- 전체 수집 결과를 저장할 충분한 디스크 공간

## 빠른 시작

아래 명령은 모두 `script.py`가 있는 프로젝트 루트에서 실행하세요. 데이터와 보고서 경로가 현재 작업 디렉터리를 기준으로 계산되기 때문에 다른 위치에서 실행하면 그 위치에 별도의 `market_data/` 또는 `adjust_market_data/`가 만들어질 수 있습니다.

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

`requirements.txt`에는 프로젝트가 직접 사용하는 패키지만 기록되어 있으며, 특정 컴퓨터의 가상환경이나 Conda 빌드 경로는 포함하지 않습니다. 나머지 하위 의존성은 `pip`가 자동으로 설치합니다.

```bash
python -m pip install -r requirements.txt
```

각 패키지의 용도는 다음과 같습니다.

- `alpaca-py`: Alpaca 과거 주가 API 호출
- `pandas`: 티커 테이블 파싱 및 시계열 처리
- `pandas-market-calendars`: XNYS 거래일, 정규 개장·폐장, 휴장 및 조기 폐장 일정 계산
- `pyarrow`: Parquet 형식을 선택했을 때의 읽기와 쓰기
- `lxml`: `pandas.read_html()`의 Wikipedia 표 파싱
- `python-dotenv`: 프로젝트 루트의 `.env` 파일에서 Alpaca 인증정보 로드

### 3. Alpaca 인증정보 설정하기

Alpaca 대시보드에서 발급한 값을 현재 터미널 세션의 환경변수로 지정합니다.

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

`script.py`는 프로젝트 루트의 `.env` 파일을 자동으로 읽습니다. `.env.example`을 `.env`로 복사한 뒤 실제 API 키를 입력해도 되고, 위처럼 현재 셸의 환경변수로 설정해도 됩니다. 셸 환경변수와 `.env`에 같은 이름이 있으면 기존 셸 환경변수가 우선합니다. 실제 API 키가 들어 있는 `.env` 파일은 커밋하지 마세요.

### 4. 티커 목록만 미리 확인하기

API 데이터 수집 전에 Wikipedia 파싱 결과를 빠르게 확인할 수 있습니다.

```bash
python get_ticker.py
```

Wikipedia 접근이나 표 파싱에 실패하면 코드에 정의된 7개 기본 티커(`AAPL`, `MSFT`, `AMZN`, `NVDA`, `GOOGL`, `META`, `TSLA`)를 대신 사용합니다.

### 5. 데이터 수집 또는 갱신하기

```bash
python script.py
```

실행하면 저장 형식과 데이터 타입을 차례로 묻습니다. 저장 형식은 `1` 또는 Enter를 입력하면 CSV, `2`를 입력하면 Parquet입니다. 데이터 타입은 `1` 또는 Enter를 입력하면 Raw, `2`를 입력하면 배당·분할을 반영한 Adjusted 데이터입니다.

```text
 [선택 1] 저장 형식을 선택해 주세요.
  1. CSV (기본값)
  2. Parquet
선택 [1/2]:

 [선택 2] 주가 데이터 타입을 선택해 주세요.
  1. Raw 데이터 (배당/분할 미보정 - 기본값)
  2. Adjusted 데이터 (배당/분할 수정주가 반영)
선택 [1/2]:
```

실행 시 다음 작업이 순서대로 수행됩니다.

1. 선택한 데이터 타입과 형식에 맞춰 `market_data/{csv,parquet}/` 또는 `adjust_market_data/{csv,parquet}/` 폴더를 생성합니다.
2. Wikipedia에서 현재 S&P 500 종목과 최근 3년 내 편출 종목을 읽습니다.
3. `ticker_info/` 폴더를 자동 생성하고 최종 티커 목록을 `ticker_info/sp500_tickers_3years.txt`에 저장합니다.
4. 선택한 형식의 종목 파일을 확인합니다.
5. 파일이 없으면 현재 UTC 시각 기준 최근 3년치를 요청합니다.
6. 파일이 있으면 저장된 마지막 시각의 5분 뒤부터 증분 데이터를 요청합니다.
7. 중복 인덱스를 제거하고 정렬한 결과를 같은 파일에 저장합니다.

Raw와 Adjusted 데이터, CSV와 Parquet은 각각 독립적으로 관리됩니다. Raw 데이터는 `market_data/`, Adjusted 데이터는 `adjust_market_data/` 아래에 저장되며, 다른 데이터 타입이나 형식의 기존 파일을 변환하지 않고 선택한 경로에서 새로 수집합니다. 이전 버전에서 `market_data/` 바로 아래에 저장한 Parquet 파일도 자동 이동하거나 변환하지 않습니다.

수집 종료 시점은 코드상 `현재 UTC 시각 - 30분`입니다. 이는 무료 플랜의 지연 제한을 피하기 위한 여유값입니다. 종목별 요청 사이에는 API 과부하 방지를 위해 0.5초의 대기 시간이 있습니다.

### 6. 정규장 데이터만 분리하기

수집한 Raw/Adjusted 데이터에서 미국 주식 정규장에 포함되는 5분봉만 별도 폴더로 복사합니다.

```bash
python filter_regular_session.py
```

기본 실행은 데이터 타입과 파일 형식을 차례로 묻습니다.

```text
 [선택 1] 처리할 데이터 타입을 선택해 주세요.
  1. Raw 데이터 (기본값)
  2. Adjusted 데이터 (배당/분할 수정주가 반영)
선택 [1/2]:

 [선택 2] 처리할 파일 형식을 선택해 주세요.
  1. CSV (기본값)
  2. Parquet
선택 [1/2]:
```

각 단계에서 Enter를 누르면 기본값이 선택됩니다. 예를 들어 `2`를 두 번 입력하면 Adjusted Parquet만 처리하고, Enter를 두 번 누르면 Raw CSV만 처리합니다. 선택한 입력 폴더나 파일이 없으면 해당 조합을 건너뜁니다. 결과는 다음 구조 중 선택한 위치에 저장됩니다.

```text
regular_market_data/
├── raw/
│   ├── csv/
│   └── parquet/
└── adjusted/
    ├── csv/
    └── parquet/
```

자동화하거나 메뉴 입력을 생략하려면 기존 명령행 옵션을 지정할 수도 있습니다. 한 옵션만 지정하면 나머지 항목만 실행 중에 묻습니다.

```bash
# Raw CSV만 처리
python filter_regular_session.py --data-type raw --format csv

# Adjusted Parquet만 원하는 폴더에 저장
python filter_regular_session.py \
  --data-type adjusted \
  --format parquet \
  --output-dir ./my_regular_data
```

Windows PowerShell에서도 여러 줄 명령 대신 위 옵션을 한 줄로 이어서 실행하면 됩니다. 전체 옵션은 다음 명령으로 확인할 수 있습니다.

```bash
python filter_regular_session.py --help
```

기본 캘린더는 `XNYS`입니다. 각 봉의 시작 시각이 해당 거래일의 `market_open` 이상이고 `market_close` 미만일 때만 보존합니다. 따라서 다음 항목이 함께 반영됩니다.

- 미국 동부 시간의 서머타임 전환
- 주말과 미국 주식시장 휴장일
- 독립기념일 전날 등 거래소의 조기 폐장
- 캘린더에 등록된 임시 휴장 및 특별 거래 일정

같은 파일을 다시 처리하면 결과 파일을 안전하게 교체하므로, 원본 데이터를 갱신한 뒤 명령을 다시 실행하면 됩니다. 원본 `market_data/`와 `adjust_market_data/` 파일은 변경하지 않습니다.

`pandas-market-calendars`의 일정 규칙은 설치된 패키지에 포함되어 있으며 실행할 때 서버에서 갱신하지 않습니다. 새로 발표되거나 정정된 특별 거래 일정을 반영하려면 의존성을 최신 버전으로 갱신한 뒤 다시 필터링하세요.

### 7. 정규장 데이터 기간과 누락 구간 검사하기

`filter_regular_session.py`로 생성한 데이터에서 종목별 시작·종료일과 빠진 5분봉을 검사합니다.

```bash
python audit_regular_session.py
```

필터 스크립트와 마찬가지로 데이터 타입(Raw/Adjusted)과 형식(CSV/Parquet)을 차례로 선택합니다. 자동 실행 시에는 옵션을 지정할 수 있습니다.

```bash
python audit_regular_session.py --data-type adjusted --format parquet
```

검사 결과는 기본적으로 `report/regular_session_audit/`에 저장됩니다. 예를 들어 Adjusted Parquet을 선택하면 다음 두 파일이 생성됩니다.

```text
report/regular_session_audit/adjusted_parquet_summary.csv
report/regular_session_audit/adjusted_parquet_missing_intervals.csv
```

`*_summary.csv`의 주요 열은 다음과 같습니다.

| 열 | 설명 |
| --- | --- |
| `first_timestamp_utc`, `last_timestamp_utc` | 실제 관측된 첫 봉과 마지막 봉의 UTC 시각 |
| `first_session_date`, `last_session_date` | XNYS 현지 기준 첫 거래일과 마지막 거래일 |
| `observed_rows`, `unique_timestamps` | 전체 행 수와 중복을 제거한 타임스탬프 수 |
| `expected_bars` | 관측 시작과 종료 사이에 XNYS 일정상 기대되는 5분봉 수 |
| `missing_bars` | 기대 타임스탬프 중 실제 데이터에 없는 봉 수 |
| `coverage_pct` | `관측된 기대 봉 / 전체 기대 봉 × 100` |
| `missing_intervals` | 같은 거래일 안에서 연속된 누락 구간 수 |
| `duplicate_timestamps` | 중복 타임스탬프 수 |
| `unexpected_timestamps` | XNYS 5분 간격에 포함되지 않는 타임스탬프 수 |

`*_missing_intervals.csv`에는 누락 시작·종료 시각, 누락 봉 수와 분량, 누락 직전·직후 봉이 기록됩니다. 서로 다른 거래일의 누락은 하나의 구간으로 합치지 않습니다.

기대 구간은 각 종목에서 실제로 관측된 첫 봉부터 마지막 봉까지만 계산합니다. 따라서 신규 상장이나 상장폐지 전후 기간을 누락으로 만들지 않으며, 종목 간 전체 데이터 기간 차이는 `first_session_date`와 `last_session_date`로 비교해야 합니다. 이 스크립트는 누락 봉을 채우거나 원본 파일을 변경하지 않습니다.

### 8. 기존 수집 결과 검증하기

터미널에서 전체 요약과 앞 20개 종목을 확인합니다.

```bash
python check_data.py
```

전 종목 상세 내역과 백테스트 참고사항을 파일로 남깁니다.

```bash
python data_report.py
```

현재 두 검사 스크립트는 Raw Parquet 전용이며 `market_data/parquet/*_5min_historical.parquet`를 전부 읽습니다. CSV와 `adjust_market_data/`에 저장한 데이터는 이 검사 대상에 포함되지 않습니다. 데이터가 많으면 검사에도 메모리와 시간이 필요하며, `data_report.py`는 `report/` 폴더를 자동 생성하고 기존 `report/data_audit_report.txt`를 새 결과로 덮어씁니다.

## 저장 데이터 형식

파일명 형식은 선택한 데이터 타입과 저장 방식에 따라 다음과 같습니다.

```text
market_data/csv/{TICKER}_5min_historical.csv
market_data/parquet/{TICKER}_5min_historical.parquet
adjust_market_data/csv/{TICKER}_5min_historical.csv
adjust_market_data/parquet/{TICKER}_5min_historical.parquet
regular_market_data/raw/csv/{TICKER}_5min_historical.csv
regular_market_data/raw/parquet/{TICKER}_5min_historical.parquet
regular_market_data/adjusted/csv/{TICKER}_5min_historical.csv
regular_market_data/adjusted/parquet/{TICKER}_5min_historical.parquet
```

파일 경로에서 `/`가 하위 폴더 구분자로 해석되지 않도록 `BRK/B`와 같은 Alpaca 티커는 `BRK-B_5min_historical.csv` 또는 `.parquet`이라는 이름으로 저장합니다. 파일 내부의 `symbol` 값은 원래 Alpaca 티커인 `BRK/B`로 유지됩니다.

Alpaca의 `bars.df` 구조를 유지해 저장하므로 데이터는 일반적으로 다음 구성을 가집니다. CSV에서는 `symbol`과 `timestamp`가 일반 열로 기록되고, 아래 읽기 예시에서 다시 인덱스로 복원합니다.

| 구분 | 이름 | 설명 |
| --- | --- | --- |
| 인덱스 | `symbol` | 종목 티커 |
| 인덱스 | `timestamp` | 봉 시작 시각, UTC 기준의 타임존 포함 시각 |
| 열 | `open` | 시가 |
| 열 | `high` | 고가 |
| 열 | `low` | 저가 |
| 열 | `close` | 종가 |
| 열 | `volume` | 거래량 |
| 열 | `trade_count` | 해당 봉의 거래 건수 |
| 열 | `vwap` | 거래량 가중 평균 가격 |

Parquet 읽기 예시:

```python
import pandas as pd

df = pd.read_parquet("market_data/parquet/AAPL_5min_historical.parquet")
df = df.sort_index()

print(df.index.names)
print(df.columns)
print(df.head())
```

CSV 읽기 예시:

```python
import pandas as pd

df = pd.read_csv("market_data/csv/AAPL_5min_historical.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.set_index(["symbol", "timestamp"]).sort_index()

print(df.index.names)
print(df.columns)
print(df.head())
```

미국 정규장 데이터는 `filter_regular_session.py`로 생성하세요. 고정 UTC 오프셋이나 평일 09:30~16:00 조건만 사용하면 서머타임뿐 아니라 휴장일과 조기 폐장을 정확히 반영할 수 없습니다.
