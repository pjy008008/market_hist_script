# S&P 500 데이터 수집

## 주요 기능

- Wikipedia의 현재 S&P 500 구성 종목과 최근 편출 이력을 결합해 수집 대상을 만듭니다.
- `BRK.B`와 같은 티커를 Alpaca 형식인 `BRK/B`로 변환합니다.
- Alpaca에서 최근 3년간의 5분봉을 종목별로 수집합니다.
- 기존 Parquet 파일이 있으면 마지막 타임스탬프 다음 5분봉부터 증분 갱신합니다.
- 중복 인덱스를 제거하고 시간순으로 정렬해 저장합니다.
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
 market_data/{TICKER}_5min_historical.parquet
                │
                ▼
       데이터 검사 및 보고서 생성
```

## 프로젝트 구성

| 경로 | 역할 |
| --- | --- |
| `script.py` | 전체 티커 수집, Alpaca 5분봉 최초 수집 및 증분 갱신 |
| `get_ticker.py` | Alpaca 호출 없이 최근 3년의 S&P 500 티커 목록만 확인 |
| `check_data.py` | 로컬 Parquet 전체 검사 후 요약과 앞 20개 종목을 터미널에 출력 |
| `data_report.py` | 전 종목 검사 결과와 백테스트 가이드를 `data_audit_report.txt`로 생성 |
| `.env.example` | Alpaca 인증정보 형식 예시 |
| `requirements.txt` | 머신별 가상환경 경로 없이 정리한 프로젝트 런타임 의존성 |
| `market_data/` | 생성된 종목별 Parquet 데이터. Git 추적 제외 |
| `sp500_tickers_3years.txt` | 마지막 실행에서 만든 수집 대상 티커 목록. Git 추적 제외 |
| `data_audit_report.txt` | `data_report.py`가 생성하는 상세 검사 보고서. Git 추적 제외 |

## 사전 준비

- Python 3.10 권장
- 인터넷 연결
- 유효한 Alpaca 계정과 Market Data API Key/Secret Key
- 전체 수집 결과를 저장할 충분한 디스크 공간

전체 S&P 500과 최근 편출 종목을 3년치 5분봉으로 저장하므로 최초 실행은 오래 걸릴 수 있고 결과는 GB 단위가 될 수 있습니다. 사용 가능한 과거 데이터와 실시간 지연 범위는 Alpaca 계정의 데이터 구독 조건에 따라 달라질 수 있습니다.

## 빠른 시작

아래 명령은 모두 `script.py`가 있는 프로젝트 루트에서 실행하세요. 데이터와 보고서 경로가 현재 작업 디렉터리를 기준으로 계산되기 때문에 다른 위치에서 실행하면 그 위치에 별도의 `market_data/`가 만들어질 수 있습니다.

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
- `pyarrow`: Parquet 읽기와 쓰기
- `lxml`: `pandas.read_html()`의 Wikipedia 표 파싱

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

> `.env.example`을 복사해 `.env`를 만들 수는 있지만, 현재 `script.py`는 `.env` 파일을 자동으로 읽지 않습니다. 따라서 위처럼 환경변수를 셸에 직접 설정하거나, 실행 환경에서 `.env` 로딩을 별도로 구성해야 합니다. 실제 API 키가 들어 있는 `.env` 파일은 커밋하지 마세요.

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

실행 시 다음 작업이 순서대로 수행됩니다.

1. Wikipedia에서 현재 S&P 500 종목과 최근 3년 내 편출 종목을 읽습니다.
2. 최종 티커 목록을 `sp500_tickers_3years.txt`에 저장합니다.
3. 각 티커의 `market_data/{TICKER}_5min_historical.parquet` 파일을 확인합니다.
4. 파일이 없으면 현재 UTC 시각 기준 최근 3년치를 요청합니다.
5. 파일이 있으면 저장된 마지막 시각의 5분 뒤부터 증분 데이터를 요청합니다.
6. 중복 인덱스를 제거하고 정렬한 결과를 같은 파일에 저장합니다.

수집 종료 시점은 코드상 `현재 UTC 시각 - 30분`입니다. 이는 무료 플랜의 지연 제한을 피하기 위한 여유값입니다. 종목별 요청 사이에는 API 과부하 방지를 위해 0.5초의 대기 시간이 있습니다.

### 6. 수집 결과 검증하기

터미널에서 전체 요약과 앞 20개 종목을 확인합니다.

```bash
python check_data.py
```

전 종목 상세 내역과 백테스트 참고사항을 파일로 남깁니다.

```bash
python data_report.py
```

두 스크립트 모두 `market_data/*_5min_historical.parquet`를 전부 읽습니다. 데이터가 많으면 검사에도 메모리와 시간이 필요하며, `data_report.py`는 기존 `data_audit_report.txt`를 새 결과로 덮어씁니다.

## 저장 데이터 형식

파일명 형식:

```text
market_data/{TICKER}_5min_historical.parquet
```

Alpaca의 `bars.df`를 그대로 저장하므로 데이터는 일반적으로 다음 구조를 가집니다.

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

간단한 읽기 예시:

```python
import pandas as pd

df = pd.read_parquet("market_data/AAPL_5min_historical.parquet")
df = df.sort_index()

print(df.index.names)
print(df.columns)
print(df.head())
```

미국 정규장 데이터만 사용할 경우 타임스탬프를 고정된 UTC 오프셋으로 계산하지 말고 뉴욕 시간대로 변환해야 합니다. 그래야 서머타임이 자동 반영됩니다.

```python
timestamps = df.index.get_level_values("timestamp")
new_york_time = timestamps.tz_convert("America/New_York")

minutes = new_york_time.hour * 60 + new_york_time.minute
regular_mask = (
    (new_york_time.weekday < 5)
    & (minutes >= 9 * 60 + 30)
    & (minutes < 16 * 60)
)

regular_market_df = df[regular_mask]
```