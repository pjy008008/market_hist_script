# 미국 주식 데이터 파이프라인

Wikipedia와 Alpaca에서 최근 3년의 S&P 500 관련 종목을 수집하고, 정규장 데이터 분리와 데이터 품질 검사를 수행하는 프로젝트입니다.

## 프로젝트 구조

스크립트는 역할에 따라 세 폴더로 분류합니다. 각 폴더의 자세한 실행법은 해당 README에서 확인할 수 있습니다.

```text
script/
├── daily_pipeline.py             # SIP 1분봉 일일 통합 파이프라인
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
script/
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
├── regular_sip_market_data/      # SIP 1분봉 정규장 필터 결과
│   ├── raw/{csv,parquet}/
│   └── adjusted/{csv,parquet}/
├── regular_sip_5min_market_data/ # 정규장 SIP 1분봉에서 생성한 5분봉
│   └── adjusted/{csv,parquet}/
├── ticker_info/
│   └── sp500_tickers_3years.txt
└── report/
    ├── data_audit_report.txt
    ├── regular_sip_session_audit/
    │   ├── 1min/
    │   │   ├── adjusted_{format}_summary.csv
    │   │   └── adjusted_{format}_missing_intervals.csv
    │   └── 5min/
    │       ├── adjusted_{format}_summary.csv
    │       └── adjusted_{format}_missing_intervals.csv
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
| `regular_sip_market_data/` | `data_filtering/filter_regular_session.py` | SIP 1분봉의 정규장 필터 결과 |
| `regular_sip_5min_market_data/` | `data_filtering/resample_sip_5min.py` | 정규장 SIP 1분봉에서 집계한 5분봉 |
| `report/data_audit_report.txt` | `data_validation/data_report.py` | Raw Parquet 전체 검사 보고서 |
| `report/regular_session_audit/` | `data_validation/audit_regular_session.py` | 종목별 기간·커버리지와 누락 구간 CSV |
| `report/regular_sip_session_audit/` | `daily_pipeline.py` | SIP 1분봉·5분봉 기간, 커버리지와 누락 구간 CSV |

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
2. Alpaca SIP Adjusted 1분봉 수집 또는 증분 갱신
3. XNYS 캘린더로 정규장 데이터 분리
4. 정규장 1분봉을 종목·거래일별 SIP 5분봉으로 집계
5. 종목별 1분봉·5분봉 커버리지와 누락 구간 보고서 생성

사용자가 선택하는 값은 CSV 또는 Parquet 형식뿐입니다. 피드는 SIP, 봉 간격은 1분, 가격은 Adjusted, 캘린더는 XNYS로 고정됩니다.

```bash
python daily_pipeline.py
```

서버나 스케줄러에서는 형식을 명시해 입력 대기 없이 실행합니다.

```bash
python daily_pipeline.py --format parquet
```

종목 파일이 없으면 가장 최근에 완료된 거래 세션을 끝으로 최근 3년 전체를 수집합니다. 기존 파일이 있으면 마지막 타임스탬프 다음 1분부터 추가하고, 데이터는 계속 최근 3년 범위로 유지합니다.

5분봉은 별도 API 호출 없이 정규장 1분봉에서 생성합니다. 시가/고가/저가/종가에는 각각 첫 값/최댓값/최솟값/마지막 값을 사용하고, 거래량과 체결 수는 합산하며 VWAP은 거래량 가중 방식으로 다시 계산합니다. 거래가 없어 없는 1분봉은 채우지 않고 각 5분봉의 실제 원천 행 수를 `source_minutes`에 기록합니다.

마지막 수집 시점은 단순한 현재 시각이 아니라 `현재 UTC - 15분` 시점에 이미 폐장한 가장 최근 XNYS 세션입니다. 따라서 주말, 휴장, 조기폐장과 서머타임을 자동 반영하고 장중 실행 시에는 아직 끝나지 않은 당일 세션을 수집하지 않습니다. 서버에서는 미국 정규장 종료 15분 이후에 하루 한 번 실행하는 것을 권장합니다.

티커 크롤링이 일시적으로 실패하면 정상적인 기존 티커 파일을 유지합니다. 수집 실패 종목이나 검사 오류가 있으면 가능한 파일의 필터와 검사는 계속 수행하지만 프로세스는 종료 코드 `1`을 반환하므로 서버 모니터링에서 실패를 감지할 수 있습니다.

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
