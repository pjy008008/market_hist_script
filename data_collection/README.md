# API 호출 및 크롤링

티커 목록을 크롤링하고 Alpaca에서 5분봉 또는 SIP 1분봉을 수집·갱신하는 스크립트가 있습니다.

## 스크립트

### `get_ticker.py`

Wikipedia의 현재 S&P 500 구성 종목과 최근 3년의 편출 이력을 결합해 티커 목록을 터미널에 출력합니다. 파일을 생성하지 않으며, 크롤링 실패 시 기본 7개 티커를 표시합니다.

```bash
python data_collection/get_ticker.py
```

### `script.py`

저장 형식(CSV/Parquet)과 데이터 타입(Raw/Adjusted)을 선택한 뒤 Alpaca에서 최근 3년의 5분봉을 종목별로 수집합니다. 기존 파일이 있으면 마지막 타임스탬프 다음부터 증분 갱신합니다.

```bash
python data_collection/script.py
```

### `collect_sip_1min.py`

Alpaca SIP 피드를 명시해 현재 UTC 시각 15분 전까지의 최근 3년 1분봉을 수집합니다. Raw/Adjusted와 CSV/Parquet을 차례로 선택하며, 7일 단위로 요청하고 종목별 완료 시점에 안전하게 저장합니다. 기존 파일이 있으면 마지막 1분봉 다음부터 증분 갱신하고 3년 범위를 벗어난 과거 행은 제거합니다.

통합 실행인 `daily_pipeline.py`는 Adjusted 데이터의 최근 10거래일을 매일 다시 조회합니다. 저장된 가격이 바뀌면 수정주가 이력 변경으로 판단하여 해당 종목의 3년 원본을 재수집하고 후속 정규장·5분봉 결과도 다시 계산합니다. 독립 실행인 `collect_sip_1min.py`는 기존과 같이 마지막 1분봉 이후만 증분 수집합니다.

```bash
python data_collection/collect_sip_1min.py
```

먼저 AAPL 한 종목으로 확인하려면 다음처럼 실행합니다.

```bash
python data_collection/collect_sip_1min.py --data-type raw --format parquet --symbols AAPL
```

전체 S&P 500 관련 종목의 3년치 1분봉은 API 요청 수, 실행 시간과 저장 공간이 매우 큽니다. 작은 종목 목록으로 먼저 검증한 뒤 전체 수집을 권장합니다.

수집한 SIP 1분봉에서 정규장 데이터만 분리하려면 다음 명령을 실행하고 SIP 데이터셋을 선택합니다.

```bash
python data_filtering/filter_regular_session.py --dataset sip
```

## 생성되는 폴더와 파일

| 선택 | 생성 경로 |
| --- | --- |
| Raw CSV | `market_data/csv/{TICKER}_5min_historical.csv` |
| Raw Parquet | `market_data/parquet/{TICKER}_5min_historical.parquet` |
| Adjusted CSV | `adjust_market_data/csv/{TICKER}_5min_historical.csv` |
| Adjusted Parquet | `adjust_market_data/parquet/{TICKER}_5min_historical.parquet` |
| 공통 | `ticker_info/sp500_tickers_3years.txt` |
| SIP Raw | `sip_market_data/raw/{csv,parquet}/{TICKER}_1min_sip_historical.*` |
| SIP Adjusted | `sip_market_data/adjusted/{csv,parquet}/{TICKER}_1min_sip_historical.*` |

Alpaca 인증정보는 프로젝트 루트의 `.env` 또는 환경변수에서 읽습니다.
