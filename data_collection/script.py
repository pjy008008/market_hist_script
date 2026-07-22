import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment  # 🛠️ 수정주가 옵션을 위한 임포트 추가
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# ==========================================
# [설정 영역] 본인의 API 키 지정
# ==========================================
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TICKER_INFO_DIR = PROJECT_ROOT / "ticker_info"   # 티커 목록 저장용 폴더 경로
TICKER_FILE = os.path.join(TICKER_INFO_DIR, "sp500_tickers_10years.txt")
TICKER_LOOKBACK_YEARS = 10

# Alpaca 클라이언트 초기화
if not API_KEY or not SECRET_KEY or API_KEY == "YOUR_ALPACA_API_KEY" or SECRET_KEY == "YOUR_ALPACA_SECRET_KEY":
    print("[Error] Alpaca API 키와 Secret 키를 먼저 입력해 주세요.")
    sys.exit(1)

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)


def choose_storage_format():
    """1번째 선택: 데이터를 저장할 파일 형식을 선택합니다."""
    choices = {
        "": "csv",
        "1": "csv",
        "csv": "csv",
        ".csv": "csv",
        "2": "parquet",
        "parquet": "parquet",
        ".parquet": "parquet",
    }

    print("==========================================")
    print(" [선택 1] 저장 형식을 선택해 주세요.")
    print("  1. CSV (기본값)")
    print("  2. Parquet")
    print("==========================================")

    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""

        if choice in choices:
            return choices[choice]

        print("잘못된 입력입니다. 1(CSV) 또는 2(Parquet)를 입력해 주세요.")


def choose_adjustment_option():
    """2번째 선택: 수정 주가(Adjustment) 반영 여부를 선택합니다."""
    choices = {
        "": False,
        "1": False,
        "raw": False,
        "2": True,
        "adjusted": True,
    }

    print("\n==========================================")
    print(" [선택 2] 주가 데이터 타입을 선택해 주세요.")
    print("  1. Raw 데이터 (배당/분할 미보정 - 기본값)")
    print("  2. Adjusted 데이터 (배당/분할 수정주가 반영)")
    print("==========================================")

    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""

        if choice in choices:
            return choices[choice]

        print("잘못된 입력입니다. 1(Raw) 또는 2(Adjusted)를 입력해 주세요.")


def load_local_data(file_path, storage_format):
    """선택한 저장 형식의 로컬 데이터를 읽고 공통 MultiIndex 구조로 반환합니다."""
    if storage_format == "parquet":
        return pd.read_parquet(file_path)

    df = pd.read_csv(file_path)
    required_index_columns = {"symbol", "timestamp"}
    missing_columns = required_index_columns.difference(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"CSV 인덱스 컬럼이 없습니다: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.set_index(["symbol", "timestamp"])


def save_local_data(df, file_path, storage_format):
    """데이터를 선택한 형식으로 저장합니다."""
    if storage_format == "parquet":
        df.to_parquet(file_path)
    else:
        df.to_csv(file_path, index=True)


def get_sp500_tickers():
    """위키피디아에서 최근 10년간 존재했던 S&P 500 히스토리컬 티커 목록을 가져옵니다."""
    import urllib.request
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    )
    try:
        with urllib.request.urlopen(req) as response:
            tables = pd.read_html(response.read())
            
        # 1. 현재 종목 추출
        df_current = tables[0]
        current_tickers = set(df_current['Symbol'].str.replace('.', '/', regex=False).tolist())
        
        # 2. 최근 10년 내 퇴출된 종목 추출
        df_changes = tables[1].copy()
        if isinstance(df_changes.columns, pd.MultiIndex):
            df_changes.columns = [col[-1].strip() for col in df_changes.columns]
        else:
            df_changes.columns = [str(col).strip() for col in df_changes.columns]
            
        if 'Date' not in df_changes.columns:
            df_changes.rename(columns={df_changes.columns[0]: 'Date'}, inplace=True)
            
        # 타임존 없이 변환 후 결측치 제거
        df_changes['Date'] = pd.to_datetime(df_changes['Date'], errors='coerce')
        df_changes = df_changes.dropna(subset=['Date'])
        
        # 타임존 에러 완벽 해결: pd.Timestamp를 사용하고 tz 정보를 완전히 제거(tz=None)
        history_cutoff = (
            pd.Timestamp(datetime.now()).replace(tzinfo=None)
            - pd.DateOffset(years=TICKER_LOOKBACK_YEARS)
        )
        
        # 데이터프레임의 Date 컬럼도 타임존이 없는 상태로 일치시킨 뒤 비교
        df_changes['Date'] = df_changes['Date'].dt.tz_localize(None)
        recent_changes = df_changes[df_changes['Date'] >= history_cutoff]
        
        removed_tickers = []
        removed_cols = [col for col in recent_changes.columns if 'Removed' in col or 'Ticker' in col]
        
        for col in removed_cols:
            if 'Added' not in col:
                col_data = recent_changes[col]
                col_series = col_data.stack().reset_index(drop=True) if isinstance(col_data, pd.DataFrame) else col_data
                tickers_to_add = col_series.dropna().astype(str).str.replace('.', '/', regex=False).tolist()
                tickers_to_add = [t.strip() for t in tickers_to_add if t.strip() and len(t.strip()) <= 6 and not any(char.isdigit() for char in t)]
                removed_tickers.extend(tickers_to_add)
                
        all_historical_tickers = current_tickers.union(set(removed_tickers))
        final_tickers = {t.upper() for t in all_historical_tickers if t and len(t) <= 6 and (t.isalpha() or '/' in t)}
        
        return sorted(list(final_tickers))
        
    except Exception as e:
        print(f"티커 목록 크롤링 실패, 기본 셋으로 대체합니다: {e}")
        return ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]


def fetch_data_from_alpaca(symbol, start_dt, end_dt, use_adjusted):
    """Alpaca API에서 특정 기간의 5분봉 데이터를 요청합니다 (선택한 수정주가 옵션 적용)."""
    # 선택에 따른 adjustment 옵션 결정
    adj_option = Adjustment.ALL if use_adjusted else Adjustment.RAW

    request_params = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=start_dt,
        end=end_dt,
        adjustment=adj_option  # 🛠️ 동적으로 선택된 옵션 대입
    )
    try:
        bars = client.get_stock_bars(request_params)
        if not bars.data or symbol not in bars.data:
            return pd.DataFrame()
        return bars.df
    except Exception as e:
        print(f"[{symbol}] 데이터 수집 중 오류 발생: {e}")
        return pd.DataFrame()


def process_symbol(symbol, now, storage_format, storage_dir, use_adjusted):
    """개별 종목에 대해 초기화 또는 증분 업데이트를 수행합니다."""
    file_symbol = symbol.replace("/", "-")
    file_path = os.path.join(
        storage_dir,
        f"{file_symbol}_5min_historical.{storage_format}",
    )
    
    # 1. 기존 로컬 데이터 파일 존재 여부 확인
    if os.path.exists(file_path):
        try:
            df_local = load_local_data(file_path, storage_format)
            df_local = df_local.sort_index()
            
            # 마지막 저장 데이터 시각 추출
            last_timestamp = df_local.index.get_level_values('timestamp').max()
            
            if last_timestamp.tzinfo is None:
                last_timestamp = last_timestamp.replace(tzinfo=timezone.utc)
            else:
                last_timestamp = last_timestamp.astimezone(timezone.utc)
                
            start_time = last_timestamp + timedelta(minutes=5)
            end_time = now - timedelta(minutes=30)  # 무료 플랜 15분 지연 제한 회피
            
            if start_time >= end_time - timedelta(minutes=5):
                print(f"[{symbol}] 최신 데이터가 이미 모두 반영되어 있습니다. 건너뜁니다.")
                return

            print(f"[{symbol}] 업데이트 진행 중... ({start_time} ~ {end_time})")
            df_new = fetch_data_from_alpaca(symbol, start_time, end_time, use_adjusted)
            
            if not df_new.empty:
                df_combined = pd.concat([df_local, df_new])
                df_combined = df_combined[~df_combined.index.duplicated(keep='last')].sort_index()
                save_local_data(df_combined, file_path, storage_format)
                print(f"[{symbol}] 업데이트 완료! (+{len(df_new)}행, 총 {len(df_combined)}행)")
            else:
                print(f"[{symbol}] 추가할 최신 데이터가 없습니다.")
                
        except Exception as e:
            print(f"[{symbol}] 파일 읽기/쓰기 오류 발생: {e}")
            
    else:
        # 2. 로컬 파일이 없는 경우 최초 3년 데이터 생성 단계 실행
        print(f"[{symbol}] 신규 종목 감지. 최초 3년치 데이터 수집을 시작합니다...")
        start_time = now - timedelta(days=3*365)
        end_time = now - timedelta(minutes=30)
        
        df_initial = fetch_data_from_alpaca(symbol, start_time, end_time, use_adjusted)
        
        if not df_initial.empty:
            df_initial = df_initial.sort_index()
            save_local_data(df_initial, file_path, storage_format)
            print(f"[{symbol}] 최초 3년치 5분봉 저장 성공! (총 {len(df_initial)}행)")
        else:
            print(f"[{symbol}] 최초 수집 실패 또는 데이터가 존재하지 않습니다.")


def main():
    # 1. 저장 포맷 선택 (CSV / Parquet)
    storage_format = choose_storage_format()
    
    # 2. 수정주가 반영 여부 선택 (Raw / Adjusted)
    use_adjusted = choose_adjustment_option()

    # 3. 선택 조합에 따라 상위 및 하위 저장 폴더 구조 결정
    parent_dir = PROJECT_ROOT / (
        "adjust_market_data" if use_adjusted else "market_data"
    )
    storage_dir = os.path.join(parent_dir, storage_format)
    os.makedirs(storage_dir, exist_ok=True)

    print("\n" + "=" * 50)
    print(f"• 데이터 타입  : {'ADJUSTED (수정주가 반영)' if use_adjusted else 'RAW (미보정)'}")
    print(f"• 파일 형식    : {storage_format.upper()}")
    print(f"• 저장 폴더 경로: {storage_dir}")
    print("=" * 50 + "\n")

    now = datetime.now(timezone.utc)
    
    print("위키피디아에서 S&P 500 히스토리컬 티커 목록 수집 중...")
    tickers = get_sp500_tickers()
    print(f"총 {len(tickers)}개의 종목을 수집/업데이트할 예정입니다.\n")
    
    # 확인용으로 티커 명단을 ticker_info 폴더에 남겨둠
    os.makedirs(TICKER_INFO_DIR, exist_ok=True)
    with open(TICKER_FILE, "w", encoding="utf-8") as f:
        for ticker in tickers:
            f.write(f"{ticker}\n")
    
    # 전 종목 루프 돌며 데이터 다운로드 진행
    for idx, symbol in enumerate(tickers, 1):
        print(f"[{idx}/{len(tickers)}] {symbol} 처리 시작")
        process_symbol(symbol, now, storage_format, storage_dir, use_adjusted)
        
        # API 과부하 및 차단 방지를 위한 미세 딜레이
        time.sleep(0.5)
        print("-" * 50)


if __name__ == "__main__":
    main()
