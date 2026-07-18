"""위키피디아에서 S&P 500 티커를 확인하는 스크립트."""

import pandas as pd
from datetime import datetime, timedelta
import urllib.request

def get_historical_sp500_tickers(years=3):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    # 403 Forbidden 우회를 위해 브라우저 정보 설정
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            html_content = response.read()
            tables = pd.read_html(html_content)
    except Exception as e:
        print(f"위키피디아 접속 실패: {e}")
        return ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]
    
    # 1. 첫 번째 테이블: 현재 구성 종목
    df_current = tables[0]
    current_tickers = set(df_current['Symbol'].str.replace('.', '/', regex=False).tolist())
    
    # 2. 두 번째 테이블: 구성 종목 변경 이력
    df_changes = tables[1].copy()
    
    # Multi-Index 컬럼을 직관적인 단일 레벨로 전환
    if isinstance(df_changes.columns, pd.MultiIndex):
        df_changes.columns = [col[-1].strip() for col in df_changes.columns]
    else:
        df_changes.columns = [str(col).strip() for col in df_changes.columns]
        
    # 'Date' 컬럼 확보 및 날짜 변환
    if 'Date' not in df_changes.columns:
        df_changes.rename(columns={df_changes.columns[0]: 'Date'}, inplace=True)
    
    df_changes['Date'] = pd.to_datetime(df_changes['Date'], errors='coerce')
    df_changes = df_changes.dropna(subset=['Date'])
    
    # 최근 3년 기준일 계산
    three_years_ago = datetime.now() - timedelta(days=years * 365)
    recent_changes = df_changes[df_changes['Date'] >= three_years_ago]
    
    # 편출(Removed)된 종목들 확보
    removed_tickers = []
    
    # 'Removed' 관련 열들을 모두 수색
    removed_cols = [col for col in recent_changes.columns if 'Removed' in col or 'Ticker' in col]
    
    for col in removed_cols:
        if 'Added' not in col:
            # ⚠️ 에러 해결 핵심: DataFrame/Series 구분 없이 1차원 Series로 강제 변환
            col_data = recent_changes[col]
            if isinstance(col_data, pd.DataFrame):
                # 동일한 이름의 컬럼이 여러 개일 경우 1차원으로 병합
                col_series = col_data.stack().reset_index(drop=True)
            else:
                col_series = col_data
            
            # 문자열 정제 및 슬래시 포맷 변환
            tickers_to_add = col_series.dropna().astype(str).str.replace('.', '/', regex=False).tolist()
            # 노이즈 정제 (공백 제거 및 'Removed' 등 본문 텍스트 제외)
            tickers_to_add = [t.strip() for t in tickers_to_add if t.strip() and len(t.strip()) <= 6 and not any(char.isdigit() for char in t)]
            removed_tickers.extend(tickers_to_add)
            
    # 3. 현재 종목과 최근 3년간 퇴출된 종목 병합
    all_historical_tickers = current_tickers.union(set(removed_tickers))
    
    # 대문자 표준화 및 최종 길이 필터링
    final_tickers = {t.upper() for t in all_historical_tickers if t and len(t) <= 6 and t.isalpha() or '/' in t}
    
    return sorted(list(final_tickers))

if __name__ == "__main__":
    print("위키피디아에서 S&P 500 히스토리컬 티커 수집 중...")
    tickers = get_historical_sp500_tickers(years=3)
    print(f"\n최근 3년간 S&P 500에 존재했던 총 티커 수: {len(tickers)}")
    print("티커 예시 (앞 20개):", tickers[:20])
