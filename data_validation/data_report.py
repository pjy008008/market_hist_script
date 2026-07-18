# 데이터 분석용 스크립트(시작일, 종료일, 정규장 비중 등)

import os
import glob
import pandas as pd
from datetime import datetime
from pathlib import Path

# script.py에서 Parquet을 선택했을 때 저장되는 폴더 경로
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "market_data" / "parquet"
REPORT_DIR = PROJECT_ROOT / "report"
REPORT_FILE = os.path.join(REPORT_DIR, "data_audit_report.txt")

def analyze_parquet(file_path):
    """개별 Parquet 파일의 데이터 정합성과 품질을 정밀 분석합니다."""
    try:
        df = pd.read_parquet(file_path)
        if df.empty:
            return "Empty", 0, "N/A", "N/A", 0.0
        
        # MultiIndex 구조 대응
        if isinstance(df.index, pd.MultiIndex):
            timestamps = df.index.get_level_values('timestamp')
        else:
            timestamps = df.index
            
        start_time = timestamps.min()
        end_time = timestamps.max()
        total_rows = len(df)
        
        # 미국 동부 뉴욕 시간으로 변환하여 정규장(09:30 ~ 16:00 EST) 데이터 필터링
        timestamps_ny = pd.to_datetime(timestamps).tz_convert('America/New_York')
        is_weekday = timestamps_ny.weekday < 5
        minutes_of_day = timestamps_ny.hour * 60 + timestamps_ny.minute
        is_regular_hours = (minutes_of_day >= 570) & (minutes_of_day < 960)
        
        regular_rows = df[is_weekday & is_regular_hours]
        regular_pct = (len(regular_rows) / total_rows) * 100 if total_rows > 0 else 0.0
        
        return "OK", total_rows, start_time.strftime('%Y-%m-%d %H:%M'), end_time.strftime('%Y-%m-%d %H:%M'), round(regular_pct, 2)
    except Exception as e:
        return f"Error ({str(e)})", 0, "N/A", "N/A", 0.0

def main():
    print("🔍 Parquet 데이터셋 전수 검사 및 개발 가이드 파일 생성을 시작합니다...")
    search_path = os.path.join(DATA_DIR, "*_5min_historical.parquet")
    files = glob.glob(search_path)
    
    if not files:
        print(f"❌ '{DATA_DIR}' 폴더에 분석할 Parquet 파일이 없습니다.")
        return
        
    print(f"총 {len(files)}개의 Parquet 파일을 감지했습니다. 데이터 분석 중...")
    
    # 텍스트 파일 버퍼 작성
    lines = []
    lines.append("=" * 100)
    lines.append("               S&P 500 5-Minute Historical Data Audit & Backtesting Guide")
    lines.append("=" * 100)
    lines.append(f" 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f" 분석 데이터 경로: {os.path.abspath(DATA_DIR)}")
    lines.append(f" 발견된 총 파일(종목) 수: {len(files)}개")
    lines.append("=" * 100 + "\n")
    
    # 데이터셋 테이블 헤더 작성
    lines.append(f"{'Ticker':<10} | {'Status':<8} | {'Total Rows':<12} | {'Start Period (UTC)':<20} | {'End Period (UTC)':<20} | {'Regular %':<10}")
    lines.append("-" * 100)
    
    ok_count = 0
    total_avg_rows = []
    total_avg_pct = []
    
    for idx, f in enumerate(sorted(files), 1):
        symbol = os.path.basename(f).split('_')[0]
        status, rows, start, end, reg_pct = analyze_parquet(f)
        
        lines.append(f"{symbol:<10} | {status:<8} | {rows:<12,} | {start:<20} | {end:<20} | {reg_pct:>8.2f}%")
        
        if status == "OK":
            ok_count += 1
            total_avg_rows.append(rows)
            total_avg_pct.append(reg_pct)
            
        if idx % 50 == 0 or idx == len(files):
            print(f"진행 상태: [{idx}/{len(files)}] 분석 완료")
            
    # 종합 요약 부분 작성
    lines.append("\n" + "=" * 100)
    lines.append("📊 1. 데이터셋 종합 통계 요약 (Dataset Summary Stats)")
    lines.append("=" * 100)
    lines.append(f" • 정상 분석 완료 종목 수: {ok_count}개")
    lines.append(f" • 에러/공백 상태인 종목 수: {len(files) - ok_count}개")
    if ok_count > 0:
        lines.append(f" • 종목당 평균 데이터 수: {int(sum(total_avg_rows)/len(total_avg_rows)):,} 행(Rows)")
        lines.append(f" • 전체 데이터셋 평균 정규장 비중: {sum(total_avg_pct)/len(total_avg_pct):.2f}%")
    lines.append("=" * 100 + "\n")
    
    # 💡 백테스팅 시스템 개발을 위한 실전 참고 및 가이드라인 추가
    lines.append("💡 2. 퀀트 백테스팅 개발 시 핵심 참고사항 (Developer Guidelines)")
    lines.append("=" * 100)
    lines.append("""
[1] 타임존(Timezone) 매핑 및 얼라인먼트의 중요성
    - Alpaca API가 리턴하는 타임스탬프는 원칙적으로 UTC 기준입니다.
    - 그러나 미국 주식시장 정규장은 뉴욕 현지 시각(America/New_York)인 09:30 ~ 16:00에 개장합니다.
    - 뉴욕은 서머타임(Daylight Saving Time; 여름엔 EDT(UTC-4), 겨울엔 EST(UTC-5))을 적용합니다.
    - 백테스팅 알고리즘을 짤 때 단순히 5시간 또는 4시간을 빼서 계산하면 안 되고, 반드시 
      Pandas의 '.tz_convert("America/New_York")'를 활용해 시스템 로컬 타임존을 뉴욕 시각으로 정렬한 뒤
      정규장 시간만 발라내야 합니다.

[2] 정규장(Regular Market) 데이터만 필터링하는 이유
    - 보고서에 기록된 'Regular %'가 100% 미만(보통 60~70%대)으로 나타나는 이유는 
      본 데이터에 장전 거래(Pre-market), 장후 시간외 거래(After-hours)의 5분봉이 포함되어 있기 때문입니다.
    - 프리/포스트 마켓은 거래량이 매우 적고 매수/매도 호가 스프레드가 넓어 슬리피지(Slippage)가 엄청나게 크게 발생합니다.
    - 대부분의 백테스팅 로직은 이 노이즈를 제거한 '정규장 데이터(Regular % 기준 100% 필터링)'로 가공한 후 시뮬레이션해야 실제 거래와 일치합니다.

[3] 데이터 길이 불일치 및 예외 처리 (신규 편입/신규 상장 종목)
    - 최근 3년 내에 새롭게 상장된 신생 주식이나 최근 S&P 500에 편입된 주식(예: ABNB 등)은 시작일(Start Period)이 다른 주식들보다 많이 늦게 잡힙니다.
    - 다중 종목 포트폴리오 백테스팅을 돌릴 때 특정 과거 시점에 데이터가 존재하지 않는 종목이 포트폴리오 편입 신호를 보낼 경우, 
      로직이 에러를 뿜으며 터지지 않도록 예외 처리(NaN 무시 또는 해당 시점 포트폴리오 비중 제외 등)를 안전하게 해두어야 합니다.

[4] 생존 편향(Survivorship Bias) 극복을 위한 설계
    - 위키피디아의 '퇴출 변경 이력'까지 긁어와 과거에 존재했다가 지금은 사라진 종목까지 수집 대상에 넣은 것은 아주 훌륭한 접근입니다.
    - 백테스팅 엔진 구동 시 특정 종목이 S&P 500에 편입되기 전/후의 시점을 날짜별로 트래킹하여, 
      그 주식이 '실제로 해당 과거 시점 당시에 S&P 500에 들어있었을 때만' 투자 바스켓에 추가하도록 코드를 설계하면 100% 엄격한 백테스팅이 완성됩니다.

[5] 소수 티커 기호(Special Ticker Format) 매핑
    - 위키피디아나 야후파이낸스에서 마침표(예: BRK.B, BF.B)를 사용하는 종목들은 본 스크립트에서 Alpaca API 명세에 맞춰 
      슬래시(예: BRK/B)로 통일하여 저장했습니다. 
      시스템 설계 시 파일 입출력 및 API 연동 시 문자 형식이 일관되게 치환되는지 점검하세요.
""")
    lines.append("=" * 100)
    
    # 파일 쓰기
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        
    print(f"\n🎉 성공! 데이터 검증 보고서 및 개발 참고사항이 '{REPORT_FILE}' 파일로 깔끔하게 저장되었습니다.")

if __name__ == "__main__":
    main()
