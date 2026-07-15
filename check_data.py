# 데이터 체크용 스크립트(데이터 추출 후 잘 불러왔는지 확인용)

import os
import glob
import pandas as pd
from datetime import datetime

# script.py에서 Parquet을 선택했을 때 저장되는 폴더 경로
DATA_DIR = "./market_data/parquet"

def check_single_parquet(file_path):
    """개별 Parquet 파일의 메타데이터 및 품질을 분석합니다."""
    try:
        # 인덱스와 컬럼을 효율적으로 읽기 위해 가볍게 로드
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return {
                "status": "Empty",
                "rows": 0,
                "start": "N/A",
                "end": "N/A",
                "regular_market_pct": 0.0
            }
        
        # MultiIndex 구조에서 timestamp 값 추출
        if isinstance(df.index, pd.MultiIndex):
            timestamps = df.index.get_level_values('timestamp')
        else:
            timestamps = df.index
            
        # 1. 시작 및 종료 시각 (UTC 기준)
        start_time = timestamps.min()
        end_time = timestamps.max()
        
        # 2. 총 데이터 개수
        total_rows = len(df)
        
        # 3. 정규장 데이터 비율 계산 (미국 동부 시간 EST 기준 09:30 ~ 16:00)
        # 뉴욕 시간으로 변환하여 정규장 시간인지 체크
        timestamps_ny = pd.to_datetime(timestamps).tz_convert('America/New_York')
        
        # 정규장 조건: 월~금요일 이고, 09:30 <= 시간 < 16:00
        is_weekday = timestamps_ny.weekday < 5
        
        # 시간 조건을 분 단위로 환산 (9*60 + 30 = 570분, 16*60 = 960분)
        minutes_of_day = timestamps_ny.hour * 60 + timestamps_ny.minute
        is_regular_hours = (minutes_of_day >= 570) & (minutes_of_day < 960)
        
        regular_rows = df[is_weekday & is_regular_hours]
        regular_pct = (len(regular_rows) / total_rows) * 100 if total_rows > 0 else 0.0
        
        return {
            "status": "OK",
            "rows": total_rows,
            "start": start_time.strftime('%Y-%m-%d %H:%M'),
            "end": end_time.strftime('%Y-%m-%d %H:%M'),
            "regular_market_pct": round(regular_pct, 2)
        }
        
    except Exception as e:
        return {
            "status": f"Error: {str(e)}",
            "rows": 0,
            "start": "N/A",
            "end": "N/A",
            "regular_market_pct": 0.0
        }

def main():
    print("🔍 로컬 Parquet 데이터셋 검증을 시작합니다...")
    
    # 1. 폴더 내 모든 parquet 파일 검색
    search_path = os.path.join(DATA_DIR, "*_5min_historical.parquet")
    files = glob.glob(search_path)
    
    if not files:
        print(f"❌ '{DATA_DIR}' 폴더에 parquet 파일이 존재하지 않습니다.")
        return
        
    print(f"발견된 총 파일 수: {len(files)}개")
    
    # 2. 메타데이터 추출 및 요약 리스트 작성
    summary_data = []
    for f in files:
        symbol = os.path.basename(f).split('_')[0]
        report = check_single_parquet(f)
        report['symbol'] = symbol
        summary_data.append(report)
        
    # DataFrame으로 변환하여 출력 가공
    df_summary = pd.DataFrame(summary_data)
    
    # 컬럼 순서 정렬
    df_summary = df_summary[['symbol', 'status', 'rows', 'start', 'end', 'regular_market_pct']]
    
    # 3. 전체 요약 통계 출력
    print("\n" + "="*80)
    print("📊 데이터셋 종합 요약 (Summary Stats)")
    print("="*80)
    print(f"• 총 수집 종목 수: {len(df_summary)}개")
    print(f"• 정상 수집 완료 종목: {len(df_summary[df_summary['status'] == 'OK'])}개")
    print(f"• 데이터 오류/공백 종목: {len(df_summary[df_summary['status'] != 'OK'])}개")
    if len(df_summary[df_summary['status'] == 'OK']) > 0:
        print(f"• 종목당 평균 데이터 수: {int(df_summary[df_summary['status'] == 'OK']['rows'].mean()):,} 행")
        print(f"• 평균 정규장 데이터 비율: {df_summary[df_summary['status'] == 'OK']['regular_market_pct'].mean():.2f}%")
    print("="*80)
    
    # 4. 상세 목록 출력 (상위 20개 종목 예시)
    print("\n📋 개별 종목 수집 현황 (상위 20개 종목 예시):")
    # 터미널에서 생략 없이 예쁘게 출력되도록 pandas 옵션 설정
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df_summary.head(20).to_string(index=False))
    
    # 데이터 결과를 csv나 excel 등으로 보관하고 싶을 경우 저장 가능
    # df_summary.to_csv("data_audit_report.csv", index=False)

if __name__ == "__main__":
    main()