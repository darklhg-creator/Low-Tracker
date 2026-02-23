import FinanceDataReader as fdr
import pandas as pd
import requests
from datetime import datetime
import warnings

# pandas 경고 메시지 숨기기 (깔끔한 로그 출력을 위해)
warnings.filterwarnings('ignore')

# ---------------------------------------------------------
# 1. 설정 부분 (디스코드 웹훅)
# ---------------------------------------------------------
# 본인의 깃허브 시크릿(Secrets)으로 웹훅을 관리하시거나, 아래 변수에 직접 입력해 주세요.
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def send_discord_message(content):
    if DISCORD_WEBHOOK_URL == "여기에_디스코드_웹훅_주소를_입력하세요":
        print("⚠️ 디스코드 웹훅 URL이 설정되지 않아 메시지를 전송하지 않습니다.")
        return
    
    data = {"content": content}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"디스코드 전송 실패: {e}")

# ---------------------------------------------------------
# 2. 메인 실행 로직
# ---------------------------------------------------------
def main():
    print("주도주 탐색 스크립트 시작...")
    
    # 한국거래소(KRX) 상장 종목 전체 가져오기
    df_total = fdr.StockListing('KRX')
    all_analyzed = []

    # 전체 종목을 순회하며 조건 검색
    for idx, row in df_total.iterrows():
        code = row['Code']
        name = row['Name']
        
        # 주식(스팩, 우선주 등 제외) 필터링이 필요하다면 여기에 추가 가능
        
        try:
            # 일봉/주봉/월봉 분석을 위해 데이터를 넉넉히 가져옴 (약 1년치)
            df_daily = fdr.DataReader(code).tail(200) 
            if len(df_daily) < 60: 
                continue
            
            # 1. 일봉 이격도 체크 (기존 로직)
            current_price = df_daily['Close'].iloc[-1]
            ma20_daily = df_daily['Close'].rolling(window=20).mean().iloc[-1]
            disparity = round((current_price / ma20_daily) * 100, 1)
            
            # 기본 조건: 이격도 95% 이하인 낙폭 과대주만 일단 대상
            if disparity > 95.0: 
                continue

            # ---------------------------------------------------------
            # 2. 주봉/월봉 상승전환 분석
            # ---------------------------------------------------------
            # 주봉 변환 (W: 일요일 기준 일주일)
            df_weekly = df_daily['Close'].resample('W').last()
            ma5_weekly = df_weekly.rolling(window=5).mean()
            
            # 주봉 상승전환 시그널: 5주선이 하락을 멈추고 상승하거나, 종가가 5주선 돌파
            is_weekly_up = (df_weekly.iloc[-1] > ma5_weekly.iloc[-1]) and (df_weekly.iloc[-2] <= ma5_weekly.iloc[-2])
            
            # 월봉 변환 (M: 월말 기준)
            df_monthly = df_daily['Close'].resample('M').last()
            # 월봉 상승전환 시그널: 이번 달 종가가 지난달 종가보다 높음 (양봉/반등)
            is_monthly_rebound = df_monthly.iloc[-1] > df_monthly.iloc[-2]

            # 최종 필터: 낙폭과대(이격도) + 주봉 돌파 + 월봉 반등
            if is_weekly_up or is_monthly_rebound:
                all_analyzed.append({
                    'name': name, 
                    'code': code, 
                    'disparity': disparity,
                    'status': "주봉돌파" if is_weekly_up else "월봉반등"
                })
        except Exception as e:
            # 개별 종목에서 에러가 나도 전체 스크립트가 멈추지 않도록 처리
            continue

    # ---------------------------------------------------------
    # 3. 결과 정리 및 디스코드 전송
    # ---------------------------------------------------------
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if len(all_analyzed) == 0:
        msg = f"[{today_str}] 조건에 맞는 종목이 없습니다."
        print(msg)
        send_discord_message(msg)
        return

    # 이격도 순으로 정렬 (가장 많이 떨어진 종목부터)
    all_analyzed = sorted(all_analyzed, key=lambda x: x['disparity'])

    msg = f"**[{today_str}] 낙폭과대 반등 예상 종목 리스트**\n"
    for item in all_analyzed:
        msg += f"- {item['name']} ({item['code']}) | 이격도: {item['disparity']}% | 상태: {item['status']}\n"
    
    # Github Actions 로그 확인용 출력
    print(msg)
    
    # 디스코드 전송
    send_discord_message(msg)
    print("스크립트 실행 및 전송 완료!")

if __name__ == "__main__":
    main()
