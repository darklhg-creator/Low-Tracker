import FinanceDataReader as fdr
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

# 사용자 디스코드 웹후크 URL
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT'

def send_discord_message(content):
    data = {"content": content}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"메시지 전송 에러: {e}")

def run_analysis():
    # 현재 시점: 2026-02-26 목요일
    today_str = datetime.now().strftime('%Y-%m-%d %A')
    print(f"--- {today_str} 분석 시작 ---")
    
    try:
        # 1. 시세 데이터(KRX)와 종목 상세 정보(KRX-DESC)를 각각 가져옴
        df_list = fdr.StockListing('KRX') # 현재 컬럼: Code, Name, Market 등
        df_desc = fdr.StockListing('KRX-DESC') # 여기 'Sector' 업종 정보가 있음
        
        # 2. 'Code'와 'Symbol' 기준으로 두 데이터를 합침 (Merge)
        # KRX-DESC의 'Symbol' 컬럼이 종목코드임
        df_krx = pd.merge(df_list, df_desc[['Symbol', 'Sector']], left_on='Code', right_on='Symbol', how='left')

        # 3. 업종명에 '반도체'가 포함된 종목 필터링
        # Sector 컬럼이 존재하므로 이제 에러가 나지 않습니다.
        semi_df = df_krx[df_krx['Sector'].str.contains('반도체', na=False)].copy()
        
        if semi_df.empty:
            send_discord_message(f"ℹ️ {today_str}: 반도체 업종 종목을 찾지 못했습니다. 데이터 형식을 재점검합니다.")
            return
            
    except Exception as e:
        send_discord_message(f"❌ 데이터 로드 및 병합 실패: {e}")
        return

    target_list = []
    
    # 4. 상위 50개 종목 이격도 분석
    for index, row in semi_df.head(50).iterrows():
        ticker = row['Code']
        name = row['Name']
        
        # 시장 구분 (KOSPI/KOSDAQ)에 따른 티커 설정
        market = row.get('MarketId', '')
        suffix = ".KS" if market == "STK" else ".KQ" # STK=코스피, KSQ=코스닥
        full_ticker = ticker + suffix
        
        try:
            data = yf.download(full_ticker, period="40d", progress=False)
            if len(data) < 20: continue

            data['MA20'] = data['Close'].rolling(window=20).mean()
            current_price = float(data['Close'].iloc[-1])
            ma20 = float(data['MA20'].iloc[-1])
            disparity = (current_price / ma20) * 100

            # 사용자 매매 기준: 이격도 90 이하
            if disparity <= 90:
                target_list.append(f"✅ **{name}** ({ticker})\n   └ 이격도: {disparity:.2f}% | 현재가: {int(current_price):,}원")
        except:
            continue

    # 5. 결과 전송
    if target_list:
        msg = f"📢 **{today_str} 반도체 이격도 90 이하 종목**\n\n" + "\n".join(target_list)
        msg += "\n\n💡 *영업이익 흑자 및 수급(외인/기관)을 꼭 확인하세요!*"
    else:
        msg = f"ℹ️ **{today_str}**\n현재 이격도 90 이하인 반도체 종목이 없습니다."

    send_discord_message(msg)

if __name__ == "__main__":
    run_analysis()
