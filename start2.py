import FinanceDataReader as fdr
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

# 사용자님의 디스코드 웹후크 URL
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT'

def send_discord_message(content):
    data = {"content": content}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"메시지 전송 에러: {e}")

def run_analysis():
    today_str = datetime.now().strftime('%Y-%m-%d %A')
    print(f"--- {today_str} 분석 시작 ---")
    
    try:
        # 'KRX' 대신 'KOSPI'와 'KOSDAQ'을 각각 불러와서 합치면 Sector 정보가 더 정확합니다.
        df_kospi = fdr.StockListing('KOSPI')
        df_kosdaq = fdr.StockListing('KOSDAQ')
        df_krx = pd.concat([df_kospi, df_kosdaq])

        # 'Sector' 컬럼이 있는지 확인 (에러 방지)
        if 'Sector' not in df_krx.columns:
            # 컬럼명이 다를 경우를 대비해 전체 컬럼 출력 (디버깅용)
            print(f"Available columns: {df_krx.columns}")
            send_discord_message("❌ 에러: 데이터에 'Sector' 항목이 없습니다. 관리자 확인 필요.")
            return

        # 업종명에 '반도체'가 포함된 종목 추출
        semi_df = df_krx[df_krx['Sector'].str.contains('반도체', na=False)].copy()
        
    except Exception as e:
        send_discord_message(f"❌ 데이터 로드 실패: {e}")
        return

    target_list = []
    
    # 시가총액 상위 종목부터 분석 (너무 많으면 깃허브에서 끊길 수 있어 100개 제한)
    search_count = 0
    for _, row in semi_df.iterrows():
        if search_count >= 100: break
        
        ticker = row['Symbol']
        name = row['Name']
        # yfinance용 티커 변환
        full_ticker = ticker + (".KS" if row['Code'] in df_kospi['Symbol'].values else ".KQ")
        
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
                search_count += 1
        except:
            continue

    # 결과 전송
    if target_list:
        msg = f"📢 **{today_str} 반도체 이격도 90 이하 종목**\n\n" + "\n".join(target_list)
        msg += "\n\n💡 *영업이익 흑자 및 수급(외인/기관)을 꼭 확인하세요!*"
    else:
        msg = f"ℹ️ **{today_str}**\n현재 이격도 90 이하인 반도체 종목이 없습니다."

    send_discord_message(msg)

if __name__ == "__main__":
    run_analysis()
