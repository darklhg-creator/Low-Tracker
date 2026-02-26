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
    today_str = datetime.now().strftime('%Y-%m-%d %A')
    print(f"--- {today_str} 분석 시작 ---")
    
    try:
        # [핵심 수정] 'KRX' 대신 'KRX-DESC'를 먼저 시도하거나 
        # 상장종목 전체를 가져오는 가장 기본 명령어를 사용합니다.
        # 최신 버전에서는 StockListing('KRX') 결과에 Sector가 빠지는 경우가 많으므로
        # 아래와 같이 상세 정보를 강제로 호출합니다.
        df_krx = fdr.StockListing('KRX-DESC') 
        
        # 만약 DESC 데이터도 문제가 있다면 일반 KRX 데이터를 가져옵니다.
        if df_krx is None or df_krx.empty:
            df_krx = fdr.StockListing('KRX')

        # 컬럼명 유연성 확보 (Symbol 또는 Code 둘 다 대응)
        code_col = 'Symbol' if 'Symbol' in df_krx.columns else 'Code'
        sector_col = 'Sector' if 'Sector' in df_krx.columns else 'Industry'

        if sector_col not in df_krx.columns:
            # 업종 컬럼이 아예 없다면 분석 불가하므로 에러 메시지 전송
            cols = ", ".join(df_krx.columns)
            send_discord_message(f"❌ 데이터 오류: 업종 정보(Sector)가 포함되지 않았습니다.\n현재 컬럼: {cols}")
            return

        # '반도체' 키워드가 포함된 종목만 필터링
        semi_df = df_krx[df_krx[sector_col].str.contains('반도체', na=False)].copy()
        
    except Exception as e:
        send_discord_message(f"❌ 데이터 로드 실패: {e}")
        return

    target_list = []
    
    # 분석 대상 (상위 50개 종목)
    for index, row in semi_df.head(50).iterrows():
        ticker = row[code_col]
        name = row['Name']
        
        # 시장 구분 (yfinance 접미사 설정)
        # MarketId(STK/KSQ) 또는 Market(KOSPI/KOSDAQ) 확인
        market = str(row.get('MarketId', row.get('Market', '')))
        suffix = ".KS" if "STK" in market or "KOSPI" in market.upper() else ".KQ"
        full_ticker = ticker + suffix
        
        try:
            # yfinance 가격 데이터 호출
            data = yf.download(full_ticker, period="40d", progress=False)
            if len(data) < 20: continue

            # 이격도 계산 (20일 이동평균 기준)
            data['MA20'] = data['Close'].rolling(window=20).mean()
            current_price = float(data['Close'].iloc[-1])
            ma20 = float(data['MA20'].iloc[-1])
            disparity = (current_price / ma20) * 100

            # 사용자 매매 기준: 이격도 90 이하
            if disparity <= 90:
                target_list.append(f"✅ **{name}** ({ticker})\n   └ 이격도: {disparity:.2f}% | 현재가: {int(current_price):,}원")
        except:
            continue

    # 결과 전송
    if target_list:
        msg = f"📢 **{today_str} 반도체 이격도 90 이하 종목**\n\n" + "\n".join(target_list)
        msg += "\n\n💡 *이후 네이버 증권에서 흑자 여부와 수급을 꼭 확인하세요!*"
    else:
        msg = f"ℹ️ **{today_str}**\n현재 이격도 90 이하인 반도체 종목이 없습니다."

    send_discord_message(msg)

if __name__ == "__main__":
    run_analysis()
