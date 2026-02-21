import pandas as pd
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import warnings
import json

# 경고 메시지 무시
warnings.filterwarnings('ignore')

# ✅ 사용자님의 디스코드 웹후크 주소
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT" 

def get_rsi(df, period=14):
    """지수이동평균(EMA) 방식의 RSI 계산"""
    delta = df['Close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=period-1, adjust=False).mean()
    ema_down = down.ewm(com=period-1, adjust=False).mean()
    rs = ema_up / ema_down
    return 100 - (100 / (1 + rs))

def is_recent_operating_profit_positive(ticker_code):
    """네이버 금융을 통해 최신 공시 기준 영업이익 흑자 확인"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
        tables = pd.read_html(res.text, encoding='euc-kr')
        finance_table = tables[3]
        finance_table.columns = ['_'.join(str(c) for c in col).strip() for col in finance_table.columns]
        op_row = finance_table[finance_table.iloc[:, 0].str.contains('영업이익', na=False)]
        
        if op_row.empty: return False
        recent_values = pd.to_numeric(op_row.iloc[0, -4:], errors='coerce').dropna()
        return recent_values.iloc[-1] > 0 if len(recent_values) > 0 else False
    except:
        return False

def send_discord_message(content):
    """디스코드로 분석 결과 전송"""
    payload = {"content": content}
    try:
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    except:
        pass

def main():
    print(f"🚀 [{datetime.now().strftime('%H:%M:%S')}] 정밀 추세 및 낙폭과대 스캔 시작...")
    
    try:
        krx_df = fdr.StockListing('KRX')
        krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
        ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    except: return

    end_date = datetime.today()
    start_date = end_date - timedelta(days=150) # 60일선 계산을 위해 기간 확보
    
    candidates = []
    tickers = list(ticker_dict.keys())
    
    for ticker in tickers:
        try:
            df = fdr.DataReader(ticker, start_date, end_date)
            if len(df) < 70: continue # 60일선 확보용
            
            # 1. 이동평균선 계산
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['MA60'] = df['Close'].rolling(window=60).mean()
            
            # [추가 조건 1] 20일선 방향성 (3일 전보다 현재가 높아야 함)
            is_ma20_up = df['MA20'].iloc[-1] > df['MA20'].iloc[-4]
            
            # [추가 조건 2] 정배열 초기 (20일선 > 60일선)
            is_gold_alignment = df['MA20'].iloc[-1] > df['MA60'].iloc[-1]
            
            if not (is_ma20_up and is_gold_alignment): continue

            # 2. 거래대금 중간값 30억 이상 (평균의 함정 제거)
            df['Value'] = df['Close'] * df['Volume']
            recent_median = df['Value'].rolling(window=20).median().iloc[-1]
            if recent_median < 3000000000: continue
                
            # 3. RSI 40 이하 (낙폭과대 타점)
            df['RSI'] = get_rsi(df)
            current_rsi = df['RSI'].iloc[-1]
            
            if current_rsi <= 40:
                candidates.append({
                    'Code': ticker, 'Name': ticker_dict[ticker],
                    'RSI': round(current_rsi, 2),
                    'Value': round(recent_median / 100000000, 1)
                })
        except: continue

    # 4. 재무 흑자 검증
    final_picks = [c for c in candidates if is_recent_operating_profit_positive(c['Code'])]
    
    if not final_picks:
        msg = f"📅 **{end_date.strftime('%Y-%m-%d')} 분석 결과**\n모든 조건(RSI 40↓, 20선 우상향, 정배열, 흑자)을 만족하는 종목이 없습니다."
    else:
        msg = f"🏆 **{end_date.strftime('%Y-%m-%d')} 정예 우량 눌림목 종목** 🏆\n"
        msg += "*(조건: RSI 40↓ / 20선 우상향 / 20>60 정배열 / 흑자)*\n\n"
        for p in final_picks:
            msg += f"• **{p['Name']}**({p['Code']}) | RSI: `{p['RSI']}` | 거래대금(중간): `{p['Value']}억` \n"

    send_discord_message(msg)
    print(f"✅ 분석 완료: {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
