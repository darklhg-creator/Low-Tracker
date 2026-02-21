import pandas as pd
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import warnings
import json
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ✅ 디스코드 웹후크 URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_indicators(df):
    """RSI, OBV, MA, 이격도, 전고점 계산"""
    delta = df['Close'].diff()
    up = delta.clip(lower=0); down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    df['RSI'] = 100 - (100 / (1 + ema_up / ema_down))
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    df['High60'] = df['High'].rolling(window=60).max()
    return df

def is_recent_operating_profit_positive(ticker_code):
    """네이버 금융 최신 공시 기준 영업이익 흑자 확인"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
        finance_table = tables[3]
        finance_table.columns = ['_'.join(str(c) for c in col).strip() for col in finance_table.columns]
        op_row = finance_table[finance_table.iloc[:, 0].str.contains('영업이익', na=False)]
        recent_values = pd.to_numeric(op_row.iloc[0, -4:], errors='coerce').dropna()
        return recent_values.iloc[-1] > 0
    except: return False

def analyze_stock(args):
    """개별 종목 정밀 분석"""
    ticker, name, start_date, end_date = args
    try:
        df = fdr.DataReader(ticker, start_date, end_date)
        if len(df) < 70: return None
        
        df = get_indicators(df)
        curr = df.iloc[-1]; prev = df.iloc[-4]
        
        # [검증 1] 20일선 우상향 & 20>60 정배열 유지
        if not (curr['MA20'] > prev['MA20'] and curr['MA20'] > curr['MA60']): return None
        
        # [검증 2] 거래대금 중간값 기준 완화 (30억 -> 15억)
        df['Val'] = df['Close'] * df['Volume']
        if df['Val'].rolling(window=20).median().iloc[-1] < 1500000000: return None
        
        # [검증 3] OBV 에너지 상승 유지
        if curr['OBV'] <= df['OBV'].iloc[-5]: return None
        
        # [검증 4] 전고점 대비 10% 이내 & 이격도 안정(105이하)
        dist_from_high = (curr['High60'] - curr['Close']) / curr['High60']
        if not (dist_from_high < 0.10 and curr['Disparity'] < 105): return None
        
        # [검증 5] 눌림목 범위 완화 (RSI 45 -> 50)
        if curr['RSI'] > 50: return None

        # [최종] 영업이익 흑자 확인
        if is_recent_operating_profit_positive(ticker):
            return {
                'Name': name, 'Code': ticker, 'RSI': round(curr['RSI'], 1), 
                '이격도': round(curr['Disparity'], 1), '전고점차': f"{round(dist_from_high*100, 1)}%"
            }
    except: return None

def main():
    start_time = time.time()
    print(f"🚀 병렬 분석 엔진 가동... (수정된 문턱 적용)")
    
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=150)
    
    tasks = [(t, n, start_date, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = [r for r in results if r is not None]
    
    # 디스코드 메시지 구성
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 완화된 조건으로도 검색된 종목이 없습니다."
    else:
        msg = f"💎 **{end_date.strftime('%Y-%m-%d')} 수정된 정예 종목** 💎\n"
        msg += "*(RSI 50↓ / 거래대금 중간값 15억↑ / 정배열 / 흑자)*\n\n"
        for p in final_picks:
            msg += f"• **{p['Name']}**({p['Code']}) | RSI: `{p['RSI']}` | 이격도: `{p['이격도']}` | 전고점차: `{p['전고점차']}`\n"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! 소요시간: {int(time.time() - start_time)}초")

if __name__ == "__main__":
    main()
