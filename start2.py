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

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def is_recent_operating_profit_positive(ticker_code):
    """실시간 영업이익 흑자 확인"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
        finance_table = tables[3]
        finance_table.columns = ['_'.join(str(c) for c in col).strip() for col in finance_table.columns]
        op_row = finance_table[finance_table.iloc[:, 0].str.contains('영업이익', na=False)]
        return pd.to_numeric(op_row.iloc[0, -4:], errors='coerce').dropna().iloc[-1] > 0
    except: return False

def analyze_stock(args):
    ticker, name, start_date, end_date = args
    try:
        df = fdr.DataReader(ticker, start_date, end_date)
        if len(df) < 30: return None
        
        # 지표 계산
        df['Val'] = df['Close'] * df['Volume']  # 일일 거래대금
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA20_Vol'] = df['Volume'].rolling(window=20).mean()
        
        curr = df.iloc[-1]
        vol_ratio = (curr['Volume'] / curr['MA20_Vol']) * 100
        day_return = (curr['Close'] - df['Close'].iloc[-2]) / df['Close'].iloc[-2]

        # ---------------------------------------------------------
        # 🛡️ [유동성 함정 탈출 필터 - 수정된 부분]
        # ---------------------------------------------------------
        # 방안 1: 거래대금 중간값 15억 이상 (단기 펌핑 무시)
        val_median = df['Val'].tail(20).median()
        if val_median < 1500000000: return None
        
        # 방안 2: 최근 20일 중 거래대금 10억 이상인 날이 15일 이상 (꾸준함 검증)
        steady_days = (df['Val'].tail(20) >= 1000000000).sum()
        if steady_days < 15: return None
        # ---------------------------------------------------------

        # [기존 폭풍전야 조건]
        if curr['Close'] < curr['MA20']: return None # 20일선 위
        if abs(day_return) > 0.03: return None      # 변동성 3% 이내
        if vol_ratio > 35: return None               # 거래량 35% 이하 급감

        if is_recent_operating_profit_positive(ticker):
            return {
                'Name': name, 'Code': ticker, 'Ratio': round(vol_ratio, 1), 
                'MedianVal': round(val_median / 100000000, 1), 
                'Return': round(day_return * 100, 2)
            }
    except: return None

def main():
    start_time = time.time()
    print(f"🚀 [유동성 정밀 필터링] 폭풍전야 분석 시작...")
    
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)
    
    tasks = [(t, n, start_date, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 정밀 유동성 조건을 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 정밀 유동성 TOP {len(final_picks)}]**\n"
        msg += "*(조건: 흑자+20일선 위+거래 급감+중간값 15억↑+유지 15일↑)*\n"
        msg += "```"
        msg += f"{'종목명':<10} {'거래비율(%)':<10} {'중간대금(억)':<10} {'오늘등락(%)':<10}\n"
        for p in final_picks:
            msg += f"{p['Name']:<10} {p['Ratio']:>12.1f} {p['MedianVal']:>12.1f} {p['Return']:>13.2f}\n"
        msg += "```"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! ({int(time.time() - start_time)}초)")

if __name__ == "__main__":
    main()
