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

def get_investor_data_3days(ticker_code):
    """네이버 금융에서 최근 3거래일 외국인/기관 순매수 합계 가져오기"""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker_code}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        table = soup.find('table', {'class': 'type2'})
        rows = table.find_all('tr', {'onmouseover': 'mouseOver(this)'})
        
        if len(rows) < 3: return "0/0", False
        
        frgn_sum = 0
        inst_sum = 0
        consecutive_buy = True # 3일 연속 매수 여부 체크
        
        for i in range(3): # 최근 3일 데이터 순회
            cols = rows[i].find_all('td')
            inst_val = int(cols[5].get_text(strip=True).replace(',', ''))
            frgn_val = int(cols[6].get_text(strip=True).replace(',', ''))
            
            frgn_sum += frgn_val
            inst_sum += inst_val
            
            # 둘 다 마이너스면 연속 매수 실패로 간주 (개별 전략에 따라 수정 가능)
            if frgn_val <= 0 and inst_val <= 0:
                consecutive_buy = False

        def format_val(val):
            return f"+{val}" if val > 0 else str(val)

        status_text = f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}"
        return status_text, (frgn_sum > 0 or inst_sum > 0)
    except:
        return "데이터미비", False

def is_recent_operating_profit_positive(ticker_code):
    """최신 공시 기준 영업이익 흑자 확인"""
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
        
        df['Val'] = df['Close'] * df['Volume']
        df['MA20_Vol'] = df['Volume'].rolling(window=20).mean()
        df['MA20_Price'] = df['Close'].rolling(window=20).mean()
        
        curr = df.iloc[-1]
        vol_ratio = (curr['Volume'] / curr['MA20_Vol']) * 100
        day_return = (curr['Close'] - df['Close'].iloc[-2]) / df['Close'].iloc[-2]
        val_median = df['Val'].tail(20).median()

        # 🚀 [폭풍전야 정밀 필터]
        if curr['Close'] < df['MA20_Price'].iloc[-1]: return None  
        if abs(day_return) > 0.03: return None                   
        if vol_ratio > 35: return None                            
        if val_median < 1500000000: return None                  
        if (df['Val'].tail(20) >= 1000000000).sum() < 15: return None 

        # [흑자 확인 및 3일 수급 데이터 가져오기]
        if is_recent_operating_profit_positive(ticker):
            supply_info, is_hot = get_investor_data_3days(ticker)
            return {
                'Name': name, 'Code': ticker, 'Ratio': round(vol_ratio, 1), 
                'MedianVal': round(val_median / 100000000, 1), 
                'Return': round(day_return * 100, 2),
                'Supply': supply_info,
                'IsHot': is_hot
            }
    except: return None

def main():
    start_time = time.time()
    print(f"🚀 [폭풍전야 + 3일 수급] 분석 시작...")
    
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)
    
    tasks = [(t, n, start_date, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 조건을 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(흑자+20일선 위+거래 급감+중간값 15억↑+3일 수급합산)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! ({int(time.time() - start_time)}초)")

if __name__ == "__main__":
    main()
