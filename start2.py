import pandas as pd
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import warnings
import json
import time
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ✅ 사용자 디스코드 웹후크
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_stable(ticker):
    """네이버 금융 PC 버전 테이블에서 수급 데이터를 추출 (안정성 강화)"""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        res = requests.get(url, headers=headers, timeout=7)
        
        # pandas read_html을 사용하여 테이블을 직접 파싱
        tables = pd.read_html(res.text, encoding='euc-kr')
        # 수급 데이터가 담긴 테이블(보통 2번째 혹은 3번째) 탐색
        df_inv = None
        for t in tables:
            if '기관' in t.columns and '외국인' in t.columns:
                df_inv = t
                break
        
        if df_inv is None: return "0/0", False

        # 불필요한 행 제거 및 최근 3일 데이터 확보
        df_inv = df_inv.dropna(subset=['기관', '외국인']).head(3)
        
        def clean_val(val):
            if isinstance(val, str):
                val = val.replace(',', '').replace('+', '')
            return int(float(val))

        inst_sum = df_inv['기관'].apply(clean_val).sum()
        frgn_sum = df_inv['외국인'].apply(clean_val).sum()
        
        def format_val(val):
            return f"+{val}" if val > 0 else str(val)
            
        is_hot = (frgn_sum > 0 or inst_sum > 0)
        return f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}", is_hot
    except Exception as e:
        return "데이터미비", False

def is_recent_operating_profit_positive(ticker_code):
    """최신 공시 기준 영업이익 흑자 확인"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
        for df in tables:
            df.columns = [str(c) for c in df.columns]
            if any('영업이익' in str(row) for row in df.iloc[:,0]):
                val = pd.to_numeric(df.iloc[0, -4:], errors='coerce').dropna()
                return val.iloc[-1] > 0
        return False
    except: return False

def analyze_stock(args):
    ticker, name, end_date = args
    start_date_price = end_date - timedelta(days=60)
    try:
        df = fdr.DataReader(ticker, start_date_price, end_date)
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

        if is_recent_operating_profit_positive(ticker):
            # 수급 데이터 추출
            supply_info, is_hot = get_investor_data_stable(ticker)
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
    print(f"🚀 [폭풍전야] 최종 안정화 엔진 가동...")
    
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))

    end_date = datetime.today()
    
    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    # max_workers를 5로 제한하여 네이버의 IP 차단을 방지합니다.
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(수정: 안정적 데이터 추출 및 흑자 조건 강화)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! ({int(time.time() - start_time)}초)")

if __name__ == "__main__":
    main()
