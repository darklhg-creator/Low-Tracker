import pandas as pd
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta
import warnings
import json
import time
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_fdr(ticker, end_date):
    """fdr을 사용하여 최근 3거래일 수급 데이터 추출 (네이버 스크래핑 대체)"""
    try:
        # 최근 10일치 데이터를 넉넉히 가져옴 (주말/공휴일 고려)
        start_date = (end_date - timedelta(days=10)).strftime('%Y-%m-%d')
        # fdr의 'STOCK_INVESTOR' 기능을 활용 (안정성 최상)
        df_inv = fdr.DataReader(ticker, start_date, end_date.strftime('%Y-%m-%d'), data_source='stock_investor')
        
        if df_inv is None or len(df_inv) < 3:
            return "0/0", False
            
        # 최근 3일치 합계 (외국인: 'ForeignNet', 기관: 'InstitutionalNet')
        recent_3 = df_inv.tail(3)
        frgn_sum = int(recent_3['ForeignNet'].sum())
        inst_sum = int(recent_3['InstitutionalNet'].sum())
        
        def format_val(val):
            return f"+{val}" if val > 0 else str(val)
            
        is_hot = (frgn_sum > 0 or inst_sum > 0)
        return f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}", is_hot
    except:
        return "데이터미비", False

def is_recent_operating_profit_positive(ticker_code):
    """영업이익 흑자 확인 (안정적인 테이블 구조 사용)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
        # 종목마다 테이블 위치가 다를 수 있어 '영업이익' 글자로 검색
        for df in tables:
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
            # ✅ 수급 데이터 소스 변경: fdr을 통한 직접 데이터 요청
            supply_info, is_hot = get_investor_data_fdr(ticker, end_date)
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
    print(f"🚀 [수급 소스 교체 완료] 정밀 분석 시작...")
    
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    
    end_date = datetime.today()
    
    # 병렬 처리
    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=8
