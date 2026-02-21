import pandas as pd
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta
import warnings
import json
import time
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ✅ 디스코드 웹후크 URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_fdr(ticker, end_date):
    """fdr을 사용하여 최근 3거래일 수급 데이터 추출"""
    try:
        # 주말/공휴일을 고려하여 최근 10일치 데이터를 가져와서 그중 마지막 3일 사용
        start_date_str = (end_date - timedelta(days=10)).strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # fdr의 'STOCK_INVESTOR' 데이터 소스 활용
        df_inv = fdr.DataReader(ticker, start_date_str, end_date_str, data_source='stock_investor')
        
        if df_inv is None or len(df_inv) < 3:
            return "0/0", False
            
        recent_3 = df_inv.tail(3)
        frgn_sum = int(recent_3['ForeignNet'].sum())
        inst_sum = int(recent_3['InstitutionalNet'].sum())
        
        def format_val(val):
            return f"+{val}" if val > 0 else str(val)
            
        # 외인이나 기관 중 한쪽이라도 3일 합계가 양수면 True
        is_hot = (frgn_sum > 0 or inst_sum > 0)
        return f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}", is_hot
    except:
        return "데이터미비", False

def is_recent_operating_profit_positive(ticker_code):
    """최신 공시 기준 영업이익 흑자 확인"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
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
    print(f"🚀 [폭풍전야] 수급 데이터 정밀 분석 시작...")
    
    try:
        krx_df = fdr.StockListing('KRX')
        krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
        ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    except Exception as e:
        print(f"목록 로드 실패: {e}")
        return

    end_date = datetime.today()
    
    # 병렬 처리 (괄호 오타 수정 완료)
    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(조건: 흑자+20일선 위+거래 급감+중간값 15억↑+3일 수급합산)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! ({int(time.time() - start_time)}초)")

if __name__ == "__main__":
    main()
