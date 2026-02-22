import pandas as pd
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta
import warnings
import json
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

warnings.filterwarnings('ignore')

# ✅ 효근님의 인증키 및 디스코드 주소 (오리지널 기반)
RAW_KEY = "62e0d95b35661ef8e1f9a665ef46cc7cd64a3ace4d179612dda40c847f6bdb7e"
PUBLIC_API_KEY = unquote(RAW_KEY) 
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_public(ticker_name):
    """공공데이터 API: 최근 3일 수급 추출 (안정성 보강 버전)"""
    try:
        url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getInvestorRegistrationStat"
        today = datetime.now()
        # 주말/공휴일 대비 넉넉히 10일치 조회
        start_dt = (today - timedelta(days=10)).strftime('%Y%m%d')
        
        params = {
            'serviceKey': PUBLIC_API_KEY,
            'resultType': 'json',
            'itmsNm': ticker_name,
            'beginBasDt': start_dt,
            'numOfRows': '10'
        }
        
        res = requests.get(url, params=params, timeout=15)
        
        # 키 활성화 대기 중이거나 API 에러인 경우
        if "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in res.text:
            return "키활성화대기", False
        if res.text.startswith("<"):
            return "조회지연", False
            
        data = res.json()
        if 'item' not in data['response']['body']['items']:
            return "데이터없음", False
            
        items = data['response']['body']['items']['item']
        # 데이터가 1개인 경우(dict)와 여러 개인 경우(list) 통합 처리
        if isinstance(items, dict): items = [items]
        
        # 날짜순 정렬 후 최근 3거래일 합산
        items = sorted(items, key=lambda x: x['basDt'], reverse=True)
        
        inst_sum, frgn_sum = 0, 0
        for i in range(min(3, len(items))):
            inst_sum += int(items[i]['insttnPurNetQty'])
            frgn_sum += int(items[i]['frgnPurNetQty'])
            
        def format_val(val):
            if abs(val) >= 10000: return f"{'+' if val > 0 else ''}{round(val/10000, 1)}만"
            return f"{'+' if val > 0 else ''}{val}"
            
        is_hot = (frgn_sum > 0 or inst_sum > 0)
        return f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}", is_hot
    except:
        return "조회지연", False

def is_recent_operating_profit_positive(ticker_code):
    """영업이익 흑자 확인 (네이버)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        tables = pd.read_html(res.text, encoding='euc-kr')
        for df in tables:
            df.columns = [str(c) for c in df.columns]
            if any('영업이익' in str(row) for row in df.iloc[:,0]):
                val = pd.to_numeric(df.iloc[0, 1:11], errors='coerce').dropna()
                if len(val) > 0:
                    return val.iloc[-1] > 0
        return False
    except: return False

def analyze_stock(args):
    """폭풍전야 오리지널 필터 로직"""
    ticker, name, end_date = args
    try:
        df = fdr.DataReader(ticker, (end_date - timedelta(days=60)), end_date)
        if len(df) < 30: return None
        
        df['Val'] = df['Close'] * df['Volume']
        df['MA20_Vol'] = df['Volume'].rolling(window=20).mean()
        df['MA20_Price'] = df['Close'].rolling(window=20).mean()
        
        curr = df.iloc[-1]
        prev_close = df['Close'].iloc[-2]
        
        vol_ratio = (curr['Volume'] / df['MA20_Vol'].iloc[-1]) * 100
        day_return = (curr['Close'] - prev_close) / prev_close
        val_median = df['Val'].tail(20).median()
        val_count_10b = (df['Val'].tail(20) >= 1000000000).sum()

        # 🚀 [오리지널 핵심 조건]
        if curr['Close'] < df['MA20_Price'].iloc[-1]: return None  # 20일선 위
        if abs(day_return) > 0.03: return None                    # 주가 안정
        if vol_ratio > 35: return None                             # 거래 응축(35%)
        if val_median < 1500000000: return None                   # 중간값 15억↑
        if val_count_10b < 15: return None                         # 10억↑ 15일 이상

        if is_recent_operating_profit_positive(ticker):
            supply_info, is_hot = get_investor_data_public(name)
            return {
                'Name': name, 'Code': ticker, 'Ratio': round(vol_ratio, 1), 
                'MedianVal': round(val_median / 100000000, 1), 
                'Return': round(day_return * 100, 2),
                'Supply': supply_info, 'IsHot': is_hot
            }
    except: return None

def main():
    print(f"🚀 [폭풍전야] 오리지널 원복 엔진 가동...")
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    end_date = datetime.today()
    
    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    # 병렬 처리로 속도 유지
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 만족하는 종목 없음"
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(로직: 흑자+20일선 위+거래 급감35%↓+중간값 15억↑+공공데이터)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    try:
        headers = {'Content-Type': 'application/json'}
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers=headers)
        print("✅ 디스코드 전송 완료!")
    except:
        print("❌ 전송 실패")

if __name__ == "__main__":
    main()
