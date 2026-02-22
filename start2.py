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

# ✅ 환경 설정
RAW_KEY = "62e0d95b35661ef8e1f9a665ef46cc7cd64a3ace4d179612dda40c847f6bdb7e"
PUBLIC_API_KEY = unquote(RAW_KEY) 
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_public(ticker_name):
    """공공데이터 API: 최근 3일 수급 추출"""
    try:
        url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getInvestorRegistrationStat"
        today = datetime.now()
        start_dt = (today - timedelta(days=10)).strftime('%Y%m%d')
        
        params = {
            'serviceKey': PUBLIC_API_KEY,
            'resultType': 'json',
            'itmsNm': ticker_name,
            'beginBasDt': start_dt,
            'numOfRows': '10'
        }
        
        res = requests.get(url, params=params, timeout=20)
        
        if "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in res.text:
            return "키활성화대기", False
            
        data = res.json()
        items = data['response']['body']['items']['item']
        if isinstance(items, dict): items = [items]
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
    """최신 공시 기준 영업이익 흑자 확인"""
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
    """폭풍전야 핵심 조건 매칭 (20/60 정배열 포함)"""
    ticker, name, end_date = args
    try:
        # 이평선 계산을 위해 80일치 데이터 로드
        df = fdr.DataReader(ticker, (end_date - timedelta(days=80)), end_date)
        if len(df) < 60: return None
        
        # 지표 계산
        df['Val'] = df['Close'] * df['Volume']
        df['MA20_Vol'] = df['Volume'].rolling(window=20).mean()
        df['MA20_Price'] = df['Close'].rolling(window=20).mean()
        df['MA60_Price'] = df['Close'].rolling(window=60).mean() # 60일선 추가
        
        curr = df.iloc[-1]
        prev_close = df['Close'].iloc[-2]
        
        vol_ratio = (curr['Volume'] / curr['MA20_Vol']) * 100
        day_return = (curr['Close'] - prev_close) / prev_close
        val_median = df['Val'].tail(20).median()
        val_count_10b = (df['Val'].tail(20) >= 1000000000).sum()

        # 🚀 [폭풍전야 무삭제 필터]
        # 1. 20일선 > 60일선 (정배열 확인) ✅ 추가됨
        if curr['MA20_Price'] < curr['MA60_Price']: return None
        
        # 2. 현재가 > 20일선 (위치 확인)
        if curr['Close'] < curr['MA20_Price']: return None  
        
        # 3. 등락률 -3% ~ +3% (안정성)
        if abs(day_return) > 0.03: return None                   
        
        # 4. 거래량 35% 이하 (응축)
        if vol_ratio > 35: return None                            
        
        # 5. 거래대금 중간값 15억 이상 (유동성)
        if val_median < 1500000000: return None                  
        
        # 6. 거래대금 10억 이상 15일 이상 (연속성)
        if val_count_10b < 15: return None 

        # 7. 영업이익 흑자 (펀더멘탈)
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
    print(f"🚀 [폭풍전야] 무삭제 정밀 로직 가동...")
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    end_date = datetime.today()
    
    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(analyze_stock, tasks))
    
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]
    
    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 조건을 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(로직: 흑자+20>60정배열+20선위+거래급감+대금유지+정식수급)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    try:
        headers = {'Content-Type': 'application/json'}
        payload = {"content": msg}
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
        print("✅ 디스코드 메시지 전송 완료!")
    except:
        print("❌ 디스코드 전송 실패")

if __name__ == "__main__":
    main()
