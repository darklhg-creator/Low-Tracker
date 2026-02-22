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

# ✅ [체크] 1. 마이페이지의 'Decoding' 인증키를 넣으세요
# ✅ 환경 설정
RAW_KEY = "62e0d95b35661ef8e1f9a665ef46cc7cd64a3ace4d179612dda40c847f6bdb7e"
PUBLIC_API_KEY = unquote(RAW_KEY) 

# ✅ [체크] 2. 본인의 디스코드 웹훅 주소가 맞는지 확인하세요
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_public(ticker_name):
@@ -34,16 +32,10 @@

        res = requests.get(url, params=params, timeout=20)

        # 상세 에러 메시지 분석
        if "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in res.text:
            return "키활성화대기", False
        if res.text.startswith("<"):
            return "API점검", False

        data = res.json()
        if 'item' not in data['response']['body']['items']:
            return "업데이트전", False
            
        items = data['response']['body']['items']['item']
        if isinstance(items, dict): items = [items]
        items = sorted(items, key=lambda x: x['basDt'], reverse=True)
@@ -63,40 +55,63 @@
        return "조회지연", False

def is_recent_operating_profit_positive(ticker_code):
    """최신 공시 영업이익 흑자 확인"""
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
                val = pd.to_numeric(df.iloc[0, 1:11], errors='coerce').dropna()
                if len(val) > 0:
                    return val.iloc[-1] > 0
        return False
    except: return False

def analyze_stock(args):
    """폭풍전야 핵심 조건 매칭 (20/60 정배열 포함)"""
    ticker, name, end_date = args
    try:
        df = fdr.DataReader(ticker, (end_date - timedelta(days=60)), end_date)
        if len(df) < 30: return None
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
        day_return = (curr['Close'] - df['Close'].iloc[-2]) / df['Close'].iloc[-2]
        day_return = (curr['Close'] - prev_close) / prev_close
        val_median = df['Val'].tail(20).median()
        val_count_10b = (df['Val'].tail(20) >= 1000000000).sum()

        if curr['Close'] < df['MA20_Price'].iloc[-1]: return None  
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
        if (df['Val'].tail(20) >= 1000000000).sum() < 15: return None 
        
        # 6. 거래대금 10억 이상 15일 이상 (연속성)
        if val_count_10b < 15: return None 

        # 7. 영업이익 흑자 (펀더멘탈)
        if is_recent_operating_profit_positive(ticker):
            supply_info, is_hot = get_investor_data_public(name)
            return {
@@ -108,7 +123,7 @@
    except: return None

def main():
    print(f"🚀 [폭풍전야] 분석 시작...")
    print(f"🚀 [폭풍전야] 무삭제 정밀 로직 가동...")
    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
@@ -121,25 +136,21 @@
    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]

    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 조건 만족 종목 없음"
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 조건을 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(로직: 흑자+20일선 위+거래 급감+중간값 15억↑+공공데이터)*\n\n"
        msg += "*(로직: 흑자+20>60정배열+20선위+거래급감+대금유지+정식수급)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    # ✅ 이 부분이 디스코드 전송 핵심 코드입니다!
    try:
        payload = {"content": msg}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
        if response.status_code == 204:
            print("✅ 디스코드 메시지 전송 성공!")
        else:
            print(f"❌ 디스코드 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ 디스코드 통신 오류: {e}")
        payload = {"content": msg}
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
        print("✅ 디스코드 메시지 전송 완료!")
    except:
        print("❌ 디스코드 전송 실패")

if __name__ == "__main__":
    main()
