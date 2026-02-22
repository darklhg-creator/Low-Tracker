import pandas as pd
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import warnings
import json
@@ -10,54 +9,63 @@

warnings.filterwarnings('ignore')

# ✅ 사용자 인증키 (캡처본 기반 입력 완료)
PUBLIC_API_KEY = "62e0d95b35661ef8e1f9a665ef46cc7cd64a3ace4d179612dda40c847f6bdb7e"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def get_investor_data_final(ticker):
    """네이버 금융 내부 데이터 경로에서 수급을 직접 추출하는 최후의 수단"""
def get_investor_data_public(ticker_name):
    """공공데이터 API를 통해 최근 3일 수급(기관/외인) 합계를 가져옴"""
    try:
        # 1. 내부 데이터 로드 경로
        url = f"https://finance.naver.com/item/frgn_investor_jindo.naver?code={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'
        # 캡처본의 End Point를 기반으로 한 수급 데이터 주소
        url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getInvestorRegistrationStat"
        
        # 최근 일주일치 데이터를 조회해서 그중 최근 3일을 추출
        today = datetime.now()
        start_dt = (today - timedelta(days=7)).strftime('%Y%m%d')
        
        params = {
            'serviceKey': PUBLIC_API_KEY,
            'resultType': 'json',
            'itmsNm': ticker_name,
            'beginBasDt': start_dt,
            'numOfRows': '10'
        }

        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        items = data['response']['body']['items']['item']
        if not items: return "데이터없음", False

        # 2. 수급 테이블 행 추출 (최근 3일치)
        # 테이블에서 '날짜', '종가', '전일비' 등을 제외하고 '기관', '외국인' 순매수량만 타겟팅
        rows = soup.select('tr')[2:5] # 상단 헤더 제외 최근 3거래일
        # 최신순 정렬 (데이터가 날짜순으로 오지 않을 경우 대비)
        items = sorted(items, key=lambda x: x['basDt'], reverse=True)

        inst_sum = 0
        frgn_sum = 0

        for row in rows:
            cols = row.select('td')
            if len(cols) >= 6:
                # 기관(5번째 열), 외인(6번째 열)
                inst_val = int(cols[4].get_text(strip=True).replace(',', ''))
                frgn_val = int(cols[5].get_text(strip=True).replace(',', ''))
                inst_sum += inst_val
                frgn_sum += frgn_val
        
        # 최근 3거래일 수급 합산
        for i in range(min(3, len(items))):
            inst_sum += int(items[i]['insttnPurNetQty'])
            frgn_sum += int(items[i]['frgnPurNetQty'])
            
        def format_val(val):
            if abs(val) >= 10000:
                return f"{'+' if val > 0 else ''}{round(val/10000, 1)}만"
            return f"{'+' if val > 0 else ''}{val}"

            
        is_hot = (frgn_sum > 0 or inst_sum > 0)
        return f"외인{format_val(frgn_sum)} / 기관{format_val(inst_sum)}", is_hot
    except:
        return "수급미비", False

def is_recent_operating_profit_positive(ticker_code):
    """영업이익 흑자 확인"""
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
@@ -79,18 +87,16 @@
        day_return = (curr['Close'] - df['Close'].iloc[-2]) / df['Close'].iloc[-2]
        val_median = df['Val'].tail(20).median()

        # 🚀 [폭풍전야 핵심 조건]
        # 🚀 [필터] 20일선 위 + 거래 급감(35% 이하) + 중간값 15억↑
        if curr['Close'] < df['MA20_Price'].iloc[-1]: return None  
        if abs(day_return) > 0.03: return None                   
        if vol_ratio > 35: return None                            
        if val_median < 1500000000: return None                  
        if (df['Val'].tail(20) >= 1000000000).sum() < 15: return None 

        if is_recent_operating_profit_positive(ticker):
            # 수급 데이터 로직 호출
            supply_info, is_hot = get_investor_data_final(ticker)
            # 차단 방지를 위한 미세 지연
            time.sleep(0.2) 
            # ✅ 공공데이터 API 수급 호출
            supply_info, is_hot = get_investor_data_public(name)
            return {
                'Name': name, 'Code': ticker, 'Ratio': round(vol_ratio, 1), 
                'MedianVal': round(val_median / 100000000, 1), 
@@ -102,31 +108,31 @@

def main():
    start_time = time.time()
    print(f"🚀 [폭풍전야] 최후의 수급 엔진 가동...")
    print(f"🚀 [폭풍전야] 공공데이터 정식 엔진 가동...")

    krx_df = fdr.StockListing('KRX')
    krx_df = krx_df[krx_df['Code'].str.match(r'^\d{5}0$')]
    ticker_dict = dict(zip(krx_df['Code'], krx_df['Name']))
    end_date = datetime.today()

    tasks = [(t, n, end_date) for t, n in ticker_dict.items()]
    # 차단 회피를 위해 워커 수를 2개로 줄여서 천천히, 하지만 확실하게 실행
    with ThreadPoolExecutor(max_workers=2) as executor:
    # API 방식은 병렬 처리를 해도 차단 위험이 낮습니다.
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(analyze_stock, tasks))

    final_picks = sorted([r for r in results if r is not None], key=lambda x: x['Ratio'])[:30]

    if not final_picks:
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 만족하는 종목이 없습니다."
        msg = f"📅 {end_date.strftime('%Y-%m-%d')} | 조건을 만족하는 종목이 없습니다."
    else:
        msg = f"🌪️ **[폭풍전야: 3일 수급 응축 TOP {len(final_picks)}]**\n"
        msg += "*(로직: 20일선 위+거래 급감+중간값 15억↑+정밀수급)*\n\n"
        msg += "*(로직: 흑자+20일선 위+거래 급감+중간값 15억↑+정식 API 수급)*\n\n"
        for p in final_picks:
            star = "⭐" if p['IsHot'] else ""
            msg += f"• {star}**{p['Name']}**({p['Code']}) | `{p['Ratio']}%` | `{p['MedianVal']}억` | `{p['Return']}%` | `[{p['Supply']}]` \n"

    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": msg}), headers={'Content-Type': 'application/json'})
    print(f"✅ 분석 완료! ({int(time.time() - start_time)}초)")

if __name__ == "__main__":
    main()
