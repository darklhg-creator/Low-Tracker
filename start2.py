import FinanceDataReader as fdr
import OpenDartReader
from pykrx import stock
import requests
import pandas as pd
from datetime import datetime
import time

# [설정]
DART_API_KEY = '732bd7e69779f5735f3b9c6aae3c4140f7841c3e'
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT'
dart = OpenDartReader(DART_API_KEY)

def send_discord(content):
    """디스코드 메시지 전송 (글자 수 제한 대응)"""
    if len(content) > 1900:
        chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
        for chunk in chunks:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk})
    else:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})

def get_market_data():
    """오늘 전체 종목의 등락률과 수급 데이터를 미리 로드 (pykrx 사용)"""
    # 깃허브 액션 서버 시간(UTC)을 고려하여 오늘 날짜 계산
    today = datetime.now().strftime("%Y%m%d")
    try:
        # 수급 데이터 (순매수량)
        df_investor = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL")
        # 종가 및 등락률 데이터
        df_price = stock.get_market_price_change(today, today)
        return df_investor, df_price
    except:
        print("시장 데이터를 가져오는 데 실패했습니다.")
        return pd.DataFrame(), pd.DataFrame()

def main():
    print("🚀 스크리닝 시작 (KOSPI 500 / KOSDAQ 1000)...")
    df_inv, df_prc = get_market_data()
    
    # 1. 대상 종목 수집 (KRX 전체)
    # KeyError 방지를 위해 컬럼 존재 여부와 관계없이 처리
    try:
        df_krx = fdr.StockListing('KRX')
    except:
        print("종목 리스트를 가져올 수 없습니다.")
        return

    # ETF/ETN 제외 필터링: Sector(업종) 정보가 없는 종목은 제외
    # fdr 버전에 따라 컬럼명이 다를 수 있으므로 체크
    sector_col = 'Sector' if 'Sector' in df_krx.columns else 'Industry'
    if sector_col in df_krx.columns:
        df_krx = df_krx.dropna(subset=[sector_col])
    
    # 시총 상위 필터링 (MarketId로 구분)
    kospi_targets = df_krx[df_krx['Market'].str.contains('KOSPI', na=False)].head(500)
    kosdaq_targets = df_krx[df_krx['Market'].str.contains('KOSDAQ', na=False)].head(1000)
    total_targets = pd.concat([kospi_targets, kosdaq_targets])
    
    found_stocks = []
    print(f"분석 대상 종목 수: {len(total_targets)}개")

    for _, row in total_targets.iterrows():
        code, name = row['Code'], row['Name']
        
        # 1. 이격도 계산 (20일 이동평균선 기준)
        try:
            # 최근 50일치 데이터로 이격도 계산
            df_hist = fdr.DataReader(code, (datetime.now() - pd.Timedelta(days=60)).strftime('%Y-%m-%d'))
            if len(df_hist) < 20: continue
            
            ma20 = df_hist['Close'].rolling(window=20).mean().iloc[-1]
            current_price = df_hist['Close'].iloc[-1]
            disp = (current_price / ma20) * 100
            
            # 조건 1: 이격도 90 이하
            if disp <= 90:
                # 2. DART 영업이익 팩트체크 (흑자 여부)
                # 2026년 기준: 24년(연간), 25년 3분기(최근 분기)
                ann = dart.finstate_all(name, 2024, '11011')
                ann_op_row = ann[ann['account_nm'] == '영업이익']
                
                qua = dart.finstate_all(name, 2025, '11014')
                qua_op_row = qua[qua['account_nm'] == '영업이익']
                
                if not ann_op_row.empty and not qua_op_row.empty:
                    ann_op = int(ann_op_row['thstrm_amount'].values[0].replace(',', ''))
                    qua_op = int(qua_op_row['thstrm_amount'].values[0].replace(',', ''))
                    
                    # 조건 2: 연간/최근 분기 모두 흑자
                    if ann_op > 0 and qua_op > 0:
                        # 3. 수급 및 등락률 매칭 (pykrx 데이터 활용)
                        change = df_prc.loc[code, '등락률'] if code in df_prc.index else 0
                        f_net = df_inv.loc[code, '외국인'] if code in df_inv.index else 0
                        i_net = df_inv.loc[code, '기관합계'] if code in df_inv.index else 0
                        
                        found_stocks.append(
                            f"✅ **{name}** ({code})\n"
                            f"└ 이격도: **{disp:.2f}** | 등락률: {change:.2f}%\n"
                            f"└ 수급(주): 外 {f_net:,} / 機 {i_net:,}\n"
                            f"└ 영업이익: '24년({format(ann_op, ',')}원), '25.3Q({format(qua_op, ',')}원)"
                        )
                        print(f"조건 부합 종목 발견: {name}")
                
                # DART API 과부하 방지
                time.sleep(0.1)
        except:
            continue

    # [결과 전송]
    now_tag = datetime.now().strftime('%Y-%m-%d %H:%M')
    if found_stocks:
        header = f"📊 **[{now_tag}] 스캔 결과 (이격도 90↓ & 흑자)**\n\n"
        send_discord(header + "\n".join(found_stocks))
    else:
        send_discord(f"🔍 [{now_tag}] 조건에 부합하는 종목이 없습니다.")

if __name__ == "__main__":
    main()
