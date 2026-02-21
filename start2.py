import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

# 🔴 [수정 완료] 새로운 디스코드 웹후크 URL 적용
WEBHOOK_URL = "https://discord.com/api/webhooks/1474739516177911979/IlrMnj_UABCGYJiVg9NcPpSVT2HoT9aMNpTsVyJzCK3yS9LQH9E0WgbYB99FHVS2SUWT"

def send_discord_message(msg_content):
    """디스코드로 메시지를 전송하는 함수"""
    payload = {"content": msg_content}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("✅ 디스코드 알림 전송 완료!")
        else:
            print(f"⚠️ 디스코드 전송 실패 (상태 코드: {response.status_code})")
    except Exception as e:
        print(f"❌ 디스코드 전송 중 에러 발생: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today_dt = datetime.now(KST)
    
    # 🌟 [주말 테스트 강제 세팅] 🌟
    target_date = "20260220" # 금요일 데이터로 테스트
    start_date = "20260115"
    
    print(f"📅 실행일시: {today_dt.strftime('%Y-%m-%d %H:%M:%S')} (KST)")
    print(f"🚀 [폭풍전야 눌림목 탐색 모드] {target_date} 기준으로 탐색을 시작합니다.")

    # 🌟 실전에서는 아래 주석(#) 3줄을 지워서 주말 알림을 켜주세요! 🌟
    # if today_dt.weekday() >= 5:
    #     msg = f"💤 **[{today_dt.strftime('%Y-%m-%d')}]** 오늘은 주말(토/일)입니다. 탐색을 쉬어갑니다!"
    #     send_discord_message(msg)
    #     return
    
    try:
        # 1. 오늘 주식 시세 가져오기
        df_today = stock.get_market_ohlcv_by_ticker(target_date, market="ALL")
        
        if df_today.empty:
            msg = f"💤 **[{target_date}]** 오늘 거래 데이터가 없습니다. (휴장일 판단)"
            print(msg)
            send_discord_message(msg)
            return

        # 2. [핵심] 재무 필터링: 최근 공시 기준 펀더멘털 데이터 수집 (EPS 흑자 확인용)
        print("📊 재무 데이터(EPS)를 확인하여 흑자 기업만 1차로 걸러냅니다...")
        df_fund = stock.get_market_fundamental_by_ticker(target_date, market="ALL")
        
        candidates = []
        
        # 3. 1차 필터링: 흑자 기업 & 스팩/우선주 제외 & 오늘 최소 거래대금 10억 이상
        for ticker, row in df_today.iterrows():
            name = stock.get_market_ticker_name(ticker)
            
            # 노이즈 종목 제외
            if "스팩" in name or name.endswith("우") or name.endswith("우B") or name.endswith("우C") or "리츠" in name:
                continue
                
            # EPS(주당순이익)가 0 이하인 적자 기업 철저히 배제
            if ticker in df_fund.index:
                eps = df_fund.loc[ticker, 'EPS']
                if pd.isna(eps) or eps <= 0:
                    continue
            else:
                continue # 재무 데이터 없으면 패스
            
            try:
                today_amt = row['거래대금']
            except:
                today_amt = row.iloc[3] * row.iloc[4]
                
            today_close = row['종가']
            today_change = row['등락률']
            today_vol = row['거래량']
            
            # 오늘 너무 많이 오르거나 내린 종목 제외 (±3% 이내의 눌림목만), 동전주 제외
            if abs(today_change) <= 3.0 and today_close >= 1000 and today_amt >= 1_000_000_000:
                candidates.append((ticker, name, today_close, today_vol, today_change))
                
        print(f"🔍 1차 필터링: 조건에 맞는 흑자/눌림목 후보 {len(candidates)}개 발견. 과거 수급 분석 중...")
        
        results = []
        
        # 4. 과거 20일 데이터와 비교 (거래량 급감 및 추세 확인)
        for ticker, name, today_close, today_vol, today_change in candidates:
            df = stock.get_market_ohlcv_by_date(start_date, target_date, ticker)
            
            if df.empty or len(df) < 20: continue
            
            past_df = df.iloc[:-1].tail(20) # 오늘 제외 과거 20일
            avg_vol = past_df['거래량'].mean()
            avg_amt = (past_df['종가'] * past_df['거래량']).mean()
            ma_20_close = past_df['종가'].mean() # 20일 이동평균선
            
            if avg_vol > 0:
                vol_ratio = today_vol / avg_vol
                
                # [선취매 최종 조건]
                # 1. 20일 평균 거래대금 50억 이상 (원래 끼가 있는 주도주)
                # 2. 오늘 종가가 20일 이평선 위 (상승 추세 안 깨짐)
                # 3. 오늘 거래량이 평균의 35% 이하로 바짝 마름
                if avg_amt >= 5_000_000_000 and today_close >= ma_20_close and vol_ratio <= 0.35:
                    results.append({
                        '종목명': name,
                        '거래비율(%)': round(vol_ratio * 100, 1), # 거래량이 평균의 몇 %인지
                        '평균대금(억)': round(avg_amt / 100_000_000, 1),
                        '오늘등락(%)': round(today_change, 2)
                    })

        # 5. 결과 정렬 (거래량이 가장 심하게 마른 순서대로) 및 디스코드 전송
        if results:
            # 거래비율이 '낮은' 순서대로 정렬 (완벽하게 메말라버린 종목이 1위)
            final_df = pd.DataFrame(results).sort_values(by='거래비율(%)', ascending=True).head(30)
            
            print("\n" + "=" * 60)
            print(f"🤫 [폭풍전야: 수급 응축 및 눌림목 TOP 30]")
            print("-" * 60)
            print(final_df.to_string(index=False))
            print("=" * 60)
            
            # 디스코드 메시지
            discord_msg = f"🤫 **[폭풍전야: 수급 응축 눌림목 TOP 30]** (테스트 - {target_date})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            discord_msg += "💡 (조건) 흑자 기업 + 20일선 위 + 변동성 3% 이내 + **평소 대비 거래량 35% 이하 급감**"
            
            send_discord_message(discord_msg)
            
        else:
            print("조건에 맞는 눌림목 종목이 없습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
