import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

# 🔴 디스코드 웹후크 URL
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
    print(f"🚀 [국내 주식 TOP 30 테스트 모드] {target_date} 기준으로 탐색을 시작합니다.")

    # 🌟 실전에서는 이 주석(#) 3줄을 지워서 주말 알림을 켜주세요! 🌟
    # if today_dt.weekday() >= 5:
    #     msg = f"💤 **[{today_dt.strftime('%Y-%m-%d')}]** 오늘은 주말(토/일)입니다. 국내 주식 탐색을 쉬어갑니다!"
    #     send_discord_message(msg)
    #     return
    
    try:
        # 3. [변경] 일반 주식(코스피, 코스닥 전체) 시세 가져오기
        df_today = stock.get_market_ohlcv_by_ticker(target_date, market="ALL")
        
        if df_today.empty:
            msg = f"💤 **[{target_date}]** 오늘 거래 데이터가 없습니다. (공휴일 등 휴장일로 판단되어 탐색을 쉬어갑니다!)"
            print(msg)
            send_discord_message(msg)
            return

        candidates = []
        
        # 4. 오늘 100억 이상 터진 찐 주도주 1차 필터링
        for ticker, row in df_today.iterrows():
            name = stock.get_market_ticker_name(ticker) # 주식 종목명 가져오기
            
            # [변경] 스팩주, 우선주, 리츠 등 주도주와 거리가 먼 종목 제외
            if "스팩" in name or name.endswith("우") or name.endswith("우B") or name.endswith("우C") or "리츠" in name:
                continue
            
            try:
                today_amt = row['거래대금']
            except:
                today_amt = row.iloc[3] * row.iloc[4] # 종가 * 거래량
            
            # [변경] 개별 주식은 후보가 너무 많으므로 최소 100억 이상으로 커트라인 상향
            if today_amt >= 10_000_000_000: 
                candidates.append((ticker, name, today_amt))
                
        print(f"🔍 1차 필터링: 100억 이상 터진 주식 {len(candidates)}개 발견. 상세 분석 중...")
        
        results = []
        
        # 5. 과거 데이터 비교 (당일 거래대금 폭발력 계산)
        for ticker, name, today_amt in candidates:
            df = stock.get_market_ohlcv_by_date(start_date, target_date, ticker)
            
            if df.empty or len(df) < 10: continue
            
            past_df = df.iloc[:-1].tail(20)
            past_amts = past_df['종가'] * past_df['거래량']
            avg_amt = past_amts.mean()
            
            if avg_amt > 0:
                ratio = today_amt / avg_amt
                results.append({
                    '종목명': name,
                    '폭발(배)': round(ratio, 2),
                    '당일(억)': round(today_amt / 100_000_000, 1),
                    '평균(억)': round(avg_amt / 100_000_000, 1) # 컬럼명을 짧게 줄임 (디스코드 가독성)
                })

        # 6. 결과 정렬 (TOP 30) 및 디스코드 전송
        if results:
            # [변경] 상위 30개 추출
            final_df = pd.DataFrame(results).sort_values(by='폭발(배)', ascending=False).head(30)
            
            print("\n" + "=" * 60)
            print(f"🔥 [순수 국내 개별주식 주도주 TOP 30]")
            print("-" * 60)
            print(final_df.to_string(index=False))
            print("=" * 60)
            
            # 디스코드 메시지 포맷팅
            discord_msg = f"🔥 **[국내 개별주식 수급 폭발 TOP 30]** (테스트 발송 - {target_date})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            discord_msg += "💡 당일 거래대금 100억 이상 종목 중, 20일 평균 대비 자금이 가장 많이 몰린 순위입니다."
            
            send_discord_message(discord_msg)
            
        else:
            print("조건에 맞는 주식 종목이 없습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
