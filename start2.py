# ... (기존 코드 생략)

        for idx, row in df_total.iterrows():
            code = row['Code']
            name = row['Name']
            try:
                # [수정] 일봉/주봉/월봉 분석을 위해 데이터를 넉넉히 가져옴 (약 1년치)
                df_daily = fdr.DataReader(code).tail(200) 
                if len(df_daily) < 60: continue
                
                # 1. 일봉 이격도 체크 (기존 로직)
                current_price = df_daily['Close'].iloc[-1]
                ma20_daily = df_daily['Close'].rolling(window=20).mean().iloc[-1]
                disparity = round((current_price / ma20_daily) * 100, 1)
                
                # 기본 조건: 이격도 95% 이하인 낙폭 과대주만 일단 대상
                if disparity > 95.0: continue

                # ---------------------------------------------------------
                # [추가] 주봉/월봉 상승전환 분석
                # ---------------------------------------------------------
                # 주봉 변환 (W: 일요일 기준 일주일)
                df_weekly = df_daily['Close'].resample('W').last()
                ma5_weekly = df_weekly.rolling(window=5).mean()
                
                # 주봉 상승전환 시그널: 5주선이 하락을 멈추고 상승하거나, 종가가 5주선 돌파
                is_weekly_up = (df_weekly.iloc[-1] > ma5_weekly.iloc[-1]) and (df_weekly.iloc[-2] <= ma5_weekly.iloc[-2])
                
                # 월봉 변환 (M: 월말 기준)
                df_monthly = df_daily['Close'].resample('M').last()
                # 월봉 상승전환 시그널: 이번 달 종가가 지난달 종가보다 높음 (양봉/반등)
                is_monthly_rebound = df_monthly.iloc[-1] > df_monthly.iloc[-2]

                # 최종 필터: 낙폭과대(이격도) + 주봉 돌파 + 월봉 반등
                if is_weekly_up or is_monthly_rebound:
                    all_analyzed.append({
                        'name': name, 
                        'code': code, 
                        'disparity': disparity,
                        'status': "주봉돌파" if is_weekly_up else "월봉반등"
                    })
            except:
                continue

# ... (결과 보고 및 디스코드 전송 로직)
