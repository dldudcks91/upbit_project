# -*- coding: utf-8 -*-
"""
Created on Mon Oct 27 13:13:30 2025

@author: user
"""
import requests
from datetime import datetime, timezone, timedelta
#%%

curl  = "https://api.bitget.com/api/v2/mix/market/tickers"
params = {'productType': 'usdt-futures'}

response = requests.get(curl, params = params)
ticker_data = response.json()
markets = [ticker['symbol'] for ticker in ticker_data['data']]
#%%
now_utc = datetime.now(timezone.utc)
    
# 2. endTime 계산 (현재 시간의 00분으로 내림)
# 초와 마이크로초를 0으로 설정하여 정각으로 만듭니다.
end_dt_utc = now_utc.replace(minute=0, second=0, microsecond=0)

# 3. startTime 계산 (endTime보다 200시간 전)
time_difference = timedelta(hours=100)
start_dt_utc = end_dt_utc - time_difference

start_time_ms = int(start_dt_utc.timestamp() * 1000)
end_time_ms = int(end_dt_utc.timestamp() * 1000)

curl =  "https://api.bitget.com/api/v2/mix/market/history-candles"
params = {'symbol':'BTCUSDT',
          'productType': 'usdt-futures',
          'granularity':'1H',
          'start_time': start_time_ms,
          'end_time': end_time_ms,
          'limit': 10}


response = requests.get(curl, params = params)


#%%
z = response.json()