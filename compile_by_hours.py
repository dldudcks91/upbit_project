#%%
import requests 
from datetime import datetime , timedelta
import time
import pandas as pd


def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    krw_markets.remove('KRW-BTC')
    return krw_markets


markets = get_krw_markets()



#%%
old_list = list()
for market in markets:
    start_date = datetime(2025, 1, 21)    
    for hour_range in range(1):
        date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        # Change endpoint to hourly candles
        url = 'https://api.upbit.com/v1/candles/minutes/60'
        params = {
            'market': market,
            'count': 24,  # Each request will get 100 hours of data
            'to': date_str
        }
        
        headers = {"accept": "application/json"}
        response = requests.get(url, params=params, headers=headers)
        
        new_list = response.json()
        old_list.extend(new_list)
        # Adjust time delta to move back 100 hours instead of 100 days
        start_date -= timedelta(hours=100)
        
        print(market, len(old_list), start_date.strftime('%Y-%m-%d %H:%M:%S'))
        

#%%
total_list = list()
for data in old_list:
    if data == 'name':
        continue
    date = data['candle_date_time_utc']
    market = data['market'][4:]  # Remove 'KRW-' prefix
    price = data['opening_price']
    volume = data['candle_acc_trade_volume']
    amount = data['candle_acc_trade_price']
    
    total_list.append([date, market, price, volume, amount])
#%%
total_list = list()
for data in old_list:
    if data == 'name':
        continue
    
    date = data['candle_date_time_utc'].replace('T', ' ')
    market = data['market']
    opening_price = data['opening_price']
    high_price = data['high_price']
    volume = data['candle_acc_trade_volume']
    amount = data['candle_acc_trade_price']
    
    total_list.append([date, market,opening_price, high_price, volume,amount])
    
    #%%
df = pd.DataFrame(total_list)
df.columns = ['log_dt','market','opening_price','high_price','volume','amount']
df = df.sort_values(by = 'market')
df_unique = df.drop_duplicates(subset=['log_dt', 'market'])
#%%
price_pivot = df_unique.pivot(index='log_dt', columns='market', values='opening_price')
corr_matrix = price_pivot.corr()
#%%
duplicates = df[df.duplicated(keep=False)] 
#%%
import pymysql
conn = pymysql.connect(
host='127.0.0.1',  # 로컬호스트
port=3306,  # 동적으로 할당된 로컬 포트 
user='root',
passwd='an98',
db='upbit')
tables = ['tb_market_hour']
with conn.cursor() as cursor:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    
    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        
        columns =  cursor.fetchall()
        column_names = [column[0] for column in columns]
        
        df_unique
        for _, row in df_unique.iterrows():
            # 동적으로 컬럼과 값 생성
            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            # 각 행 삽입
            try:
                cursor.execute(sql, tuple(row))
            except:
                continue
    
        print(f'success migration table {table}')
    conn.commit()
#%%
with conn.cursor() as cursor:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()