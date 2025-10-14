# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 17:00:57 2025

@author: user
"""

#%%
import asyncio
import aiohttp
from datetime import datetime , timedelta, timezone
import time
import pandas as pd
import yaml
import pymysql
import nest_asyncio
nest_asyncio.apply()

# 비동기 함수로 변경
async def get_krw_markets_async():
    """업비트 KRW 마켓의 모든 거래쌍 조회 (비동기)"""
    url = "https://api.bithumb.com/v1/market/all"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            markets = await response.json()
            # KRW 마켓만 필터링
            krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
            print(f"Total KRW markets: {len(krw_markets)}")
            return krw_markets

# 비동기 데이터 요청 함수
# 업데이트된 fetch_market_data 함수
async def fetch_market_data(session, market, start_time, data_cnt):
    date_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    url = 'https://api.bithumb.com/v1/candles/minutes/5'
    params = {
        'market': market,
        'count': data_cnt,
        'to': date_str
    }
    headers = {"accept": "application/json"}

    try:
        async with session.get(url, params=params, headers=headers) as response:
            # HTTP 상태 코드가 200번대인지 확인하고, 아니면 예외 발생
            response.raise_for_status()

            # 응답의 Content-Type이 JSON인지 확인
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                return await response.json()
            else:
                # JSON이 아닌 응답 처리
                print(f"경고: {market}에 대한 응답이 JSON 형식이 아닙니다. 상태 코드: {response.status}, Content-Type: {content_type}")
                return []  # 빈 리스트 반환하여 오류가 데이터 처리를 멈추지 않게 함
    except aiohttp.ClientResponseError as e:
        # 4xx 또는 5xx와 같은 HTTP 오류 처리
        print(f"{market}에 대한 HTTP 오류 발생: {e}")
        return []
    except Exception as e:
        # 그 외 요청 중 발생할 수 있는 모든 오류 처리
        print(f"{market} 데이터 요청 중 오류 발생: {e}")
        return []

# 비동기 메인 실행 함수 (수정된 부분)
async def main_async():
    markets = await get_krw_markets_async()
    
    current_time = datetime.now(tz=timezone.utc) 
    start_time = current_time + timedelta(hours = 9)
    DATA_CNT = 10
    
    old_list = []
    
    # 150개씩 배치로 나누어 요청 (수정된 부분)
    BATCH_SIZE = 150
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(markets), BATCH_SIZE):
            batch = markets[i:i + BATCH_SIZE]
            
            tasks = [fetch_market_data(session, market, start_time, DATA_CNT) for market in batch]
            responses = await asyncio.gather(*tasks)
            
            for response_data in responses:
                if isinstance(response_data, list) and response_data:
                    old_list.extend(response_data)
            
            # 다음 배치를 시작하기 전에 1초 대기
            if i + BATCH_SIZE < len(markets):
                print(f"다음 {BATCH_SIZE}개 요청을 위해 1초 대기...")
                await asyncio.sleep(1)

    print(f"총 수집된 데이터 포인트: {len(old_list)}개 ({len(markets)}개 마켓)")
    print(f"데이터 수집 시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return old_list, start_time

# 메인 함수 호출 및 실행
if __name__ == '__main__':
    old_list, start_time = asyncio.run(main_async())

#%%
tables = ['tb_market_5_minutes']

total_list = list()
for data in old_list:
    if data == 'name':
        continue
    
    date = data['candle_date_time_utc'].replace('T', ' ')
    market = data['market']
    opening_price = data['opening_price']
    trade_price = data['trade_price']
    high_price = data['high_price']
    low_price = data['low_price']
    volume = data['candle_acc_trade_volume']
    amount = data['candle_acc_trade_price']
    
    total_list.append([date, market,opening_price, trade_price, high_price, low_price, volume,amount])
    
#%%
df = pd.DataFrame(total_list)
df.columns = ['log_dt','market','opening_price','trade_price','high_price','low_pridce','volume','amount']
df = df.sort_values(by = 'market')
df_unique = df.drop_duplicates(subset=['log_dt', 'market'])

#%%
file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
with open(file_path, 'r', encoding = 'utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']

DB = 'bithumb'
conn = pymysql.connect(
    host=yaml_data['HOST'],
    user=yaml_data['USER'],
    password=yaml_data['PASSWORD'],
    db= DB
)

# 쿼리 실행
data_list = list()
with conn.cursor() as cursor:
    
    
    for table in tables:
        try:
            # 테이블 최대값 읽기
            
            
            
            # 데이터 가져오기
            cursor.execute(f"SELECT max(log_dt) as log_dt FROM {table}")
            rows = cursor.fetchall()
            
            # DataFrame 생성 시 컬럼명 지정
            log_dt = pd.DataFrame(rows, columns=['log_dt'])
            data_list.append(log_dt)
            
        
        except Exception as e:
            print(f"{table} 마이그레이션 중 오류: {e}")
conn.close()
#%%


old_df = data_list[0]
last_log_dt = old_df['log_dt'].max()
#%%
df_unique = df_unique[pd.to_datetime(df_unique['log_dt'])>last_log_dt]
print(df_unique.shape, last_log_dt)
#%%

#insert

conn = pymysql.connect(
    host=yaml_data['HOST'],
    user=yaml_data['USER'],
    password=yaml_data['PASSWORD'],
    db= DB
)



    
# 쿼리 실행
with conn.cursor() as cursor:
    
    
    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        
        columns =  cursor.fetchall()
        column_names = [column[0] for column in columns]
        
        
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
conn.close()
#%%
print('Start set ma')
conn = pymysql.connect(
    host=yaml_data['HOST'],
    user=yaml_data['USER'],
    password=yaml_data['PASSWORD'],
    db= DB
)
data_dic = dict()



now = (datetime.now(tz=timezone.utc) - timedelta(minutes = 5))

now -= timedelta(minutes=now.minute % 5, seconds=now.second, microseconds=now.microsecond)

ma_dic = dict()
with conn.cursor() as cursor:
    
    
    time_list = [10, 20, 34, 50 ,100, 144, 200, 400, 800]
        
    for time in time_list:      
        ago = (now - timedelta(minutes=time * 5)).strftime('%Y-%m-%d %H:%M:%S')     
        cursor.execute(f"SELECT market, avg(trade_price) as ma FROM tb_market_5_minutes WHERE log_dt > '{ago}' group by market")
            
        ma_data = pd.DataFrame(cursor.fetchall())
        
        ma_dic[time] = ma_data
    five_minute_ago = (now - timedelta(minutes = 5)).strftime('%Y-%m-%d %H:00:00')      
    cursor.execute(f"SELECT * FROM tb_ma_5_minutes where log_dt = '{five_minute_ago}'")
    last_ma_data = pd.DataFrame(cursor.fetchall())
#%%
markets = list(ma_data.iloc[:,0])
market_ma_dic = {market:[now.strftime('%Y-%m-%d %H:%M:%S')] for market in markets}

for market in markets:
    for time_ma in time_list:
        ma_time_data = ma_dic[time_ma]
        
        price_ma = ma_time_data[ma_time_data.iloc[:,0] == market]
        if price_ma.empty:
            price_ma = 0
        else:
            price_ma = price_ma.iloc[0,1]
        market_ma_dic[market].append(price_ma)
        
        if time_ma == 10:
            new_ma_10 = price_ma
        elif time_ma == 34:
            new_ma_34 = price_ma
    
    try:
        last_ma_market_data = last_ma_data[last_ma_data.iloc[:,0] == market]
        old_ma_10 = last_ma_market_data.iloc[0,2]
        old_ma_34 = last_ma_market_data.iloc[0,4]
    except:
        old_ma_10 = 0
        old_ma_34 = 0
    
    
    if (old_ma_10 < old_ma_34) & (new_ma_10 >= new_ma_34):
        is_golden_cross = 1
    else:
        is_golden_cross = 0
        
    if (old_ma_10 > old_ma_34) & (new_ma_10 <= new_ma_34):
        is_dead_cross = 1
    else:
        is_dead_cross = 0
    
    market_ma_dic[market].append(is_golden_cross)
    market_ma_dic[market].append(is_dead_cross)
    market_ma_dic[market].append(datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    #%%
    
input_data = pd.DataFrame(market_ma_dic).transpose().reset_index()
column_names = ['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_144','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at']
input_data.columns = column_names
conn = pymysql.connect(
    host=yaml_data['HOST'],
    user=yaml_data['USER'],
    password=yaml_data['PASSWORD'],
    db= DB
)
with conn.cursor() as cursor:
    
    
    
        
    
    for _, row in input_data.iterrows():
        # 동적으로 컬럼과 값 생성
        columns = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names))
        
        
        sql = f"INSERT INTO tb_ma_5_minutes ({columns}) VALUES ({placeholders})"
        
        # 각 행 삽입
        
        cursor.execute(sql, tuple(row))
        
        
    
conn.commit()

conn.close()
print(f'Complete All Task: {start_time} ')