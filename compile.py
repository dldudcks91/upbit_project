# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 13:09:37 2025

@author: user
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 23:02:15 2025

@author: 82109
"""
# 모든 키 조회

import os
import yaml
import json
from datetime import datetime, timedelta

import redis
import requests
import pymysql




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


def connect_redis():
    try:
        r = redis.Redis(
            host='localhost', 
            port=6379, 
            db=0, 
            decode_responses=True,
            socket_connect_timeout=5
        )
        r.ping()  # Redis 서버 연결 테스트
        print("Successfully connected to Redis")
        return r
    except redis.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")
        print("Please make sure Redis server is running")
        raise




def get_current_prices(markets, formatted_time):
  
   # 업비트 API 호출
   url = "https://api.upbit.com/v1/ticker"
   #markets = ["KRW-BTC,KRW-ETH,KRW-XRP"]  # 원하는 마켓 추가
   markets = markets
   data_dic = dict()
   try:
       response = requests.get(url, params={"markets": markets})
       price_data = response.json()
       

       
       
       for ticker in price_data:
           market = ticker['market']
           data_dic[market] = dict()
           data_dic[market][formatted_time] = ticker['trade_price']
           
           

       
       

   except Exception as e:
       print(f"Error: {e}")
   return data_dic

def get_current_time(current_time):   
    
    rounded_minutes = (current_time.minute // 5) * 5 
    rounded_time = current_time.replace(minute=rounded_minutes, second=0, microsecond=0) - timedelta(minutes=5)
    formatted_time = rounded_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time
   
    
#%%
#1. markets데이터 불러옴
markets = get_krw_markets()

#2. 현재가격 불러오기
#z = get_current_prices(markets)
r = connect_redis()

formatted_time = get_current_time(datetime.now())


keys = r.keys("trade_volume:*")
count = 0

price_dic = get_current_prices(markets,formatted_time)
volume_dic =dict()
for key in keys:
    volume = r.get(key)
    _, market, timestamp_ms = key.split(":")
    timestamp_ms = int(timestamp_ms)
    dt = datetime.fromtimestamp(timestamp_ms/1000)
    new_formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if new_formatted_time == formatted_time:
        
        volume_dic[market] = dict()
        
        volume_dic[market][formatted_time] = volume
        count+=1
    else:
        pass

total_list = list()
for market in markets:
    
    try:
        volume = volume_dic[market][formatted_time]
    except:
        volume = 0
    try:
        price = price_dic[market][formatted_time]
    except:
        price = 0
    
    values = (formatted_time, market, price,volume)
    total_list.append(values)
#%%



# 사용 예시
file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
with open(file_path, 'r', encoding = 'utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']


#MySQL 연결


connection = pymysql.connect(
   host=yaml_data['HOST'],
   user=yaml_data['USER'],
   password=yaml_data['PASSWORD'],
   db= 'upbit'
)

try:
    with connection.cursor() as cursor:
        
        sql = sql = """
               INSERT INTO tb_market
               (log_dt, market, price, volume) 
               VALUES (%s, %s, %s, %s)
               """
        
        cursor.executemany(sql, total_list)
        connection.commit()
        print(f"Successfully inserted {len(total_list)} records")
except Exception as e:
       print(f"Error: {e}")
       connection.rollback()
#%%

    
    
    
    
    

