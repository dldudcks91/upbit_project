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
import pandas as pd
import time


def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    
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
    url = "https://api.upbit.com/v1/ticker"
    try:
        # 최적화 포인트 1: 직접 join으로 markets 처리
        response = requests.get(url, params={"markets": ",".join(markets)})
        price_data = response.json()
        
        # 최적화 포인트 2: Dictionary Comprehension 사용
        return {
            ticker['market']: {formatted_time: ticker['trade_price']} 
            for ticker in price_data
        }
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return {}

# def get_current_time(current_time):   
    
#     if current_time.minute == 0:
#         # 0분일 때는 한 시간 전으로 가고 59분으로 설정
#         rounded_time = current_time.replace(hour=current_time.hour - 1, minute=59, second=0, microsecond=0)
#         # 만약 0시인 경우 이전 날 23시로 설정
#         if rounded_time.hour < 0:
#             rounded_time = rounded_time.replace(hour=23)
#     else:
#         # 그 외의 경우는 현재 분에서 1을 빼기
#         rounded_minutes = current_time.minute - 1
#         rounded_time = current_time.replace(minute=rounded_minutes, second=0, microsecond=0)
    
#     formatted_time = rounded_time.strftime('%Y-%m-%d %H:%M:%S')
#     return formatted_time
   
def get_current_time(current_time):
    # 현재 시간을 10초 단위로 내림
    
    rounded_seconds = (current_time.second // 10) * 10
    rounded_time = current_time.replace(second=rounded_seconds, microsecond=0)- timedelta(seconds=10)
    formatted_time = rounded_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time
#%%
#1. markets데이터 불러옴
markets = get_krw_markets()

#2. 현재가격 불러오기
#z = get_current_prices(markets)




r = connect_redis()

while True:
    try:
        formatted_time = get_current_time(datetime.now())

        keys = r.keys("trade_volume:*")

        price_dic = get_current_prices(markets,formatted_time)

        # 해시 구조에 맞게 볼륨 추출 로직 수정
        volume_dic = dict()
        for key in keys:
            # 키 구조가 "trade_volume:타임스탬프" 형식
            _, timestamp_ms = key.split(":")
            timestamp_ms = int(timestamp_ms)
            dt = datetime.fromtimestamp(timestamp_ms/1000)
            new_formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            
            if new_formatted_time == formatted_time:
                # 해당 타임스탬프의 모든 마켓 볼륨 가져오기
                market_volumes = r.hgetall(key)
                
                for market, volume in market_volumes.items():
                    volume_dic[market] = {formatted_time: volume}



        
        file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
        with open(file_path, 'r', encoding = 'utf-8') as file:
            yaml_data = yaml.safe_load(file)
            yaml_data = yaml_data['BASEBALL']
            
        connection = pymysql.connect(
        host=yaml_data['HOST'],
        user=yaml_data['USER'],
        password=yaml_data['PASSWORD'],
        db= 'upbit'
        )

        try:
            with connection.cursor() as cursor:
                
                sql = """
                SELECT * FROM tb_market_info
                """
                
                cursor.execute(sql)
                market_info_data = pd.DataFrame(cursor.fetchall())
                
                cursor.execute("SHOW COLUMNS FROM tb_market_info")


                market_info_columns = cursor.fetchall()
                market_info_columns =  [row[0] for row in market_info_columns]

                market_info_data.columns = market_info_columns
                
                
                
                total_list = list()
                for market in markets:
                    
                    
                    try:
                        volume = float(volume_dic[market][formatted_time])
                    except:
                        volume = 0
                    try:
                        price = float(price_dic[market][formatted_time])
                    except:
                        price = 0
                    amount = volume * price   
                    
                    foreigner_price = 0
                    values = (formatted_time, market, price, volume, amount, foreigner_price)
                    
                    total_list.append(values)
                
                
                sql = """
                    INSERT INTO tb_market
                    (log_dt, market, price, volume, amount, price_foreign) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                
                cursor.executemany(sql, total_list)
                connection.commit()
                print(f"[{datetime.now()}, {formatted_time}]: Successfully inserted {len(total_list)} records")
        except Exception as e:
            print(f"Error: {e}")
            connection.rollback()
    except Exception as e:
        print(f"Main loop error: {e}")

    finally:
        time.sleep(10)
#%%

    
    
    
    
    

