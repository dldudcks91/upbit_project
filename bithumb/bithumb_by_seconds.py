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
import time

import redis
import requests
import pymysql
import pandas as pd



def get_krw_markets():
    """빗썸 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.bithumb.com/v1/market/all"
    response = requests.Session().get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    
    return krw_markets



def get_current_prices(markets, formatted_time):
    url = "https://api.bithumb.com/v1/ticker"
    try:
        # 최적화 포인트 1: 직접 join으로 markets 처리
        len_markets = len(markets)
        price_total_data = list()
        for i in range(len_markets//100+1):
            min_idx = i * 100
            max_idx = (i+1)*100
            response = requests.Session().get(url, params={"markets": ",".join(markets[min_idx:max_idx])})
            price_data = response.json()
            
            price_total_data.extend(price_data)
            print(len(price_data))
        # 최적화 포인트 2: Dictionary Comprehension 사용
        return {
            ticker['market']: {formatted_time: ticker['trade_price']} 
            for ticker in price_total_data
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



def wait_until_next_interval():
    """
    다음 10초 구간의 시작까지 대기
    ex) 현재 23초면 30초가 될 때까지 대기
    """
    now = datetime.now()
    next_interval = now + timedelta(seconds=10 - now.second % 10)
    next_interval = next_interval.replace(microsecond=0)
    sleep_seconds = (next_interval - now).total_seconds()
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

#1. markets데이터 불러옴
#markets = get_krw_markets()
#%%
while True:
    try:
        markets = get_krw_markets()
        wait_until_next_interval()
        formatted_time = get_current_time(datetime.now())

        price_dic = get_current_prices(markets,formatted_time)

        

        file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
        with open(file_path, 'r', encoding = 'utf-8') as file:
            yaml_data = yaml.safe_load(file)
            yaml_data = yaml_data['BASEBALL']
            
        connection = pymysql.connect(
        host=yaml_data['HOST'],
        user=yaml_data['USER'],
        password=yaml_data['PASSWORD'],
        db= 'bithumb'
        )

        try:
            with connection.cursor() as cursor:
                

                

                total_list = list()
                for market in markets:
                    
                    
                    try:
                        price = float(price_dic[market][formatted_time])
                    except:
                        price = None
                    
                    
                    
                    values = (formatted_time, market, price)
                    
                    total_list.append(values)
                
                
                sql = """
                    INSERT INTO tb_market
                    (log_dt, market, price) 
                    VALUES (%s, %s, %s)
                    """
                
                cursor.executemany(sql, total_list)
                connection.commit()
                
                print(f"[{datetime.now()}, {formatted_time}]: Successfully inserted {len(total_list)} records")
        except Exception as e:
            print(f"Error: {e}")
            connection.rollback()
    except Exception as e:
        print(f"Main loop error: {e}")

    
        
#%%

    
    
    
    
    

