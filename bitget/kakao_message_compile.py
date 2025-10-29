# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 14:49:35 2025

@author: user
"""
#%%
import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
import yaml
import pymysql
from contextlib import contextmanager
import json
import numpy as np
import kakao_message_sender
#%%
class DatabaseManager:
# ... (이하 동일) ...
    """데이터베이스 연결 관리 클래스 (기존 코드와 동일)"""
    def __init__(self, yaml_data, db_name):
        self.config = {
            'host': yaml_data['HOST'],
            'user': yaml_data['USER'],
            'password': yaml_data['PASSWORD'],
            'db': db_name,
            'charset': 'utf8mb4',
            'autocommit': False
        }
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """연결 컨텍스트 매니저"""
        connection = None
        try:
            connection = pymysql.connect(**self.config)
            yield connection
        except Exception as e:
            print(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
# 현재 UTC 시간을 기준으로 한국 시간 (UTC+9)의 '정시'를 계산하는 유틸리티 함수
def get_current_time():
# ... (이하 동일) ...
    """현재 한국 시간(KST)의 정각(00분 00초) datetime 객체 반환"""
    now_utc = datetime.now(timezone.utc)
    # KST로 변환
    # 정시로 내림 (replace)
    now_utc = now_utc.replace(second=0, microsecond=0) - timedelta(seconds =10)
    return now_utc

file_path = "/home/ubuntu/baseball_project/db_settings.yml" 
with open(file_path, 'r', encoding='utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']

DB = 'bithumb' # 데이터베이스 이름 수정 필요
db_manager = DatabaseManager(yaml_data, DB)

#5분전 기준 5%이상 변화한 코인 찾는 로직
# now_utc = get_current_time()
# last_utc = now_utc - timedelta(seconds = 300)
# utc_list = [now_utc, last_utc]
# table = 'tb_market_bitget'
# data_list = list()
# with db_manager.get_connection() as conn:
#     with conn.cursor() as cursor:
#         for utc in utc_list:
#             try:
#                 cursor.execute(f'SELECT market, price FROM {table} WHERE log_dt = "{utc}"')
#                 rows = cursor.fetchall()
#                 new_data = pd.DataFrame(rows)
#                 data_list.append(new_data)
#                 print(new_data.shape)
#             except Exception as e:
#                 print(f"{table} 조회 중 오류: {e}")


# now_data, last_data = data_list

# now_data.columns = ['market','now_price']
# last_data.columns = ['market','last_price']
# total_data = pd.merge(now_data, last_data, on = 'market')
# total_data['diff'] = (total_data['now_price'] - total_data['last_price']) / total_data['last_price']
# filtered_data = total_data[total_data['diff'].abs() >= 0.05]

# # 결과 확인
# print(f'{now_utc} 캐치된 데이터:', filtered_data)

# kakao_sender = kakao_message_sender.setup_kakao_sender()
# message_lines = []

# # filtered_data의 각 행을 순회하며 메시지 문자열 생성 및 리스트에 추가
# for i, row in filtered_data.iterrows():
#     market = row['market']
#     diff = row['diff']
#     # .1f% 포맷팅을 사용하여 메시지 생성
#     line = f"{market}: 5분전 대비 {diff * 100:.1f}%"
#     message_lines.append(line)

# # 리스트에 담긴 모든 메시지 문자열을 줄바꿈 문자(\n)로 연결하여 하나의 최종 메시지 생성
# final_message = "\n".join(message_lines)
now_utc = get_current_time()
now_utc.replace(hour = 0, minute = 0) - timedelta(hours=1)
last_utc = now_utc - timedelta(hours = 1)

#utc_list = [now_utc, last_utc]
table = 'tb_market_hour_bitget'
data_list = list()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        
        try:
            cursor.execute(f'SELECT * FROM {table} WHERE log_dt = "{now_utc}"')
            rows = cursor.fetchall()
            new_data = pd.DataFrame(rows)
            data_list.append(new_data)
            print(now_utc, "data shape:", new_data.shape)
        except Exception as e:
            print(f"{table} 조회 중 오류: {e}")

now_data = data_list[0]

now_data.columns = ['log_dt','market','opening_price','trade_price','high_price','low_price','volume','amount']

now_data['range'] = (now_data['high_price'] - now_data['low_price']) / now_data['low_price']
now_data['diff'] = (now_data['trade_price'] - now_data['opening_price']) / now_data['opening_price']

filtered_data = now_data[now_data['range'].abs() >= 0.1]
print(f'{now_utc} 캐치된 데이터:', filtered_data)

kakao_sender = kakao_message_sender.setup_kakao_sender()
message_lines = ['지난 1시간동안 10%이상 변동']

# filtered_data의 각 행을 순회하며 메시지 문자열 생성 및 리스트에 추가
for i, row in filtered_data.iterrows():
    market = row['market']
    range_data = row['range']
    diff = row['diff']
    
    # .1f% 포맷팅을 사용하여 메시지 생성
    line = f"{market}) range: {range * 100:.1f}%, diff: {diff * 100:.1f}%"
    message_lines.append(line)
    
final_message = "\n".join(message_lines)
# 최종 메시지를 한 번에 전송
if final_message: # 데이터가 있을 경우에만 전송
    kakao_sender.send_text_message(final_message)


