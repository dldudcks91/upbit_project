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
#%%
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
    now_utc = now_utc.replace(second=0, microsecond=0)
    return now_utc

file_path = "/home/ubuntu/baseball_project/db_settings.yml" 
with open(file_path, 'r', encoding='utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']

DB = 'bithumb' # 데이터베이스 이름 수정 필요
db_manager = DatabaseManager(yaml_data, DB)


now_utc = get_current_time()
last_utc = now_utc - timedelta(seconds = 60)

utc_list = [now_utc, last_utc]
table = 'tb_market_bitget'
data_list = list()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        for utc in utc_list:
            try:
                cursor.execute(f'SELECT market, price FROM {table} WHERE log_dt = "{utc}"')
                rows = cursor.fetchall()
                new_data = pd.DataFrame(rows)
                data_list.append(new_data)
                print(new_data.shape)
            except Exception as e:
                print(f"{table} 조회 중 오류: {e}")
                
                