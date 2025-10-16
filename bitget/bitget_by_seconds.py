# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 20:14:47 2025

@author: user
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 13:09:37 2025

@author: user
"""

import os
import yaml
import json
from datetime import datetime, timedelta
import time
from contextlib import contextmanager

import redis
import requests
import pymysql
from pymysql import connections
import pandas as pd


class DatabasePool:
    """데이터베이스 연결 풀 관리 클래스"""
    def __init__(self, yaml_data, pool_size=5):
        self.config = {
            'host': yaml_data['HOST'],
            'user': yaml_data['USER'],
            'password': yaml_data['PASSWORD'],
            'db': 'bithumb',
            'charset': 'utf8mb4',
            'autocommit': False,
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.pool_size = pool_size
        self.connections = []
        self._create_pool()
    
    def _create_pool(self):
        """연결 풀 생성"""
        for _ in range(self.pool_size):
            try:
                conn = pymysql.connect(**self.config)
                self.connections.append(conn)
            except Exception as e:
                print(f"Connection pool creation error: {e}")
    
    @contextmanager
    def get_connection(self):
        """연결 풀에서 연결 가져오기"""
        connection = None
        try:
            if self.connections:
                connection = self.connections.pop()
                # 연결 상태 확인 및 재연결
                connection.ping(reconnect=True)
                yield connection
            else:
                # 풀이 비어있으면 새로운 연결 생성
                connection = pymysql.connect(**self.config)
                yield connection
        except Exception as e:
            print(f"Database connection error: {e}")
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            if connection:
                try:
                    # 연결을 풀에 반환
                    if len(self.connections) < self.pool_size:
                        self.connections.append(connection)
                    else:
                        connection.close()
                except:
                    pass



def get_current_prices(markets, formatted_time):
    url = "https://api.bitget.com/api/v2/mix/market/tickers"
    try:
        # 최적화 포인트 1: 직접 join으로 markets 처리
        
        params = {'productType': 'USDT-FUTURES'}
        response = requests.Session().get(url, params=params)
        response_data = response.json()
        future_data = None
        if response_data['msg'] == 'success':
            future_data = response_data['data']
            old_dic = list()
            for data in future_data:
                market = data['symbol']
                price = float(market['markPrice'])
                volume = float(data['quoteVolume'])
                funding_rate = float(data['fundingRate'])
                
                old_dic['market'] = [price, volume, funding_rate]
        # 최적화 포인트 2: Dictionary Comprehension 사용
        return old_dic
            
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return {}


def get_current_time(current_time):
    # 현재 시간을 10초 단위로 내림
    rounded_seconds = (current_time.second // 10) * 10
    rounded_time = current_time.replace(second=rounded_seconds, microsecond=0) - timedelta(seconds=10)
    formatted_time = rounded_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


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


def insert_market_data(db_pool, total_list):
    """배치 INSERT 최적화 함수"""
    if not total_list:
        return False
        
    # INSERT IGNORE 또는 ON DUPLICATE KEY UPDATE 사용으로 중복 데이터 처리
    sql = """
        INSERT INTO tb_market_bitget (log_dt, market, price,volume,funding_rate) 
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        price = VALUES(price)
    """
    
    try:
        with db_pool.get_connection() as connection:
            with connection.cursor() as cursor:
                # executemany는 이미 배치 처리되므로 그대로 사용
                cursor.executemany(sql, total_list)
                connection.commit()
                print(f"[{datetime.now()}]: Successfully inserted {len(total_list)} records")
                return True
    except Exception as e:
        print(f"Database insert error: {e}")
        return False


def main():
    """메인 실행 함수"""
    # 설정 파일 로드
    file_path = "/home/ubuntu/baseball_project/db_settings.yml"
    with open(file_path, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)
        yaml_data = yaml_data['BASEBALL']
    
    # 데이터베이스 연결 풀 생성
    db_pool = DatabasePool(yaml_data, pool_size=3)
    
    while True:
        try:
            wait_until_next_interval()
            formatted_time = get_current_time(datetime.now())
            
            market_dic = get_current_prices(formatted_time)
            
            # 데이터 준비 최적화
            total_list = []
            for item, value in market_dic.items():
                total_list.append([formatted_time, item] + value)
            
            # 배치 INSERT 실행
            if total_list:
                success = insert_market_data(db_pool, total_list)
                if not success:
                    print(f"Failed to insert data at {formatted_time}")
                    
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(1)  # 오류 발생 시 잠시 대기


if __name__ == "__main__":
    main()