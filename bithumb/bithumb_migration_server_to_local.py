# -*- coding: utf-8 -*-
"""
Created on Fri Sep 19 13:06:33 2025

@author: user
"""

import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
import sqlalchemy
import pandas as pd
# SSH 설정
ssh_host = '43.201.254.212'  # EC2 퍼블릭 IP
ssh_username = 'ubuntu'    # EC2 사용자명
ssh_pkey = paramiko.RSAKey.from_private_key_file("C:/Users/user/Desktop/python_authority/lyc_baseball_20241126.pem")  # 키페어 경로

# RDS 설정
rds_endpoint = 'database-1.ch08cm8s8t6j.ap-northeast-2.rds.amazonaws.com'  # RDS 엔드포인트
rds_port = 3306
rds_username = 'admin'
rds_password = 'Gnfkqhsh91!'
DB = 'bithumb'


#%%
with SSHTunnelForwarder(
    (ssh_host, 22),  # SSH 서버 주소와 포트
    ssh_username=ssh_username,
    ssh_pkey=ssh_pkey,  # 또는 ssh_password=ssh_password
    remote_bind_address=(rds_endpoint, 3306)  # RDS 엔드포인트와 MySQL 포트
) as tunnel:
    
    # 로컬 포트로 MySQL 연결
    conn = pymysql.connect(
        host='127.0.0.1',  # 로컬호스트
        port=tunnel.local_bind_port,  # 동적으로 할당된 로컬 포트 
        user=rds_username,
        passwd=rds_password,
        db=DB
    )
    
    # 쿼리 실행
    with conn.cursor() as cursor:
        # RDS 소스 데이터베이스 엔진 생성
        
        
        
        
        # 테이블 목록 가져오기
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        data_list = list()
        # 각 테이블 마이그레이션
        for table in tables:
            try:
                # 테이블 데이터 읽기
                
                cursor.execute(f"SHOW COLUMNS FROM {table}")
                columns = [column[0] for column in cursor.fetchall()]
                
                # 데이터 가져오기
                try:
                    cursor.execute(f"SELECT * FROM {table} order by log_dt desc limit 103000")
                except:
                    cursor.execute(f"SELECT * FROM {table} order by date desc limit 103000")
                    
                rows = cursor.fetchall()
                
                # DataFrame 생성 시 컬럼명 지정
                df = pd.DataFrame(rows, columns=columns)
                data_list.append(df)
                
                print(f"{table} 마이그레이션 완료")
            
            except Exception as e:
                print(f"{table} 마이그레이션 중 오류: {e}")
    conn.close()
tunnel.close()
#%%

ma_days = data_list[1]
#%%
from datetime import timedelta
market_df = data_list[2]
now_df = market_df.sort_values(by = 'log_dt', ascending = False).iloc[:414]

last_time = market_df['log_dt'].min() + timedelta(seconds = 10)
last_df = market_df[market_df['log_dt'] == last_time]
#%%
join_data = pd.merge(last_df,now_df,how = 'left',on = 'market')
join_data = pd.merge(join_data,ma_days,how = 'left',on = 'market')
#%%
join_data['price_ratio'] = join_data['price_y'] / join_data['price_x']
join_data['diff_200'] = join_data['price_y'] / join_data['ma_200']
#%%