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
                    cursor.execute(f"SELECT * FROM {table} order by log_dt desc limit 10000")
                except:
                    try:
                        cursor.execute(f"SELECT * FROM {table} order by date desc limit 10000")
                        
                    except: 
                        cursor.execute(f"SELECT * FROM {table}")
                    
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

#%%
import mysql.connector
# 사용 예

def truncate_all_tables(host, user, password, database):
    try:
        # DB 연결
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        # Foreign key checks 비활성화
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # 모든 테이블 이름 가져오기
        cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{database}'")
        tables = cursor.fetchall()

        # 각 테이블 truncate
        for table in tables:
            table_name = table[0]
            print(f"Truncating table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")

        # Foreign key checks 다시 활성화
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        conn.commit()
        print("All tables have been truncated successfully")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


truncate_all_tables(
    host='localhost',
    user='root',
    password='an98',
    database=DB
)
#%%
#tb_market에 넣는 data 개수줄이기
data_list[0] = data_list[0][data_list[0].log_dt <= '2025-01-23 09:00:00']
#%%
import mysql.connector
from sqlalchemy import create_engine

def insert_data_to_mysql(df, host, user, password, database, table_name):
    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
        
        # DataFrame을 MySQL 테이블에 삽입
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='append',  # 테이블 존재 시 데이터 추가
            index=False,          # DataFrame의 인덱스는 삽입하지 않음
            chunksize=1000        # 대량 데이터 처리를 위한 청크 사이즈 설정
        )
        
        print(f"Data successfully inserted into {table_name}")
        
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        # SQLAlchemy는 연결 관리를 자동으로 해줌
        if 'engine' in locals():
            engine.dispose()

# 데이터 삽입 실행
for table, data in zip(tables, data_list):
    insert_data_to_mysql(
        df=data,
        host='localhost',
        user='root',
        password='an98',
        database=DB,
        table_name=table
    )









#%%
raw_data = data_list[1]
#%%
#raw_data.columns = ['log_dt','market','price','volume','amount']
#raw_data.columns = ['market','symbol','korean_name','english_name','capitalization','gecko_id']
#%%

#raw_data['hour'] = pd.to_datetime(raw_data['log_dt']).dt.hour

#%%
from datetime import datetime
current_time = datetime.now()

market_list = pd.unique(raw_data['market'])

#%%
import numpy as np
mean_dic = dict()
range_data_dic = dict()
for market in market_list:
    window_size = 12
    
    market_data = raw_data[raw_data['market'] == market]
    range_data = market_data[(market_data['log_dt'] >= '2025-01-07 23:00:00') & (market_data['log_dt'] <= '2025-01-08 00:00:00')]
    rolling_mean = market_data['volume'].rolling(window=window_size).mean()
    rolling_std = market_data['volume'].rolling(window=window_size).std()
    range_data_dic[market] = range_data
    
    volumes = list(range_data['volume'])
    firsts, last = volumes[:-1], volumes[-1]
    
    first_mean = np.mean(firsts)
    now_ratio = last / first_mean
    mean_dic[market] = now_ratio
    
#%%
from sqlalchemy import create_engine

#%%
market_info = pd.DataFrame(market_list)
market_info.columns = ['market']
#%%
db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'an98',
            'database': DB
        }
        
# SQLAlchemy engine 생성
engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

# DataFrame을 SQL 테이블로 저장
raw_data.to_sql(name= 'tb_market_info', 
         con=engine, 
         if_exists='replace', 
         index=False,
         chunksize=1000)

engine.dispose()
#%%
x = 1555555.5
f'{x:,}'
