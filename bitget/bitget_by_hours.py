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
import nest_asyncio
# 기존 이벤트 루프에 새로운 비동기 루프를 중첩할 수 있도록 허용
nest_asyncio.apply()
#%%
# 현재 UTC 시간을 기준으로 한국 시간 (UTC+9)의 '정시'를 계산하는 유틸리티 함수
def get_current_hour():
# ... (이하 동일) ...
    """현재 한국 시간(KST)의 정각(00분 00초) datetime 객체 반환"""
    now_utc = datetime.now(timezone.utc)
    # KST로 변환
    
    start_time = now_utc.replace(minute=0, second=0, microsecond=0)
    return start_time

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

def get_bitget_usdt_markets():
# ... (이하 동일) ...
    """Bitget USDT-Futures 마켓의 모든 거래쌍 조회"""
    url = "https://api.bitget.com/api/v2/mix/market/tickers"
    params = {'productType': 'usdt-futures'}
    response = requests.get(url, params=params)
    response.raise_for_status() # HTTP 오류 발생 시 예외 처리
    ticker_data = response.json()
    
    # symbol 필드만 추출
    markets = [ticker['symbol'] for ticker in ticker_data.get('data', [])]
    print(f"Total USDT-Futures markets: {len(markets)}")
    return markets

def batch_insert_market_data(db_manager, df_unique, table_name):
# ... (이하 동일) ...
    """배치 INSERT 최적화 함수 (기존 코드와 동일)"""
    if df_unique.empty:
        print(f"No data to insert for {table_name}")
        return
    
    with db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            # 테이블 컬럼 정보 조회
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = cursor.fetchall()
            column_names = [column[0] for column in columns]
            
            # INSERT 쿼리 준비 (ON DUPLICATE KEY UPDATE 사용)
            columns_str = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            
            # 중복 키 업데이트 구문 생성 (log_dt, market 제외한 나머지 컬럼들)
            update_columns = [col for col in column_names if col not in ['log_dt', 'market']]
            update_str = ', '.join([f"{col} = VALUES({col})" for col in update_columns])
            
            sql = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_str}
            """
            
            # DataFrame을 튜플 리스트로 변환
            # MySQL datetime 컬럼에 맞추기 위해 datetime 객체나 포맷된 문자열을 전달해야 함
            data_tuples = [tuple(row) for row in df_unique.values]
            
            try:
                cursor.executemany(sql, data_tuples)
                conn.commit()
                print(f'Success: Inserted {len(data_tuples)} records into {table_name}')
            except Exception as e:
                print(f'Error inserting into {table_name}: {e}')
                conn.rollback()

# batch_insert_ma_data 함수는 현재 사용되지 않으므로 생략 (필요 시 복사하여 사용)
def batch_insert_ma_data(db_manager, input_data, table_name):
    """이동평균 데이터 배치 INSERT"""
    if input_data.empty:
        print(f"No MA data to insert for {table_name}")
        return
    
    with db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            column_names = ['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at']
            
            columns_str = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            
            # 중복 키 업데이트 (log_dt, market 제외)
            update_columns = [col for col in column_names if col not in ['log_dt', 'market']]
            update_str = ', '.join([f"{col} = VALUES({col})" for col in update_columns])
            
            sql = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_str}
            """
            
            # DataFrame을 튜플 리스트로 변환
            data_tuples = [tuple(row) for row in input_data.values]
            
            try:
                cursor.executemany(sql, data_tuples)
                conn.commit()
                print(f'Success: Inserted {len(data_tuples)} MA records into {table_name}')
            except Exception as e:
                print(f'Error inserting MA data into {table_name}: {e}')
                conn.rollback()
async def get_bitget_candles_async(session, market, count, end_dt_kst):
# ... (이하 동일) ...
    """
    Bitget 1시간 캔들 데이터를 비동기적으로 조회하는 함수.
    
    Args:
        session (aiohttp.ClientSession): aiohttp 세션 객체.
        market (str): 조회할 마켓 심볼.
        count (int): 조회할 캔들 개수 (limit).
        end_dt_kst (datetime): 한국 시간 정각 datetime 객체.
    """
    curl = "https://api.bitget.com/api/v2/mix/market/history-candles"
    
    # KST 정각 -> UTC 정각
    end_dt_utc = end_dt_kst - timedelta(hours=9) 
    
    # API의 'end' 파라미터: 조회할 마지막 캔들 시간 + 1시간 (UTC)의 ms 값
    end_time_ms = int((end_dt_utc + timedelta(hours=1)).timestamp() * 1000)
    
    # API의 'start' 파라미터: count 개수 이전의 시간 (UTC)의 ms 값
    start_dt_utc = end_dt_utc - timedelta(hours=count - 1)
    start_time_ms = int(start_dt_utc.timestamp() * 1000)
    
    params = {
        'symbol': market,
        'productType': 'usdt-futures',
        'granularity': '1H',
        'start_time': start_time_ms,
        'end_time': end_time_ms,
        'limit': count
    }
    
    headers = {"accept": "application/json"}
    
    try:
        # aiohttp로 비동기 GET 요청
        async with session.get(curl, params=params, headers=headers) as response:
            response.raise_for_status()
            response_data = await response.json()
            
            if response_data.get('code') == '00000' and response_data.get('data'):
                # 수집된 데이터에 market 정보를 추가하여 반환
                return [{'market': market, 'data': candle} for candle in response_data['data']]
            else:
                # API 응답 오류 메시지 출력
                print(f"Error fetching {market} (Status: {response.status}, Msg: {response_data.get('msg')})")
                return []
                
    except aiohttp.ClientError as e:
        print(f"Request error for {market}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error for {market}: {e}")
        return []


async def fetch_all_candles(markets, data_cnt, current_kst_hour_start, rate_limit_per_second=20):
# ... (이하 동일) ...
    """
    모든 마켓의 캔들 데이터를 비동기적으로 수집하고 API 요청 제한을 관리합니다.
    """
    all_candles = []
    
    # 비동기 세션 시작
    async with aiohttp.ClientSession() as session:
        # 모든 마켓에 대한 비동기 작업(Task) 리스트 생성
        tasks = []
        for market in markets:
            task = get_bitget_candles_async(session, market, data_cnt, current_kst_hour_start)
            tasks.append(task)
        
        # API 제한 관리를 위한 청크(Chunk) 처리
        chunk_size = rate_limit_per_second  # 1초당 20개 요청 제한
        total_tasks = len(tasks)
        
        for i in range(0, total_tasks, chunk_size):
            chunk = tasks[i:i + chunk_size]
            start_time = time.time()
            
            print(f"Batch {i//chunk_size + 1}: Fetching {len(chunk)} markets...")
            
            # 현재 청크의 모든 작업을 동시에 실행
            results = await asyncio.gather(*chunk)
            
            # 결과 처리 및 API 제한 준수
            for result_list in results:
                all_candles.extend(result_list)
            
            elapsed_time = time.time() - start_time
            # 1초 제한을 준수하기 위해 남은 시간만큼 대기
            sleep_time = 1.0 - elapsed_time
            
            if i + chunk_size < total_tasks and sleep_time > 0:
                # 다음 배치를 시작하기 전에 1초 대기 (API 제한 준수)
                await asyncio.sleep(sleep_time)

    return all_candles

# --- 메인 실행 블록 ---
#%%
# 설정 파일 로드 (환경에 맞게 경로 수정)
file_path = "/home/ubuntu/baseball_project/db_settings.yml" 
with open(file_path, 'r', encoding='utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']

DB = 'bithumb' # 데이터베이스 이름 수정 필요
db_manager = DatabaseManager(yaml_data, DB)
#%%
# 1. 마켓 목록 가져오기
markets = get_bitget_usdt_markets()

#%%
# 2. 데이터 수집
DATA_CNT = 10 # 요청당 가져올 캔들 개수 <--- DATA_CNT 정의
start_t = time.time()

# 현재 한국 시간 정각
current_hour = get_current_hour()

# **[수정된 부분]** 'await' outside function 오류 해결을 위해 asyncio.run() 사용
# 일반 파이썬 스크립트(.py) 실행을 위한 표준 방식입니다.

# 비동기 실행이 실패하거나 건너뛰어질 경우를 대비해 all_candles를 미리 빈 리스트로 초기화
all_candles = []

# __name__이 '__main__'일 때만 실행하도록 하여 안전성 확보
if __name__ == '__main__':
    try:
        all_candles = asyncio.run(fetch_all_candles(
            markets, 
            DATA_CNT, 
            current_hour, 
            rate_limit_per_second=10
        ))
    except RuntimeError as e:
        # Jupyter/IPython 환경에서 이미 루프가 실행 중일 경우를 위한 대체 실행 (옵션)
        if "cannot run non-main thread" in str(e) or "There is no current event loop in thread" in str(e):
            print("Warning: Running in interactive environment. Using existing event loop.")
            loop = asyncio.get_event_loop()
            all_candles = loop.run_until_complete(fetch_all_candles(
                markets, 
                DATA_CNT, 
                current_hour, 
                rate_limit_per_second=10
            ))
        else:
            raise

print(f"\n총 {len(all_candles)}개의 캔들 데이터를 수집했습니다. (Elapsed: {time.time() - start_t:.2f}s)")

#%%
# 3. 데이터프레임으로 변환 및 정제
tables = ['tb_market_hour_bitget'] # 데이터베이스 테이블 이름 (기존 Bithumb 코드와 동일하게 사용)

total_list = list()
for item in all_candles:
    market = item['market']
    data = item['data']
    
    # Bitget 캔들 데이터 구조: [ms_timestamp, open, high, low, close, volume, amount]
    
    # 1. 타임스탬프 처리 (밀리초 -> 초 -> KST datetime 문자열)
    timestamp_ms = int(data[0])
    timestamp_s = timestamp_ms / 1000.0
    
    # 타임스탬프를 UTC datetime으로 변환
    dt_utc = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    # KST (UTC+9)로 변환
    dt_kst = dt_utc + timedelta(hours=9)
    # 데이터베이스에 저장할 포맷 (문자열)
    log_dt = dt_utc.strftime('%Y-%m-%d %H:%M:%S')

    opening_price = float(data[1])
    high_price = float(data[2])
    low_price = float(data[3])
    trade_price = float(data[4]) # Bitget에서는 close price가 trade_price 역할
    volume = float(data[5])
    amount = float(data[6])
    
    # Bithumb 코드의 컬럼 순서: ['log_dt','market','opening_price','trade_price','high_price','low_pridce','volume','amount']
    # Bitget 데이터를 이에 맞게 순서 조정
    total_list.append([log_dt, market, opening_price, trade_price, high_price, low_price, volume, amount])
    
#%%
df = pd.DataFrame(total_list)
df.columns = ['log_dt','market','opening_price','trade_price','high_price','low_pridce','volume','amount']
df = df.sort_values(by = ['market', 'log_dt'])
df_unique = df.drop_duplicates(subset=['log_dt', 'market'])

#%%
# 4. 기존 데이터와의 중복 확인 및 필터링
data_list = list()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        for table in tables:
            try:
                # 데이터베이스에서 마켓별 최대 log_dt (가장 최근 시간) 조회
                cursor.execute(f"SELECT market, max(log_dt) as log_dt FROM {table} group by market")
                rows = cursor.fetchall()
                log_dt_df = pd.DataFrame(rows, columns=['market', 'log_dt'])
                data_list.append(log_dt_df)
            except Exception as e:
                # 테이블이 없는 경우 등을 처리하기 위해 빈 DataFrame 추가
                print(f"{table} 조회 중 오류 (테이블이 없거나 컬럼명 오류일 수 있음): {e}")
                data_list.append(pd.DataFrame(columns=['market', 'log_dt']))


old_df = data_list[0]
if not old_df.empty:
    last_log_dt = old_df.groupby('market')['log_dt'].max().reset_index()
    last_log_dt.columns = ['market', 'last_log_dt']
else:
    # 기존 데이터가 없는 경우를 대비해 빈 DataFrame 생성
    last_log_dt = pd.DataFrame(columns=['market', 'last_log_dt'])


df_with_last = df_unique.merge(last_log_dt, on='market', how='left')

# `last_log_dt`는 datetime 객체로, `log_dt`는 문자열이므로 비교를 위해 변환 필요
df_filtered = df_with_last[
    (pd.to_datetime(df_with_last['log_dt']) > df_with_last['last_log_dt']) | 
    (df_with_last['last_log_dt'].isna())  # 기존에 없던 새로운 market
].drop('last_log_dt', axis=1)

print(f"\n--- 필터링 결과 ---")
print(f"총 수집된 유니크 레코드 수: {df_unique.shape[0]}")
print(f"기존에 있던 마켓 수: {last_log_dt.shape[0]}")
print(f"DB에 삽입될 최종 레코드 수: {df_filtered.shape[0]} (새로운 데이터)")

batch_insert_market_data(db_manager, df_filtered, 'tb_market_hour_bitget')
#%%
print('Start set ma')
now = (datetime.now() - timedelta(hours=1))

ma_dic = dict()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        time_list = [10, 20, 34, 50 ,100, 200, 400, 800]
           
        # 모든 MA 계산을 한 번의 연결에서 처리
        for time_val in time_list:        
            ago = (now - timedelta(hours=time_val)).strftime('%Y-%m-%d %H:00:00')      
            cursor.execute(f"SELECT market, avg(trade_price) as ma FROM tb_market_hour_bitget WHERE log_dt > '{ago}' group by market")
            ma_data = pd.DataFrame(cursor.fetchall())
            ma_dic[time_val] = ma_data
        
        # 이전 MA 데이터 조회 (1시간 전)
        one_hour_ago = (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')      
        cursor.execute(f"SELECT * FROM tb_ma_60_minutes_bitget where log_dt = '{one_hour_ago}'")
        last_ma_data = pd.DataFrame(cursor.fetchall())

#%%
# MA 데이터 처리
if not ma_dic[10].empty:
    markets = list(ma_dic[10].iloc[:,0])
    market_ma_dic = {market:[now.strftime('%Y-%m-%d %H:00:00')] for market in markets}

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
        
        # 골든크로스/데드크로스 계산
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
    # MA 데이터 배치 INSERT
    input_data = pd.DataFrame(market_ma_dic).transpose().reset_index()
    column_names = ['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at']
    input_data.columns = column_names

    batch_insert_ma_data(db_manager, input_data, 'tb_ma_60_minutes_bitget')

print(f'Complete All Task: {datetime.now()}: {time.time() - start_t}')
#%%
# '''
# 로컬용
# '''
# from sshtunnel import SSHTunnelForwarder
# import sqlalchemy
# import pandas as pd
# import paramiko
# # SSH 설정
# ssh_host = '43.201.254.212'  # EC2 퍼블릭 IP
# ssh_username = 'ubuntu'    # EC2 사용자명
# ssh_pkey = paramiko.RSAKey.from_private_key_file("C:/Users/user/Desktop/python_authority/lyc_baseball_20241126.pem")  # 키페어 경로

# # RDS 설정
# rds_endpoint = 'database-1.ch08cm8s8t6j.ap-northeast-2.rds.amazonaws.com'  # RDS 엔드포인트
# rds_port = 3306
# rds_username = 'admin'
# rds_password = 'Gnfkqhsh91!'
# DB = 'bithumb'



# with SSHTunnelForwarder(
#     (ssh_host, 22),  # SSH 서버 주소와 포트
#     ssh_username=ssh_username,
#     ssh_pkey=ssh_pkey,  # 또는 ssh_password=ssh_password
#     remote_bind_address=(rds_endpoint, 3306)  # RDS 엔드포인트와 MySQL 포트
# ) as tunnel:
    
#     # 로컬 포트로 MySQL 연결
#     conn = pymysql.connect(
#         host='127.0.0.1',  # 로컬호스트
#         port=tunnel.local_bind_port,  # 동적으로 할당된 로컬 포트 
#         user=rds_username,
#         passwd=rds_password,
#         db=DB
#     )
    
#     # 쿼리 실행
#     with conn.cursor() as cursor:
#         cursor.execute(f"SHOW COLUMNS FROM {tables[0]}")
#         columns = cursor.fetchall()
#         column_names = [column[0] for column in columns]
        
#         # INSERT 쿼리 준비 (ON DUPLICATE KEY UPDATE 사용)
#         columns_str = ', '.join(column_names)
#         placeholders = ', '.join(['%s'] * len(column_names))
        
#         # 중복 키 업데이트 구문 생성 (log_dt, market 제외한 나머지 컬럼들)
#         update_columns = [col for col in column_names if col not in ['log_dt', 'market']]
#         update_str = ', '.join([f"{col} = VALUES({col})" for col in update_columns])
        
#         sql = f"""
#             INSERT INTO {tables[0]} ({columns_str}) 
#             VALUES ({placeholders})
#             ON DUPLICATE KEY UPDATE {update_str}
#         """
        
#         # DataFrame을 튜플 리스트로 변환
#         # MySQL datetime 컬럼에 맞추기 위해 datetime 객체나 포맷된 문자열을 전달해야 함
#         data_tuples = [tuple(row) for row in df_unique.values]
#         try:
#             cursor.executemany(sql, data_tuples)
#             conn.commit()
#             print(f'Success: Inserted {len(data_tuples)} MA records into {tables[0]}')
#         except Exception as e:
#             print(f'Error inserting MA data into {tables[0]}: {e}')
#             conn.rollback()
        
#         # 각 테이블 마이그레이션
#         # for table in tables:
#         #     batch_insert_market_data(db_manager, df_unique, table)
#     conn.close()
# tunnel.close()
# 5. 배치 INSERT 실행
