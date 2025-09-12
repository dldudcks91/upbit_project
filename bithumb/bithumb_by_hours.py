#%%
import requests 
from datetime import datetime , timedelta, timezone
import time
import pandas as pd
import yaml
import pymysql
from contextlib import contextmanager

class DatabaseManager:
    """데이터베이스 연결 관리 클래스"""
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

def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.bithumb.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    return krw_markets

def batch_insert_market_data(db_manager, df_unique, table_name):
    """배치 INSERT 최적화 함수"""
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
            data_tuples = [tuple(row) for row in df_unique.values]
            
            try:
                cursor.executemany(sql, data_tuples)
                conn.commit()
                print(f'Success: Inserted {len(data_tuples)} records into {table_name}')
            except Exception as e:
                print(f'Error inserting into {table_name}: {e}')
                conn.rollback()

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

# 설정 파일 로드
file_path = "/home/ubuntu/baseball_project/db_settings.yml"
with open(file_path, 'r', encoding='utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']

DB = 'bithumb'
db_manager = DatabaseManager(yaml_data, DB)

markets = get_krw_markets()

#%%
old_list = list()
current_time = datetime.now(tz = timezone.utc) + timedelta(hours=9)

start_time = current_time.replace(minute=0, second=0, microsecond=0)
start_t = time.time()
DATA_CNT = 10
for market in markets:
    market_start_time = start_time + timedelta(hours = 0)
    for hour_range in range(1):
        market_start_time += timedelta(hours = -DATA_CNT * hour_range)
        date_str = market_start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Change endpoint to hourly candles
        url = 'https://api.bithumb.com/v1/candles/minutes/60'
        params = {
            'market': market,
            'count': DATA_CNT,  # Each request will get 100 hours of data
            'to': date_str
        }
        
        headers = {"accept": "application/json"}
        response = requests.get(url, params=params, headers=headers)
        
        new_list = response.json()
        old_list.extend(new_list)

#%%
tables = ['tb_market_hour']

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
# 최대값 조회 최적화
data_list = list()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"SELECT max(log_dt) as log_dt FROM {table}")
                rows = cursor.fetchall()
                log_dt = pd.DataFrame(rows, columns=['log_dt'])
                data_list.append(log_dt)
            except Exception as e:
                print(f"{table} 조회 중 오류: {e}")

#%%
old_df = data_list[0]
last_log_dt = old_df['log_dt'].max()
df_unique = df_unique[pd.to_datetime(df_unique['log_dt'])>last_log_dt]
print(df_unique.shape, last_log_dt)

#%%
# 배치 INSERT 실행
for table in tables:
    batch_insert_market_data(db_manager, df_unique, table)

#%%
print('Start set ma')
now = (datetime.now() - timedelta(hours=10))

ma_dic = dict()
with db_manager.get_connection() as conn:
    with conn.cursor() as cursor:
        time_list = [10, 20, 34, 50 ,100, 200, 400, 800]
           
        # 모든 MA 계산을 한 번의 연결에서 처리
        for time_val in time_list:        
            ago = (now - timedelta(hours=time_val)).strftime('%Y-%m-%d %H:00:00')      
            cursor.execute(f"SELECT market, avg(trade_price) as ma FROM tb_market_hour WHERE log_dt > '{ago}' group by market")
            ma_data = pd.DataFrame(cursor.fetchall())
            ma_dic[time_val] = ma_data
        
        # 이전 MA 데이터 조회
        one_hour_ago = (now - timedelta(hours = 1)).strftime('%Y-%m-%d %H:00:00')      
        cursor.execute(f"SELECT * FROM tb_ma_60_minutes where log_dt = '{one_hour_ago}'")
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
        market_ma_dic[market].append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    #%%
    # MA 데이터 배치 INSERT
    input_data = pd.DataFrame(market_ma_dic).transpose().reset_index()
    column_names = ['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at']
    input_data.columns = column_names

    batch_insert_ma_data(db_manager, input_data, 'tb_ma_60_minutes')

print(f'Complete All Task: {start_time}')