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
import matplotlib.pyplot as plt
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
        'granularity': '1D',
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
DATA_CNT = 200 # 요청당 가져올 캔들 개수 <--- DATA_CNT 정의
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
df.columns = ['log_dt','market','opening_price','trade_price','high_price','low_price','volume','amount']
df = df.sort_values(by = ['market', 'log_dt'])
df_unique = df.drop_duplicates(subset=['log_dt', 'market'])

#%%
df_unique['price_range'] = abs(1 - (df_unique['high_price'] / df_unique['low_price']))
df_avg = df_unique.groupby(by = 'market').mean()
df_median = df_unique.groupby(by = 'market').median()
z = df_unique[df_unique['market'] == 'FARTCOINUSDT']
#%%
#%%
markets = df_unique['market'].unique()
p_list = list()
for market in markets:

    P_data = np.array(df_unique[df_unique['market'] == market].sort_values(by = 'log_dt').trade_price)
    t = np.arange(1, P_data.shape[0]+1)
    P_data = P_data/P_data[0]
    
    # 2차 다항 회귀 적합 (계수 [a, b, c] 계산)
    coefficients = np.polyfit(t, P_data, 2)
    a, b, c = coefficients

    p_list.append([market,P_data.shape[0], a,b,c])

p_data = pd.DataFrame(p_list)
    
    
#%%
P_data = np.array(df_unique[df_unique['market'] == 'HUSDT'].sort_values(by = 'log_dt').trade_price)
t = np.arange(1, P_data.shape[0]+1)
P_data = P_data/P_data[0]

# 2차 다항 회귀 적합 (계수 [a, b, c] 계산)
coefficients = np.polyfit(t, P_data, 2)
a, b, c = coefficients


# 회귀 곡선 값 계산
P_regression = a * t**2 + b * t + c
a, b, c = coefficients

# 회귀 곡선 값 계산
P_regression = a * t**2 + b * t + c

print(f"계수 (a, b, c): {a:.4f}, {b:.4f}, {c:.4f}")
print(f"a = {a:.4f} (0에 가까움): 선형에 가까움 (추세 유지)")

# 그래프 출력
plt.figure(figsize=(10, 6))
plt.scatter(t, P_data, label='실제 종가 (P)', color='blue')
plt.plot(t, P_regression, label=f'2차 회귀곡선 (a={a:.4f})', color='red', linestyle='--')
plt.title('예시 3: 선형에 가까움 (추세 유지형)')
plt.xlabel('시간 순서 (t)')
plt.ylabel('종가 (P)')
plt.legend()
plt.grid(True)
plt.show()