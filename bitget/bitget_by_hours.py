import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
import yaml
import pymysql
from contextlib import contextmanager
import nest_asyncio

# Jupyter/IPython 환경 지원
nest_asyncio.apply()

# --- [1] 유틸리티 및 DB 관리 클래스 ---
def get_current_hour():
    """현재 한국 시간(KST)의 정각 datetime 객체 반환"""
    now_utc = datetime.now(timezone.utc)
    # 분, 초를 0으로 초기화하여 정시 기준 설정
    start_time = now_utc.replace(minute=0, second=0, microsecond=0)
    return start_time

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
    
    @contextmanager
    def get_connection(self):
        connection = pymysql.connect(**self.config)
        try:
            yield connection
        except Exception as e:
            print(f"DB Error: {e}")
            connection.rollback()
            raise
        finally:
            connection.close()

# --- [2] API 데이터 수집 함수 ---
def get_bitget_usdt_markets():
    """Bitget 모든 USDT-Futures 마켓 조회"""
    url = "https://api.bitget.com/api/v2/mix/market/tickers"
    params = {'productType': 'usdt-futures'}
    response = requests.get(url, params=params)
    response.raise_for_status()
    markets = [ticker['symbol'] for ticker in response.json().get('data', [])]
    return markets

async def get_bitget_candles_async(session, market, count, end_dt_kst, granularity='1H'):
    """Bitget 캔들 데이터 비동기 조회 (1H, 4H, 1D 지원)"""
    url = "https://api.bitget.com/api/v2/mix/market/history-candles"
    end_dt_utc = end_dt_kst - timedelta(hours=9)
    
    # 시간 단위별 간격 설정
    if granularity == '1H': interval = timedelta(hours=1)
    elif granularity == '4H': interval = timedelta(hours=4)
    elif granularity == '1D': interval = timedelta(days=1)
    else: interval = timedelta(hours=1)

    end_time_ms = int((end_dt_utc + interval).timestamp() * 1000)
    start_dt_utc = end_dt_utc - (interval * (count - 1))
    start_time_ms = int(start_dt_utc.timestamp() * 1000)
    
    params = {
        'symbol': market, 'productType': 'usdt-futures',
        'granularity': granularity, 'start_time': start_time_ms,
        'end_time': end_time_ms, 'limit': count
    }
    
    try:
        async with session.get(url, params=params) as response:
            res = await response.json()
            if res.get('code') == '00000' and res.get('data'):
                return [{'market': market, 'data': candle} for candle in res['data']]
            return []
    except: return []

async def fetch_all_candles(markets, data_cnt, current_hour, granularity='1H'):
    """API 제한을 준수하며 모든 마켓 데이터 수집"""
    all_candles = []
    async with aiohttp.ClientSession() as session:
        tasks = [get_bitget_candles_async(session, m, data_cnt, current_hour, granularity) for m in markets]
        chunk_size = 10
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            results = await asyncio.gather(*chunk)
            for res in results: all_candles.extend(res)
            await asyncio.sleep(1) # 초당 요청 제한 준수
    return all_candles

# --- [3] 데이터 정제 및 DB 삽입 함수 ---
def batch_insert_data(db_manager, df, table_name, pk_cols):
    """중복 키 업데이트를 포함한 배치 삽입"""
    if df.empty: return
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            cols = list(df.columns)
            sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))}) "
            update_parts = [f"{c} = VALUES({c})" for c in cols if c not in pk_cols]
            sql += f"ON DUPLICATE KEY UPDATE {', '.join(update_parts)}"
            cur.executemany(sql, [tuple(x) for x in df.values])
            conn.commit()
            print(f"Inserted {len(df)} records into {table_name}")

def process_market_data(db_manager, all_candles, table_name):
    """수집 데이터를 필터링하여 마켓 테이블에 저장"""
    raw_list = []
    for item in all_candles:
        m, d = item['market'], item['data']
        dt_utc = datetime.fromtimestamp(int(d[0])/1000.0, tz=timezone.utc)
        # DB 저장용 시간 (UTC 문자열)
        log_dt = dt_utc.strftime('%Y-%m-%d %H:%M:%S')
        raw_list.append([log_dt, m, float(d[1]), float(d[4]), float(d[2]), float(d[3]), float(d[5]), float(d[6])])
    
    df = pd.DataFrame(raw_list, columns=['log_dt','market','opening_price','trade_price','high_price','low_price','volume','amount'])
    
    # 중복 필터링 (DB의 최신 시간과 비교)
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT market, max(log_dt) as last_dt FROM {table_name} GROUP BY market")
            last_df = pd.DataFrame(cur.fetchall(), columns=['market', 'last_dt'])
    
    df_filtered = df.merge(last_df, on='market', how='left')
    df_filtered = df_filtered[(pd.to_datetime(df_filtered['log_dt']) > df_filtered['last_dt']) | (df_filtered['last_dt'].isna())].drop('last_dt', axis=1)
    
    batch_insert_data(db_manager, df_filtered, table_name, ['log_dt', 'market'])

def update_ma_data(db_manager, market_table, ma_table, gran):
    """MA 지표 계산 및 저장"""
    now = get_current_hour()
    time_list = [10, 20, 34, 50, 100, 200, 400, 800]
    gap = {'1H': 1, '4H': 4, '1D': 24}[gran]
    
    ma_results = {}
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            for t in time_list:
                ago = (now - timedelta(hours=t * gap)).strftime('%Y-%m-%d %H:%M:%S')
                cur.execute(f"SELECT market, avg(trade_price) FROM {market_table} WHERE log_dt >= '{ago}' GROUP BY market")
                ma_results[t] = {m: val for m, val in cur.fetchall()}
            
            prev_log_dt = (now - timedelta(hours=gap)).strftime('%Y-%m-%d %H:%M:%S')
            cur.execute(f"SELECT market, ma_10, ma_34 FROM {ma_table} WHERE log_dt = '{prev_log_dt}'")
            prev_ma = {m: (m10, m34) for m, m10, m34 in cur.fetchall()}

    final_ma_list = []
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for m in ma_results[10].keys():
        row = [m, now.strftime('%Y-%m-%d %H:00:00')]
        for t in time_list: row.append(ma_results[t].get(m, 0))
        
        # 크로스 계산
        new10, new34 = ma_results[10].get(m, 0), ma_results[34].get(m, 0)
        old10, old34 = prev_ma.get(m, (0, 0))
        
        is_golden = 1 if (old10 < old34 and new10 >= new34) else 0
        is_dead = 1 if (old10 > old34 and new10 <= new34) else 0
        row.extend([is_golden, is_dead, created_at])
        final_ma_list.append(row)
    
    ma_df = pd.DataFrame(final_ma_list, columns=['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at'])
    batch_insert_data(db_manager, ma_df, ma_table, ['log_dt', 'market'])

# --- [4] 메인 실행 로직 ---
if __name__ == '__main__':
    # 1. 환경 설정 로드
    with open("/home/ubuntu/baseball_project/db_settings.yml", 'r') as f:
        conf = yaml.safe_load(f)['BASEBALL']
    db_mgr = DatabaseManager(conf, 'bithumb')
    markets = get_bitget_usdt_markets()
    
    curr_kst = get_current_hour()
    hour = curr_kst.hour

    # 2. 실행할 작업 리스트 (단위, 마켓테이블, MA테이블)
    jobs = [('1H', 'tb_market_hour_bitget', 'tb_ma_60_minutes_bitget')]
    
    # 4시간 주기 (1, 5, 9, 13, 17, 21시)
    #if hour % 4 == 1:
    jobs.append(('4H', 'tb_market_4hour_bitget', 'tb_ma_4hour_bitget'))
    
    # 일일 주기 (오전 9시)
    #if hour == 9:
    jobs.append(('1D', 'tb_market_day_bitget', 'tb_ma_day_bitget'))

    # 3. 작업 순차 실행
    for gran, m_table, ma_table in jobs:
        print(f"\n--- Starting {gran} Task ---")
        candles = asyncio.run(fetch_all_candles(markets, 10, curr_kst, granularity=gran))
        process_market_data(db_mgr, candles, m_table)
        update_ma_data(db_mgr, m_table, ma_table, gran)

    print(f"All Tasks Completed at {datetime.now()}")