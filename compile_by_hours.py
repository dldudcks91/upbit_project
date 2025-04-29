#%%
import requests 
from datetime import datetime , timedelta, timezone
import time
import pandas as pd
import yaml
import pymysql

def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    return krw_markets


markets = get_krw_markets()



#%%
old_list = list()
current_time = datetime.now(tz = timezone.utc)
start_date = current_time.replace(minute=0, second=0, microsecond=0)
date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
for market in markets:
    for hour_range in range(1):
        
        # Change endpoint to hourly candles
        url = 'https://api.upbit.com/v1/candles/minutes/15'
        params = {
            'market': market,
            'count': 15,  # Each request will get 100 hours of data
            'to': date_str
        }
        
        headers = {"accept": "application/json"}
        response = requests.get(url, params=params, headers=headers)
        
        new_list = response.json()
        old_list.extend(new_list)
        # Adjust time delta to move back 100 hours instead of 100 days
        
        
        print(market, len(old_list), start_date.strftime('%Y-%m-%d %H:%M:%S'))
        time.sleep(0.1)
#%%

tables = ['tb_market_hour']
#%%
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
# price_pivot = df_unique.pivot(index='log_dt', columns='market', values='opening_price')
# corr_matrix = price_pivot.corr()
# duplicates = df[df.duplicated(keep=False)] 
#%%
file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
with open(file_path, 'r', encoding = 'utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']
    
conn = pymysql.connect(
   host=yaml_data['HOST'],
   user=yaml_data['USER'],
   password=yaml_data['PASSWORD'],
   db= 'upbit'
)


with conn.cursor() as cursor:
    
    
    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        
        columns =  cursor.fetchall()
        column_names = [column[0] for column in columns]
        
        
        for _, row in df_unique.iterrows():
            # 동적으로 컬럼과 값 생성
            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            # 각 행 삽입
            try:
                cursor.execute(sql, tuple(row))
            except:
                continue
    
        print(f'success migration table {table}')
    conn.commit()
#%%

# ma_데이터 가져오기
data_dic = dict()



now = (datetime.now() - timedelta(hours=1) - timedelta(hours = 9))

ma_dic = dict()
with conn.cursor() as cursor:
    
    
    time_list = [10, 20, 34, 50 ,100, 200, 400, 800]
       
    for time in time_list:        
        ago = (now - timedelta(hours=time)).strftime('%Y-%m-%d %H:00:00')      
        cursor.execute(f"SELECT market, avg(trade_price) as ma FROM tb_market_hour WHERE log_dt > '{ago}' group by market")
            
        ma_data = pd.DataFrame(cursor.fetchall())
        
        ma_dic[time] = ma_data
    one_hour_ago = (now - timedelta(hours = 1)).strftime('%Y-%m-%d %H:00:00')      
    cursor.execute(f"SELECT * FROM tb_ma_60_minutes where log_dt = '{one_hour_ago}'")
    last_ma_data = pd.DataFrame(cursor.fetchall())
            
    
            

conn.close()
#%%
    
markets = list(ma_data.iloc[:,0])
market_ma_dic = {market:[now.strftime('%Y-%m-%d %H:00:00')] for market in markets}

for market in markets:
    for time_ma in time_list:
        ma_time_data = ma_dic[time_ma]
        price_ma = ma_time_data[ma_time_data.iloc[:,0] == market].iloc[0,1]
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
input_data = pd.DataFrame(market_ma_dic).transpose().reset_index()
column_names = ['market','log_dt','ma_10','ma_20','ma_34','ma_50','ma_100','ma_200','ma_400','ma_800','golden_cross_10_34','dead_cross_10_34','created_at']
input_data.columns = column_names
#%%

    
with conn.cursor() as cursor:
    
    
    
        
    
    for _, row in input_data.iterrows():
        # 동적으로 컬럼과 값 생성
        columns = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names))
        
        
        sql = f"INSERT INTO tb_ma_60_minutes ({columns}) VALUES ({placeholders})"
        
        # 각 행 삽입
        
        cursor.execute(sql, tuple(row))
        
        

    
conn.commit()


