#%%



import requests
import time
import pymysql
import yaml
#%%
def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [[market['market'],market['market'][4:], market['korean_name']] for market in markets if market['market'].startswith('KRW-')]

    print(f"Total KRW markets: {len(krw_markets)}")
    return krw_markets




#%%
def convert_korean_to_number(value):
    if isinstance(value, str):
        # '억원' 처리
        if '억원' in value:
            number = float(value.replace('억원', ''))
            return int(number * 100000000)  # 1억 = 100,000,000
        # '조원' 처리
        elif '조원' in value:
            number = float(value.replace('조원', ''))
            return int(number * 1000000000000)  # 1조 = 1,000,000,000,000
    if value == '':
        return None
    return value
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

markets = get_krw_markets()

with conn.cursor() as cursor:
    # supply_columns는 'market' 하나뿐이므로 더 간단하게 처리
    
    
    # tb_market_now 테이블에도 동일한 market 값 삽입
    now_sql = f"""
        INSERT INTO tb_market_now (market, symbol, korean_name) 
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        market = VALUES(market),
        symbol = VALUES(symbol),
        korean_name = VALUES(korean_name)
    """
    cursor.executemany(now_sql, markets)
    
    conn.commit()





#market_supply채우기
with conn.cursor() as cursor:
    cursor.execute(f"SELECT symbol FROM upbit.tb_market_info")

    markets =  list(cursor.fetchall())
    
    
    cursor.execute(f"SHOW COLUMNS FROM upbit.tb_market_supply")
    supply_columns = cursor.fetchall()
    supply_columns = [column[0] for column in supply_columns]
    
    
#%%


old_list = list()
for market in markets:
    
    market = market[0]
    
    response = requests.get(f'https://api-manager.upbit.com/api/v1/coin_info/pub/{market}.json')

    
    response_dic = response.json()    
    data_dic = response_dic['data']
    
    
    capitalization = data_dic['market_data']['coin_market_cap']['market_cap']
    capitalization = convert_korean_to_number(capitalization)
    
    max_supply = data_dic['market_data']['max_supply']
    max_supply = int(max_supply.replace(',','')) if max_supply != '' else None
    
    now_supply = data_dic['market_data']['coin_market_cap']['circulating_supply']
    now_supply = int(now_supply.replace(',','')) if now_supply != '' else None
    
    '''
    #발행일은 고정값이라 계속 가져올 필요없음
    
    try: 
        initial_release = data_dic['initial_release_at']['value']
    except:
        initial_release = data_dic['header_key_values']['initial_release_at']['value']
    '''
    new_list = [market, capitalization, max_supply, now_supply]
    old_list.append(new_list)
#%%

with conn.cursor() as cursor:
    # 동적으로 컬럼과 값 생성
    columns_str = ', '.join(supply_columns)
    placeholders = ', '.join(['%s'] * len(supply_columns))
    set_clause = ', '.join([f"{col} = VALUES({col})" for col in supply_columns])
    
    sql = f"""
        INSERT INTO tb_market_supply ({columns_str}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {set_clause}
    """
    # executemany로 모든 행 한 번에 삽입
    cursor.executemany(sql, old_list)
    conn.commit()
conn.close()
#%%
