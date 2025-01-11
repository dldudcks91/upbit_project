#%%



import requests
import pymysql
import yaml

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
headers = {
    'X-CMC_PRO_API_KEY': 'c667961f-2490-4983-8aee-d67bb51e2083'
}
parameters = {
    'start': '1',
    'limit': '1000',
    'convert': 'KRW'  # USD 대신 KRW 사용
}

response = requests.get(url, headers=headers, params=parameters)
data = response.json()


# API 호출 후 데이터 출력 예시
crypto_dic = dict()
for crypto in data['data']:
    market_cap_krw = crypto['quote']['KRW']['price']
    name = 'KRW-' + crypto['symbol']
    
    crypto_dic[name] = round(float(market_cap_krw),3)
    


file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
with open(file_path, 'r', encoding = 'utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']


connection = pymysql.connect(
   host=yaml_data['HOST'],
   user=yaml_data['USER'],
   password=yaml_data['PASSWORD'],
   db= 'upbit'
)

with connection.cursor() as cursor:
    # RDS 소스 데이터베이스 엔진 생성
    
    try:
        cursor.execute(f"SELECT market FROM tb_market_info")
        market_list = [market[0] for market in cursor.fetchall()]    
        print(f"market_info get success")
    except Exception as e:
        print(f"market_info get False")

#%%





#%%

old_list = list()
for market in market_list:
    try:
        
        old_list.append([market, crypto_dic[market]])
    except:
        old_list.append([market, 0])

print(old_list)

#%%

