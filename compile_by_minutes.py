import os
import yaml
import json
from datetime import datetime, timedelta
import time

import redis
import requests
import pymysql
import pandas as pd

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

        try:
            with connection.cursor() as cursor:
                
                sql = """
                SELECT * FROM tb_market_info
                """
                
                cursor.execute(sql)
                market_info_data = pd.DataFrame(cursor.fetchall())
                
                cursor.execute("SHOW COLUMNS FROM tb_market_info")


                market_info_columns = cursor.fetchall()
                market_info_columns =  [row[0] for row in market_info_columns]

                market_info_data.columns = market_info_columns
                
                #김치프리미엄불러오기
                try:
                    gecko_id_list = list(market_info_data.gecko_id)
                    gecko_ids = ','.join(gecko_id_list)
                   
                    gecko_url = "https://api.coingecko.com/api/v3/simple/price"
                    gecko_params = {
                        'ids': gecko_ids,  # "bitcoin,ethereum" 형태로 전달
                        'vs_currencies': 'krw'
                    }
    
                    gecko_price_dic = requests.get(gecko_url,params = gecko_params).json()
                   

                except Exception as e:
                    
                    print(f"Error: {e}")

                try:
                    gecko_id = market_info_data[market_info_data['market'] == market]['gecko_id'].iloc[0]
                    foreigner_price = gecko_price_dic[gecko_id]['krw']
                except Exception as e: