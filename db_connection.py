import pymysql
import yaml
import json
from datetime import datetime
import os


# 사용 예시
file_path = "/home/ubuntu/baseball_project/db_settings.yml"  # YAML 파일이 있는 폴더 경로
with open(file_path, 'r', encoding = 'utf-8') as file:
    yaml_data = yaml.safe_load(file)
    yaml_data = yaml_data['BASEBALL']
print(yaml_data)

#MySQL 연결


connection = pymysql.connect(
   host=yaml_data['HOST'],
   user=yaml_data['USER'],
   password=yaml_data['PASSWORD'],
   db=yaml_data['NAME']
)

with connection.cursor() as cursor:
    sql = 'select * from baseball.team_info'
    print(cursor.execute(sql))
