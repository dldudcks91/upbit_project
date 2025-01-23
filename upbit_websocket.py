import requests
#import nest_asyncio
import redis
import websockets
import json
import asyncio
from datetime import datetime
from asyncio import Queue

def get_krw_markets():
   """업비트 KRW 마켓의 모든 거래쌍 조회"""
   url = "https://api.upbit.com/v1/market/all"
   response = requests.get(url)
   markets = response.json()
   # KRW 마켓만 필터링
   krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
   print(f"Total KRW markets: {len(krw_markets)}")
   return krw_markets

def connect_redis():
   try:
       r = redis.Redis(
           host='localhost', 
           port=6379, 
           db=0, 
           decode_responses=True,
           socket_connect_timeout=5
       )
       r.ping()  # Redis 서버 연결 테스트
       print("Successfully connected to Redis")
       return r
   except redis.ConnectionError as e:
       print(f"Could not connect to Redis: {e}")
       print("Please make sure Redis server is running")
       raise

krw_markets = get_krw_markets()
try:
   r = connect_redis()
except Exception as e:
   print(f"Redis connection failed: {e}")

async def process_data(queue):
   while True:
       try:
           data = await queue.get()
           
           TEN_SECONDS = 10000
           base_timestamp = data['tms'] - (data['tms'] % TEN_SECONDS)
           key = f"trade_volume:{data['cd']}:{base_timestamp}"
           
           try:
               r.execute_command('INCRBYFLOAT', key, str(data['tv']))
               r.expire(key, 60)
               current_value = r.get(key)
               print(f"Added volume {data['tv']} for {data['cd']}, total: {current_value}")
           except redis.RedisError as e:
               print(f"Redis operation failed: {e}")
           
           queue.task_done()
       except Exception as e:
           print(f"Error processing data: {e}")

async def upbit_ws_client():
   uri = "wss://api.upbit.com/websocket/v1"
   queue = Queue(maxsize=10000)
   
   # 처리 태스크 시작
   process_task = asyncio.create_task(process_data(queue))
   
   while True:
       try:
           async with websockets.connect(uri) as websocket:
               subscribe_fmt = [
                   {"ticket":"test"},
                   {
                       "type": "trade",
                       "codes": krw_markets,
                       "isOnlyRealtime": True
                   },
                   {"format": "SIMPLE"}
               ]
               await websocket.send(json.dumps(subscribe_fmt))
               
               while True:
                   try:
                       data = await websocket.recv()
                       data = json.loads(data.decode('utf8'))
                       
                       if queue.full():
                           print("Queue is full! Data might be lost.")
                       else:
                           await queue.put(data)
                           
                   except websockets.exceptions.ConnectionClosed:
                       print("WebSocket connection closed. Attempting to reconnect...")
                       break
                   except Exception as e:
                       print(f"Error receiving message: {e}")
                       continue
       except Exception as e:
           print(f"Connection error: {e}")
           await asyncio.sleep(5)

asyncio.run(upbit_ws_client())