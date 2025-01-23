import requests
import redis
import websockets
import json
import asyncio
from datetime import datetime
from asyncio import Queue

def get_krw_markets():
   url = "https://api.upbit.com/v1/market/all"
   response = requests.get(url)
   markets = response.json()
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
       r.ping()
       print("Successfully connected to Redis")
       return r
   except redis.ConnectionError as e:
       print(f"Could not connect to Redis: {e}")
       print("Please make sure Redis server is running")
       raise

async def process_data(queue):
   """데이터 처리를 담당하는 코루틴"""
   while True:
       try:
           data = await queue.get()
           
           TEN_SECONDS = 10000
           base_timestamp = data['tms'] - (data['tms'] % TEN_SECONDS)
           key = f"trade_volume:{data['cd']}:{base_timestamp}"
           
           try:
               r.execute_command('INCRBYFLOAT', key, str(data['tv']))
               r.expire(key, 60)
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
                       
                       # 큐가 가득 차있는지 확인
                       if queue.full():
                           print("Queue is full! Some data might be lost.")
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

async def main():
   try:
       global krw_markets, r
       krw_markets = get_krw_markets()
       r = connect_redis()
       await upbit_ws_client()
   except KeyboardInterrupt:
       print("Program terminated by user")
   except Exception as e:
       print(f"Unexpected error: {e}")

if __name__ == "__main__":
   asyncio.run(main())