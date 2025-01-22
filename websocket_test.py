import requests
import redis
import websockets
import json
import asyncio
import time
from datetime import datetime
from collections import deque
import orjson

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

krw_markets = get_krw_markets()
try:
    r = connect_redis()
except Exception as e:
    print(f"Redis connection failed: {e}")
async def upbit_ws_client():
    uri = "wss://api.upbit.com/websocket/v1"
    # 성능 측정을 위한 변수들
    message_count = 0
    performance_stats = deque(maxlen=1000)
    start_time = time.time()
    
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
                await websocket.send(orjson.dumps(subscribe_fmt))
                
                while True:
                    try:
                        # 데이터 수신 시작 시간
                        recv_start = time.perf_counter()
                        data = await websocket.recv()
                        recv_time = time.perf_counter() - recv_start
                        
                        # 처리 시작 시간
                        process_start = time.perf_counter()
                        
                        # 데이터 처리
                        data = orjson.loads(data.decode('utf8'))
                        TEN_SECONDS = 10000
                        base_timestamp = data['tms'] - (data['tms'] % TEN_SECONDS)
                        key = f"test_trade_volume:{data['cd']}:{base_timestamp}"
                        
                        # Redis 처리
                        r.execute_command('INCRBYFLOAT', key, str(data['tv']))
                        r.expire(key, 60)
                        
                        # 처리 완료 시간 계산
                        process_time = time.perf_counter() - process_start
                        total_time = recv_time + process_time
                        
                        # 통계 저장
                        performance_stats.append({
                            'recv_time': recv_time,
                            'process_time': process_time,
                            'total_time': total_time
                        })
                        
                        message_count += 1
                        
                        # 매 1000개 메시지마다 통계 출력
                        if message_count % 1000 == 0:
                            current_time = time.time()
                            elapsed_time = current_time - start_time
                            avg_recv = sum(stat['recv_time'] for stat in performance_stats) / len(performance_stats)
                            avg_process = sum(stat['process_time'] for stat in performance_stats) / len(performance_stats)
                            avg_total = sum(stat['total_time'] for stat in performance_stats) / len(performance_stats)
                            
                            print(f"\n=== Performance Statistics (Last 1000 messages) ===")
                            print(f"Messages per second: {message_count/elapsed_time:.2f}")
                            print(f"Average receive time: {avg_recv*1000:.2f}ms")
                            print(f"Average process time: {avg_process*1000:.2f}ms")
                            print(f"Average total time: {avg_total*1000:.2f}ms")
                            print(f"Total messages processed: {message_count}")
                            print("================================================\n")
                            
                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed. Attempting to reconnect...")
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        continue
                        
        except Exception as e:
            print(f"Connection error: {e}")
            await asyncio.sleep(5)

async def main():
    try:
        krw_markets = get_krw_markets()
        r = connect_redis()
        await upbit_ws_client()
    except KeyboardInterrupt:
        print("Program terminated by user")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(main())