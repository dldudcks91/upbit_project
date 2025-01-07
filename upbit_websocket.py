
import requests
#import nest_asyncio
import redis
import websockets
import json
import asyncio
from datetime import datetime

# IPython/Jupyter 환경에서 필요
#nest_asyncio.apply()

def get_krw_markets():
    """업비트 KRW 마켓의 모든 거래쌍 조회"""
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    markets = response.json()
    # KRW 마켓만 필터링
    krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
    print(f"Total KRW markets: {len(krw_markets)}")
    krw_markets.remove('KRW-BTC')
    return krw_markets

# Redis 연결 함수
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
   
#%%
async def upbit_ws_client():
    uri = "wss://api.upbit.com/websocket/v1"
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
                        save_data = {"tms":data["tms"], "cd":data["cd"], "tv":data["tv"]}
                        
                        FIVE_MINUTES = 300000
                        base_timestamp = data['tms'] - (data['tms'] % FIVE_MINUTES)
                        
                        # Redis 키 생성
                        key = f"trade_volume:{data['cd']}:{base_timestamp}"
                        
                        try:
                            # volume 값을 누적 (INCRBYFLOAT 사용)
                            r.execute_command('INCRBYFLOAT', key, str(data['tv']))
                            # TTL 설정
                            r.expire(key, 1200)  # 20분
                        except redis.RedisError as e:
                            print(f"Redis operation failed: {e}")
                            continue
                        
                        # 로깅
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        #print(f"[{current_time}] Processed: {data['cd']} - Volume: {data['tv']}")
                        
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
        await upbit_ws_client()
    except KeyboardInterrupt:
        print("Program terminated by user")
    except Exception as e:
        print(f"Unexpected error: {e}")


asyncio.run(main())
