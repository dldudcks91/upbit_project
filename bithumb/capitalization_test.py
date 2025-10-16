import aiohttp
import asyncio
import re
from typing import List, Dict, Any
import nest_asyncio
nest_asyncio.apply()
import pandas as pd
async def fetch_market_info(session: aiohttp.ClientSession, code: int) -> Dict[str, Any]:
    """단일 마켓 정보를 비동기로 가져오는 함수"""
    url = f'https://gw.bithumb.com/exchange/v2/trade/info-coin/C{code:04d}-C0100?lang=korean&_=1760549537926&retry=0'
    
    try:
        async with session.get(url) as response:
            if response.status == 200:
                response_data = await response.json()
                
                # 데이터가 있는 경우에만 처리
                if response_data.get('data'):
                    market = 'KRW-' + response_data['data']['scoinType']
                    total_supply = float(response_data['data']['marketTotalSupply'].replace(',', ''))
                    yester_day_price = float(response_data['data']['yesterdayPrice'])
                    capitalization = total_supply * yester_day_price
                    
                    return {
                        'code': code,
                        'market': market,
                        'total_supply': total_supply,
                        'yesterday_price': yester_day_price,
                        'capitalization': capitalization,
                        'raw_data': response_data
                    }
            return {'code': code, 'error': f'Status code: {response.status}'}
    except Exception as e:
        return {'code': code, 'error': str(e)}

async def fetch_batch(session: aiohttp.ClientSession, codes: List[int]) -> List[Dict]:
    """배치 단위로 비동기 요청을 처리하는 함수"""
    tasks = [fetch_market_info(session, code) for code in codes]
    return await asyncio.gather(*tasks)

async def fetch_all_markets(start: int = 0, end: int = 2000, batch_size: int = 100):
    """모든 마켓 정보를 배치 단위로 가져오는 메인 함수"""
    all_results = []
    
    # aiohttp 세션 생성 (커넥터 설정으로 동시 연결 수 제한)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 코드를 배치 단위로 나누기
        for batch_start in range(start, end + 1, batch_size):
            batch_end = min(batch_start + batch_size, end + 1)
            codes = list(range(batch_start, batch_end))
            
            print(f"Processing codes {batch_start} to {batch_end - 1}...")
            
            # 현재 배치 실행
            batch_results = await fetch_batch(session, codes)
            all_results.extend(batch_results)
            
            # 마지막 배치가 아니면 1초 대기
            if batch_end <= end:
                print(f"Batch complete. Waiting 1 second...")
                await asyncio.sleep(1)
    
    return all_results
#%%
async def main():
    """실행 예제"""
    # 모든 마켓 정보 가져오기
    results = await fetch_all_markets(start=0, end=2000, batch_size=100)
    
    # 성공한 결과만 필터링
    successful_results = [r for r in results if 'error' not in r]
    failed_results = [r for r in results if 'error' in r]
    
    print(f"\n총 {len(results)}개 요청 완료")
    print(f"성공: {len(successful_results)}개")
    print(f"실패: {len(failed_results)}개")
    
    # 성공한 결과 중 상위 10개 출력
    print("\n=== 상위 10개 결과 ===")
    for result in successful_results[:10]:
        if 'market' in result:
            print(f"Code: {result['code']:04d}, Market: {result['market']}, "
                  f"시가총액: {result['capitalization']:,.0f} KRW")
    
    return results
def create_dataframe(results):
    """결과 리스트에서 성공한 데이터만 추출하여 DataFrame 생성"""
    
    # 성공한 결과만 필터링 (error가 없고 market이 있는 경우)
    successful_data = [
        {
            'market': r['market'],
            'capitalization': r['capitalization']
        }
        for r in results 
        if 'error' not in r and 'market' in r
    ]
    
    # DataFrame 생성
    df = pd.DataFrame(successful_data)
    
    # capitalization으로 내림차순 정렬 (옵션)
    if not df.empty:
        df = df.sort_values('capitalization', ascending=False).reset_index(drop=True)
    
    return df
#%%
# 실행
if __name__ == "__main__":
    # 이벤트 루프 실행
    results = asyncio.run(main())
    
    # 결과를 DataFrame으로 변환하려면:
    # import pandas as pd
    # df = pd.DataFrame([r for r in results if 'error' not in r])
    # df.to_csv('market_info.csv', index=False)
    
#%%
df = create_dataframe(results)