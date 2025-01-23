**1. Websocket으로 실시간 데이터 수령**
  - Upbit Websocket에 연결 -> 실시간 데이터 수령
  - 데이터 유실방지를 위해 Queue에 데이터 저장
  - 가공 후 Redis에 저장
  - Redis에 저장된 데이터를 n초 간격으로 DB에 저장

**2. Requests로 주기적인 데이터 수령**
  - 1분, 1시간, 1일 간격으로 추가적인 정보 가져오기
    - 1분) 김프 계산을위해 coin_gecko에서 해외 코인가격 1분 간격으로 수집
    - 1시간) 실시간 데이터는 저장공간이 너무 큼. 저장공간을 줄이기 위해 1시간 간격으로 데이터를 새롭게 가져와 저장
    - 1일) 시가총액, 총 발행량같은 데이터를 1일 간격으로 가져와 저장

**3. 웹사이트 개발**
  - 웹사이트에 단타/스윙/일별 데이터 등 필요한 데이터 시각화
    - 단타) 
    - 스윙) 
    - 일별 데이터)
