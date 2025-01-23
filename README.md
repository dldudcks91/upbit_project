**1. Websocket으로 실시간 데이터 수령**
  - Upbit Websocket에 연결 -> 실시간 데이터 수령
  - 데이터 유실방지를 위해 Queue에 데이터 저장
  - 가공 후 Redis에 저장
  - Redis에 저장된 데이터를 n초 간격으로 DB에 저장

**2. Requests로 주기적인 데이터 수령**
  - 1분, 1시간, 하루 간격으로 추가적인 정보 가져오기
  - 김프 계산을위해 coin_gecko에서 해외 코인가격 등 필요한 데이터 수집
