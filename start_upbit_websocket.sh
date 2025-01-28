#!/bin/bash

export PYTHONPATH=/home/ubuntu/upbit_project
export LANG=en_US.UTF-8

# 웹소켓 스크립트
/usr/bin/python3 /home/ubuntu/upbit_project/upbit_websocket.py &

# 가격 데이터 수집 스크립트 실행
/usr/bin/python3 /home/ubuntu/upbit_project/upbit_by_seconds.py &

wait

