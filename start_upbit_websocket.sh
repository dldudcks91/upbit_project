export PYTHONPATH=/home/ubuntu/upbit_project
export LANG=en_US.UTF-8
# 메모리 모니터링 스크립트 실행
#/usr/bin/python3 /home/ubuntu/baseball_project/system_monitor/memory__monitor.py &
# CPU 모니터링 스크립트 실행
#/usr/bin/python3 /home/ubuntu/baseball_project/system_monitor/cpu_monitor.py &
# 백그라운드 프로세스들이 종료되지 않도록 대기
/usr/bin/python3 /home/ubuntu/upbit_project/upbit_websocket &
wait

