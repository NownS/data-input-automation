# GrowTo Data insertion tool
GrowTo 서버의 데이터 삽입을 도와주는 도구입니다.

# Usage
```
python3 main.py
``` 
# Requirement
```
python 3.8.1
psycopg2==2.9.3
python-dotenv==0.19.2
```
# Explanation
```
프로그램을 실행시킨 뒤, csv 파일 경로와 이름을 입력합니다.
필드의 형식이 맞으면 프로그램이 실행됩니다.

형식이 맞지 않는 row의 경우, ${파일이름}-error-${현재시간}.csv 형식의 파일에 따로 저장되어 차후 데이터를 정제한 후 다시 프로그램을 통해 데이터를 삽입하면 됩니다.

콘솔 창을 통해 어떤 문제 때문에 형식이 맞지 않는지 확인할 수 있습니다.

데이터베이스의 .env 파일을 이용하여 접속하기 때문에, main.py 파일의 경로에 .env 파일을 저장해야 합니다.
