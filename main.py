import csv
import sys
from datetime import datetime

from dotenv import load_dotenv
import os
import psycopg2 as pg2

env = load_dotenv()
USER = os.getenv('DB_USER')
HOST = os.getenv('DB_HOST')
DB = os.getenv('DB_DB')
PASSWORD = os.getenv('DB_PASSWORD')
conn = pg2.connect(database=DB, user=USER, password=PASSWORD, host=HOST)
conn.autocommit = True
cur = conn.cursor()


def input_data(line):
    try:
        skill, name, price, hour, date, duration, user_tag, url, site, difficulty, tool, curriculum, characteristic = line
        name.replace('\n', ' ')
        price = int(price)
        hour = float(hour) if hour.upper() != "UNKNOWN" else -1
        date = int(date) if date.upper() != "UNKNOWN" else -1
        duration = int(duration) if duration != "∞" else 100000000

        optional = "False"
        return_time = "UNKNOWN"

        return_time_type = ["FAST", "UNKNOWN", "LATE"]
        if return_time.upper() in return_time_type:
            return_time = return_time_type.index(return_time.upper()) + 1
        else:
            raise Exception("질문 답변 응답시간 형식 에러")

        user_tag = list(map(str.strip, user_tag.split(",")))

        preview = "False"

        cur.execute('select * from category')
        rets = cur.fetchall()
        categories = {}
        for ret in rets:
            categories[ret[1]] = ret[0]

        if category in categories:
            category_id = categories[category]
        else:
            cur.execute('INSERT INTO category(name) VALUES(\'{}\') RETURNING *'.format(category))
            category_id = cur.fetchall()[0][0]
            conn.commit()

        cur.execute('SELECT id, name FROM skill WHERE category_id = {}'.format(category_id))
        rets = cur.fetchall()
        skills = {}
        for ret in rets:
            skills[ret[1]] = ret[0]
        if skill in skills:
            skill_id = skills[skill]
        else:
            cur.execute('INSERT INTO skill(name, category_id) VALUES(\'{}\',{}) RETURNING *'.format(skill, category_id))
            skill_id = cur.fetchall()[0][0]
            conn.commit()

        cur.execute('SELECT id, name FROM tag WHERE skill_id = {}'.format(skill_id))
        rets = cur.fetchall()
        tags = {}
        user_tag_ids = []
        for ret in rets:
            tags[ret[1]] = ret[0]
        for ut in user_tag:
            if ut in tags:
                user_tag_ids.append(tags[ut])
            else:
                if ut in difficulty:
                    tag_type_id = 1
                elif ut in tool:
                    tag_type_id = 2
                elif ut in curriculum:
                    tag_type_id = 3
                elif ut in characteristic:
                    tag_type_id = 4
                else:
                    raise Exception("태그 형식 에러")
                cur.execute('INSERT INTO tag(name, skill_id, tag_type_id) VALUES(\'{}\',{}) RETURNING *'.format(ut, skill_id, tag_type_id))
                user_tag_ids.append(cur.fetchall()[0][0])
                conn.commit()

        cur.execute('''
        INSERT INTO 
        lecture(name, price, time, start_year, duration, review_time_id, has_preview, has_optional, url, category_id, skill_id, site)
        VALUES('{}',{},{},{},{},{},{},{},'{}',{},{},'{}')
        RETURNING id
        '''.format(name, price, hour, date, duration, return_time, preview, optional, url, category_id, skill_id, site))
        lecture_id = cur.fetchall()[0][0]
        conn.commit()

        for user_tag_id in user_tag_ids:
            cur.execute('''
            INSERT INTO
            tag_lecture(lecture_id, tag_id)
            VALUES({}, {})
            '''.format(lecture_id, user_tag_id))
            conn.commit()

        conn.commit()
        return True, None
    except Exception as error:
        return False, error


filename = input("파일 이름을 입력하세요: ").strip()
cur.execute('select * from category')
print("\n현재 DB 내 존재하는 분야")
rets = cur.fetchall()
categories = {}
for ret in rets:
    categories[ret[1]] = ret[0]
print(list(categories.keys()))
category = input("\n분야를 입력하세요 : ").strip()
csv_file = open(filename, 'r', encoding='cp949')
field = csv_file.readline().strip()
print("필드 - " + field)

if field != "스킬,Name,가격,총 소요시간,시작일,수강 기간,Tag,URL,강의사이트,난이도,사용툴,커리큘럼,강좌특성":
    print("필드 값이 적절하지 않습니다.")
    print("적절한 필드 값 : 스킬,Name,가격,총 소요시간,시작일,수강 기간,Tag,URL,강의사이트,난이도,사용툴,커리큘럼,강좌특성")
    sys.exit()

reader = csv.reader(csv_file)
now = datetime.now()

no_output_file = True

input_num = 0
not_input_num = 0

for data in reader:
    input_num += 1
    success, msg = input_data(data)
    if not success:
        if no_output_file:
            output_file = open(
                "{}-error-{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}.csv".format(filename.split(".")[0], now.year, now.month,
                                                                           now.day, now.hour, now.minute, now.second),
                "w", newline='')
            writer = csv.writer(output_file)
            output_file.write(field + "\n")
            no_output_file = False
        writer.writerow(data)
        not_input_num += 1
        print(msg, "-", data)

if no_output_file:
    print("{}개의 모든 데이터가 정상 등록 완료되었습니다.".format(input_num))
else:
    print("{}개 중 {}개 데이터가 등록되지 않았습니다.".format(input_num, not_input_num))
    print("비정상 데이터는 error.csv 파일에 저장되었습니다.")

conn.close()
