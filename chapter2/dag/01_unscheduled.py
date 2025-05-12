import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 

dag = DAG(
    dag_id = "01_unscheduled",
    start_date=dt.datetime.now(), #현재 시간으로 DAG 시작
    schedule_interval=None, #스케줄되지 않는 DAG로 지정
)

## 02_daily_schedule.py
# dag = DAG(
#     dag_id = "02_daily_schedule",
#     schedule_interval = @daily, -> 매일 자정에 실행되도록 DAG를 스케줄
#     start_date = dt.datetime(2019, 1, 1), -> DAG 실행 스케줄을 시작할 날짜/시간
# )

## 03_with_end_date.py
# dag = DAG(
#     dag_id = "03_with_end_date",
#     schedule_interval = @daily,
#     start_date = dt.datetime(year = 2019, month = 1, day = 1),
#     end_date = dt.datetime(year = 2019, month = 1, day = 10),
# )

## 04_time_delta.py
# dag = DAG(
#     dag_id = "04_time_delta",
#     schedule_interval=dt.timedelta(days=3), -> timedelta는 빈도 기반 스케줄을 사용할 수 있는 기능을 제공한다.
#     start_date=dt.datetime(year = 2019, month = 1, day = 1),
#     end_date=dt.datetime(year = 2019, month  = 1, day = 5),
# )

fetch_event = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "https://raw.githubusercontent.com/AstronomyLive/airflow-" #API에서 이벤트를 가져온 후 저장
    ),
    dag=dag,
)

def _calculate_stats(**context): #모든 콘텍스트 변수를 수신한다.
    """이벤트 통계 계산하기"""
    # templates_dict 개체에서 템플릿 값을 검색한다.
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

calculate_stats=PythonOperator(
    task_id = "calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/data/events/{{ds}}.json", # 템플릿되는 값을 전달한다.
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag,
)

fetch_event >> calculate_stats #실행 순서 설정