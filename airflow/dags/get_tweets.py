from airflow import DAG
from airflow.operators.bash import BashOperator  # Updated import
from airflow.operators.empty import EmptyOperator  # Updated import
from datetime import datetime, timedelta

default_args = {
    "owner": "DSG-Fozouni",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 17, 0, 0),
    "email": ["fozouni@gonbad.ac.ir"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "Get-Tweets", 
    default_args=default_args, 
    schedule="* * * * *",  # Updated to 'schedule' instead of 'schedule_interval'
    catchup=False
)

#-------------Operators--------------------------------

get_tweets = BashOperator(
    task_id="Get-Sahamyab-Tweets",
    bash_command=(
        "/usr/bin/curl -s -H 'User-Agent:Chrome/123.0' https://www.sahamyab.com/guest/twiter/list?v=0.1 | "
        "/usr/bin/jq '.items[0,2,3,4,5,6,7,8,9,10] | [.id, .sendTime, .sendTimePersian, .senderName, "
        ".senderUsername, .type, .content] | join(\",\") ' > /workspaces/DataEng/airflow/data/stage/step1/$(date +%s).csv"
    ),
    dag=dag,
)

task_dummy = EmptyOperator(task_id="Dummy-Operator", dag=dag)  # Using EmptyOperator instead of DummyOperator

#----------------------- DAG Structure -------------------------------

get_tweets >> task_dummy
