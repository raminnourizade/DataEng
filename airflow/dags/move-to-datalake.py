from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
import pandas as pd
import glob
import os

#--------------------------------------------------------------------------------

default_args = {
    "owner": "Data Science Group",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 18),
    "email": ["fozouni@gonbad.ac.ir"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

STAGE2 = "/home/amin/data/stage/step2"
LAKE = "/home/amin/data/lake"

dag = DAG("Move-To-Datalake", 
          default_args=default_args, 
          schedule_interval="*/2 * * * *", 
          catchup=False, 
          template_searchpath='/home/amin/data/scripts')

#------------------------------------Python-Functions---------------------------------------

def convert_files_to_parquet(**kwargs):
    flist = glob.glob(os.path.join(STAGE2, "*.csv"))
    for i in flist:
        df = pd.read_csv(i, header=None, names=['id', 'sendTime', 'sendTimePersian', 'senderName', 'senderUsername', 'type', 'content'], dtype={'content': object})
        df.to_parquet(os.path.join(LAKE, f"{os.path.basename(i).split('.')[0]}.parquet"))

def say_hello():
    print("Hello")

#------------------------------------Operators---------------------------------------

combine_csv_files = BashOperator(
    task_id='Combine-CSV-Files',
    bash_command="combine_csv.sh",
    dag=dag,
)

convert_to_parquet = PythonOperator(
    task_id='Convert-Hourly-CSV-To-Parquet',
    python_callable=convert_files_to_parquet,
    dag=dag,
)

congratulation = PythonOperator(
    task_id="Congratulation-to-you",
    python_callable=say_hello,
    dag=dag,
)

#----------------------- DAG Structure -------------------------------

combine_csv_files >> convert_to_parquet >> congratulation

#----------------------------------------------------------------------

