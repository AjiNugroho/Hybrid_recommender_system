from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {"owner": "airflow", "start_date": datetime(2020, 2, 26)}

dag = DAG(dag_id="rebuild-model", default_args=args, schedule_interval="@daily")

cmd_etl = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task daily-etl"
)

cmd_rebuild_model = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task rebuild-model"
)

cmd_upload_dill = (
    "hdfs dfs "
    "-put /home/sbgs-workspace1/recsys/dill/* "
    "/user/sbgs-workspace1/recsys/dill/"
)

etl = BashOperator(
    task_id='daily-etl',
    bash_command=cmd_etl,
    dag=dag
)

rebuild_model = BashOperator(
    task_id='rebuild-model',
    bash_command=cmd_rebuild_model,
    dag=dag
)

upload_dill = BashOperator(
    task_id='save-dill-to-hdfs',
    bash_command=cmd_upload_dill,
    dag=dag
)

etl >> rebuild_model >> upload_dill
