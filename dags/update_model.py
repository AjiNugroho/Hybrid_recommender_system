from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {"owner": "airflow", "start_date": datetime(2020, 2, 26)}

dag = DAG(dag_id="rebuild-model", default_args=args, schedule_interval="@daily")

cmd_etl = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl"
)

cmd_update_model = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task update-model"
)

cmd_upload_dill = (
    "hdfs dfs "
    "-put -f /home/sbgs-workspace1/recsys/dill/* "
    "/user/sbgs-workspace1/recsys/dill/"
)

etl = BashOperator(
    task_id='hourly-etl',
    bash_command=cmd_etl,
    dag=dag
)

update_model = BashOperator(
    task_id='update-model',
    bash_command=cmd_update_model,
    dag=dag
)

upload_dill = BashOperator(
    task_id='save-dill-to-hdfs',
    bash_command=cmd_upload_dill,
    dag=dag
)

etl >> update_model >> upload_dill
