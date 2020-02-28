from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {"owner": "airflow", "start_date": datetime(2020, 2, 26)}

dag = DAG(dag_id="update-model", default_args=args, schedule_interval="@hourly")

cmd_etl_users = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub users"
)

cmd_etl_items = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub items"
)

cmd_etl_interactions = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub interactions"
)

cmd_etl_user_features = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub user_features"
)

cmd_etl_item_features = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub item_features"
)

cmd_etl_new_item_features = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub new_item_features"
)

cmd_etl_update_item_existing = (
    "spark-submit "
    "--master spark://159.69.109.173:7077 "
    "/home/sbgs-workspace1/recsys/pipeline.py "
    "--task hourly-etl --sub update_item_existing"
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

users_etl = BashOperator(
    task_id='users-etl',
    bash_command=cmd_etl_users,
    dag=dag
)

items_etl = BashOperator(
    task_id='items-etl',
    bash_command=cmd_etl_items,
    dag=dag
)

interactions_etl = BashOperator(
    task_id='interactions-etl',
    bash_command=cmd_etl_interactions,
    dag=dag
)

user_features_etl = BashOperator(
    task_id='user_features-etl',
    bash_command=cmd_etl_user_features,
    dag=dag
)

item_features_etl = BashOperator(
    task_id='item_features-etl',
    bash_command=cmd_etl_item_features,
    dag=dag
)

new_item_features_etl = BashOperator(
    task_id='new_item_features-etl',
    bash_command=cmd_etl_new_item_features,
    dag=dag
)

update_item_existing_etl = BashOperator(
    task_id='update_item_existing-etl',
    bash_command=cmd_etl_update_item_existing,
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

users_etl >> new_item_features_etl
items_etl >> new_item_features_etl
interactions_etl >> new_item_features_etl
user_features_etl >> new_item_features_etl
item_features_etl >> new_item_features_etl
new_item_features_etl >> update_item_existing_etl
update_item_existing_etl >> update_model
update_model >> upload_dill