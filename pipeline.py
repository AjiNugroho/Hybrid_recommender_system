from Rebuild_Model import Rebuild_Model
import etl
import argparse

HDFS_DIR = "hdfs://159.69.109.173:9000/user/sbgs-workspace1/recsys/"


def check_new_item():
    spark = etl.create_spark_session(spark_master="spark://159.69.109.173:7077", app_name="check_new_item")
#    spark.sparkContext.addPyFile("lib.zip")
    interaction_weekly = etl.load_parquet(spark, HDFS_DIR + "parquet-daily/interactions")
    interaction_daily = etl.load_parquet(spark, HDFS_DIR + "parquet-hourly/interactions")
    weekly_items = set([x["item_id"] for x in interaction_weekly.select("item_id").distinct().collect()])
    daily_items = set([x["item_id"] for x in interaction_daily.select("item_id").distinct().collect()])
    new_items = list(daily_items.difference(weekly_items))
    if len(new_items) > 0:
        etl.new_item_feature_preprocessing(spark, new_items, HDFS_DIR + "parquet-hourly/new-item-features")
        return True
    else:
        return False


def pipeline(task, **kwargs):
    spark = etl.create_spark_session(spark_master="spark://159.69.109.173:7077", app_name=task)
#    spark.sparkContext.addPyFile("lib.zip")
    if task == "hourly-etl":
        etl.user_preprocessing(spark, HDFS_DIR + "parquet-hourly/users")
        etl.item_preprocessing(spark, HDFS_DIR + "parquet-hourly/items")
        etl.interaction_preprocessing(spark, HDFS_DIR + "parquet-hourly/interactions")
        etl.user_feature_preprocessing(spark, HDFS_DIR + "parquet-hourly/user-features")
        etl.item_feature_preprocessing(spark, HDFS_DIR + "parquet-hourly/item-features")

    if task == "daily-etl":
        etl.user_preprocessing(spark, HDFS_DIR + "parquet-daily/users")
        etl.item_preprocessing(spark, HDFS_DIR + "parquet-daily/items")
        etl.interaction_preprocessing(spark, HDFS_DIR + "parquet-daily/interactions")
        etl.user_feature_preprocessing(spark, HDFS_DIR + "parquet-daily/user-features")
        etl.item_feature_preprocessing(spark, HDFS_DIR + "parquet-daily/item-features")

    if task == "update-model":
        if check_new_item():
            rebuild = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet-hourly/", online=True, new_item_exist=True)
        else:
            rebuild = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet-hourly/", online=True)
        rebuild.get_model_perfomance()

    if task == "rebuild-model":
        rebuild = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet-daily/", online=False)
        rebuild.get_model_perfomance()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True, type=str, help="task name")
    args = parser.parse_args()
    pipeline(task=args.task)
