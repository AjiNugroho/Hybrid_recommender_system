from Rebuild_Model import Rebuild_Model
import etl
import argparse

SPARK_MASTER = "spark://159.69.109.173:7077"
HDFS_DIR = "hdfs://159.69.109.173:9000/user/sbgs-workspace1/recsys/"
LOCAL_DILL = "/home/sbgs-workspace1/recsys/dill/"


def get_new_item():
    spark = etl.create_spark_session(spark_master=SPARK_MASTER, app_name="get_new_item")
    interactions = etl.load_parquet(spark, HDFS_DIR + "parquet/interactions")
    item_existing = spark.read.csv(HDFS_DIR + "item_existing", header=True)
    interactions_items = set([x["item_id"] for x in interactions.select("item_id").distinct().collect()])
    existing_items = set([x["item_id"] for x in item_existing.select("item_id").collect()])
    new_items = list(interactions_items.difference(existing_items))
    return new_items


def check_new_item():
    spark = etl.create_spark_session(spark_master=SPARK_MASTER, app_name="check_new_item")
    new_items = etl.load_parquet(spark, HDFS_DIR + "parquet/new-item-features")
    if new_items.count() > 0:
        return True
    else:
        return False


def pipeline(task, subtask):
    spark = etl.create_spark_session(spark_master=SPARK_MASTER, app_name=task)
    #    spark.sparkContext.addPyFile("lib.zip")
    if task == "hourly-etl":
        if subtask == "users":
            etl.user_preprocessing(spark, save_path=HDFS_DIR + "parquet/users")
        elif subtask == "items":
            etl.item_preprocessing(spark, save_path=HDFS_DIR + "parquet/items")
        elif subtask == "interactions":
            user_existing = spark.read.csv(HDFS_DIR + "user_existing", header=True)
            existing_users = set([x["user_id"] for x in user_existing.select("user_id").collect()])
            etl.interaction_preprocessing(spark, user_existing=existing_users,
                                          save_path=HDFS_DIR + "parquet/interactions")
        elif subtask == "user_features":
            etl.user_feature_preprocessing(spark, save_path=HDFS_DIR + "parquet/user-features")
        elif subtask == "item_features":
            etl.item_feature_preprocessing(spark, save_path=HDFS_DIR + "parquet/item-features")
        elif subtask == "new_item_features":
            etl.get_existing_items(spark, parquet_dir=HDFS_DIR)
            new_items = get_new_item()
            if new_items:
                etl.item_feature_preprocessing(spark, new_items=new_items,
                                               save_path=HDFS_DIR + "parquet/new-item-features")
        elif subtask == "update_item_existing":
            etl.get_existing_items(spark, parquet_dir=HDFS_DIR)

    if task == "daily-etl":
        if subtask == "users":
            etl.user_preprocessing(spark, save_path=HDFS_DIR + "parquet/users")
        elif subtask == "items":
            etl.item_preprocessing(spark, save_path=HDFS_DIR + "parquet/items")
        elif subtask == "interactions":
            etl.interaction_preprocessing(spark, save_path=HDFS_DIR + "parquet/interactions")
        elif subtask == "user_features":
            etl.user_feature_preprocessing(spark, save_path=HDFS_DIR + "parquet/user-features")
        elif subtask == "item_features":
            etl.item_feature_preprocessing(spark, save_path=HDFS_DIR + "parquet/item-features")
            # set new item features as empty
            etl.item_feature_preprocessing(spark, new_items=new_items,
                                           save_path=HDFS_DIR + "parquet/new-item-features")
        elif subtask == "user_item_existing":
            etl.get_existing_users(spark, parquet_dir=HDFS_DIR)
            etl.get_existing_items(spark, parquet_dir=HDFS_DIR)

    if task == "update-model":
        if check_new_item():
            model = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet/", local_dill_path=LOCAL_DILL,
                          online=True, new_item_exist=True)
            auc = model.get_model_perfomance()
            with open(LOCAL_DILL + "auc.txt", "w") as f:
                f.write("AUC SCORE : " + str(auc))
                f.close()
        else:
            model = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet/", local_dill_path=LOCAL_DILL,
                          online=True)
            auc = model.get_model_perfomance()
            with open(LOCAL_DILL + "auc.txt", "w") as f:
                f.write("AUC SCORE : " + str(auc))
                f.close()

    if task == "rebuild-model":
        model = Rebuild_Model(spark_session=spark, parquet_dir=HDFS_DIR + "parquet/", local_dill_path=LOCAL_DILL,
                      online=False)
        auc = model.get_model_perfomance()
        with open(LOCAL_DILL + "auc.txt", "w") as f:
            f.write("AUC SCORE : " + str(auc))
            f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True, type=str, help="task name")
    parser.add_argument("--sub", type=str, help="subtask name")
    args = parser.parse_args()
    pipeline(task=args.task, subtask=args.sub)
