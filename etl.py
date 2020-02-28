from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_list, lit, sum, coalesce, count, regexp_replace, split, explode
from pyspark.sql.types import StringType

URL_LOAD = "jdbc:mysql://localhost/arenaliv3aidi2017"
DRIVER = "com.mysql.jdbc.Driver"
USER = "dataengnr"
PASSWORD = "023y2hf82hfff"


def create_spark_session(app_name="Hireko", spark_master="local[*]"):
    return SparkSession.builder.master(spark_master).appName(app_name).getOrCreate()


def load_mysql(spark: SparkSession, tbl_name) -> DataFrame:
    """This method will load mysql table from giving name,
       then return as spark dataframe.
       Args: -spark = SparkSession for running job with spark
             -tbl_name = Name of the table that will be loaded

       Return: df : Spark Dataframe from mysql table"""
    print('Loading %s table to Spark Dataframe' % tbl_name)
    df = spark.read.format('jdbc').options(
        url=URL_LOAD,
        driver=DRIVER,
        dbtable=tbl_name,
        user=USER,
        password=PASSWORD).load()
    return df


def load_parquet(spark: SparkSession, parquet_name) -> DataFrame:
    """This method will load parquet giving name from HDFS,
           then return as spark dataframe.
           Args: -spark = SparkSession for running job with spark
                 -parquet_name = Name of the parquet that will be loaded

           Return: df : Spark Dataframe from mysql table"""
    print('Loading %s parquet to Spark Dataframe' % parquet_name)
    df = spark.read.parquet(parquet_name)
    return df


def save_parquet(df: DataFrame, parquet_name):
    """
    This method will save giving dataframe into HDFS
    :param df: Spark Dataframe
    :param parquet_name: Name of the saved parquet will be
    """
    print('Saving Spark Dataframe to Parquet %s files' % parquet_name)
    df.write.mode('overwrite').parquet(parquet_name)


def user_preprocessing(spark: SparkSession, save_path="users"):
    """
    This method will generate user list and its feature.
    Generated dataframe will contains user_id, birth_year, gender, category_subscribe, subscribe column.
    :param spark: spark Session
    """
    arena_user_data = load_mysql(spark, "arena_user_data")
    arena_category_subscribers = load_mysql(spark, "arena_category_subscribers")
    arena_categories = load_mysql(spark, "arena_categories")
    arena_user_subscribers = load_mysql(spark, "arena_user_subscribers")
    arena_user_data = arena_user_data.select("id", "birth_year", "gender")
    arena_category_subscribers = arena_category_subscribers.select("user_id", "cat_id").where(col("status") == 1)
    arena_categories = arena_categories.select("id", "cat_name")
    arena_user_subscribers = arena_user_subscribers.select("subscriber_id", "user_id").where(col("status") == 1)
    cat_name_subs = arena_category_subscribers.alias("a").join(arena_categories.alias("b"),
                                                               on=col("a.cat_id") == col("b.id"),
                                                               how="left").select("a.user_id", "b.cat_name")
    cat_name_subs = cat_name_subs.groupBy("user_id").agg(collect_list("cat_name")
                                                         .cast(StringType()).alias("category_subscribe"))
    cat_name_subs = cat_name_subs.withColumn("category_subscribe", regexp_replace(col("category_subscribe"), r"' ", r"'"))
    cat_name_subs = cat_name_subs.withColumn("category_subscribe", regexp_replace(col("category_subscribe"), r"[\[\]\']", r""))
    cat_name_subs = cat_name_subs.withColumn("category_subscribe", regexp_replace(col("category_subscribe"), r", ", r","))
    subscriber_list = arena_user_subscribers.groupBy("subscriber_id").agg(collect_list("user_id").cast(StringType()).alias("subscribe"))
    subscriber_list = subscriber_list.withColumn("subscribe", regexp_replace(col("subscribe"), r"[\[\]\'\s]", r""))
    users = arena_user_data.alias("a").join(cat_name_subs.alias("b"), on=col("a.id") == col("b.user_id"), how="left")\
        .join(subscriber_list.alias("c"), on=col("a.id") == col("c.subscriber_id"), how="left")\
        .select("a.*", "b.category_subscribe", "c.subscribe").orderBy("id")
    users = users.withColumnRenamed("id", "user_id")
    users = users.withColumn("category_subscribe", coalesce("category_subscribe")).withColumn("subscribe", coalesce("subscribe"))
    save_parquet(users, save_path)


def item_preprocessing(spark: SparkSession, save_path="items"):
    """
    This method will generate item list and its feature.
    Generated dataframe will contains item_id, title, cat_id, cat_name, genre_id, genre_name, eo_id column.
    :param spark: spark Session
    """
    arena_data = load_mysql(spark, "arena_data")
    arena_genres = load_mysql(spark, "arena_genres")
    arena_data = arena_data.select("id", "title", "cat_id", "genre_id", "eo_id")
    arena_genres = arena_genres.select("id", "genre_name")
    arena_categories = load_mysql(spark, "arena_categories")
    items = arena_data.alias("a").join(arena_categories.alias("b"), on=col("a.cat_id") == col("b.id"), how="left")\
        .join(arena_genres.alias("c"), on=col("a.genre_id") == col("c.id"), how="left")\
        .select("a.id", "a.title", "a.cat_id", "b.cat_name", "a.genre_id", "c.genre_name", "a.eo_id")
    items = items.withColumnRenamed("id", "item_id").orderBy("item_id", ascending=False)
    save_parquet(items, save_path)


def interaction_preprocessing(spark: SparkSession, user_existing = [], save_path="interactions"):
    """
    This method will generate interaction between user and item from user activity.
    Generated dataframe will contains user_id, item_id, rate columns.
    Rate value is a rank between 1 - 5 with this criteria :
        - User viewing stage : 1
        - User post expression : 1
        - User polling : 1
        - User create post in audience : 1
        - User filling questioner : 1
    :param spark: spark Session
    """
    action_log = load_mysql(spark, "action_log")
    arena_questioner = load_mysql(spark, "arena_questioner")
    questioner_user_participations = load_mysql(spark, "questioner_user_participations")
    action_types = ['view_stage', 'expression', 'polling', 'create_post_audience']
    action_log = action_log.select("user_id", "channel_id", "action_type").where(col("action_type").isin(action_types))
    action_log = action_log.where(~col("user_id").isNull()).withColumn("rate", lit(1))
    action_log = action_log.drop_duplicates()
    arena_questioner = arena_questioner.select("id", "arena_id").where(col("post_id") != 0)
    questioner_user_participations = questioner_user_participations.select("questioner_id", "user_id")
    questioner = arena_questioner.alias("a").join(questioner_user_participations.alias("b"),
                                                  on=col("a.id") == col("b.questioner_id"), how="inner")\
        .select("b.user_id", "a.arena_id")
    questioner = questioner.drop_duplicates()
    questioner = questioner.withColumn("action_type", lit("questioner")).withColumn("rate", lit(1))
    questioner = questioner.withColumnRenamed("arena_id", "channel_id")
    interactions = action_log.union(questioner)
    if user_existing:
        interactions = interactions.where(col("user_id").isin(user_existing))
    interactions = interactions.groupBy("user_id", "channel_id").agg(sum("rate").alias("rate"))
    interactions = interactions.withColumnRenamed("channel_id", "item_id")
    save_parquet(interactions, save_path)


def user_feature_preprocessing(spark: SparkSession, save_path="user_features"):
    """
    This method will generate every feature from the user.
    Generated dataframe will contains user_id, features and weight column
    Each feature will have value 1 on weight
    :param spark: spark Session
    """
    arena_user_subscribers = load_mysql(spark, "arena_user_subscribers")
    arena_user_data = load_mysql(spark, "arena_user_data")
    arena_category_subscribers = load_mysql(spark, "arena_category_subscribers")
    arena_categories = load_mysql(spark, "arena_categories")
    user_subscriber = arena_user_subscribers.withColumnRenamed("user_id", "feature")\
        .withColumnRenamed("subscriber_id", "user_id")
    user_subscriber = user_subscriber.select("user_id", "feature").where(col("status") == 1)
    user_data = arena_user_data.select("id", "gender").withColumnRenamed("id", "user_id")\
        .withColumnRenamed("gender", "feature")
    category_subscriber = arena_category_subscribers.select("user_id", "cat_id").where(col("status") == 1)
    category_subscriber = category_subscriber.alias("a").join(arena_categories.alias("b"),
                                                              on=col("a.cat_id") == col("b.id"), how="left")\
        .select("a.user_id", "b.cat_name")
    category_subscriber = category_subscriber.withColumnRenamed("cat_name", "feature")
    user_feature = user_data.union(category_subscriber).union(user_subscriber).orderBy("user_id")
    user_feature = user_feature.withColumn("weight", lit(1))
    save_parquet(user_feature, save_path)


def item_feature_preprocessing(spark: SparkSession, new_items=[], save_path="item_features"):
    """
    This method will generate every feature from the item.
    Generated dataframe will contains item_id, features and weight column
    Each feature will have value 1 on weight
    :param spark: spark Session
    """
    arena_data = load_mysql(spark, "arena_data")
    arena_categories = load_mysql(spark, "arena_categories")
    arena_genres = load_mysql(spark, "arena_genres")
    if new_items:
        arena_data = arena_data.where(col("id").isin(new_items))
    item_category = arena_data.alias("a").join(arena_categories.alias("b"), on=col('a.cat_id') == col("b.id"), how="left")\
        .select("a.id", "b.cat_name")
    item_category = item_category.withColumnRenamed("id", "item_id").withColumnRenamed("cat_name", "feature")
    item_genres = arena_data.alias("a").join(
        arena_genres.alias("b"), on=col("a.genre_id") == col("b.id")).select(
        "a.id", "b.genre_name")
    item_genres = item_genres.withColumnRenamed("id", "item_id").withColumnRenamed("genre_name", "feature")
    item_eo = arena_data.select("id", "eo_id").withColumnRenamed("id", "item_id").withColumnRenamed("eo_id", "feature")
    item_feature = item_category.union(item_genres).union(item_eo)
    item_feature = item_feature.withColumn("weight", lit(1)).orderBy("item_id")
    save_parquet(item_feature, save_path)


def get_existing_users(spark: SparkSession, parquet_dir):
    interactions = load_parquet(spark, parquet_dir + "parquet/interactions")
    user_existing = interactions.select(col("user_id")).distinct()
    user_existing = user_existing.repartition(1)
    user_existing.write.mode("overwrite").csv(parquet_dir + "user_existing", header=True)


def get_existing_items(spark: SparkSession, parquet_dir):
    interactions = load_parquet(spark, parquet_dir + "parquet/interactions")
    item_existing = interactions.select(col("item_id")).distinct()
    item_existing = item_existing.repartition(1)
    item_existing.write.mode("overwrite").csv(parquet_dir + "item_existing", header=True)