from pyspark.sql import *
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import *
import logging
import sys

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s [%(levelname)s] %(message)s",
handlers=[
logging.FileHandler("../../log/assignment_1.log"),
logging.StreamHandler(sys.stdout)
]
)


def spark_session():
    spark = SparkSession.builder.appName("Assignment 1").getOrCreate()
    logging.info("spark session created")
    return spark

def creating_rdd(spark,filepath):
    rdd = spark.sparkContext.textFile(filepath)
    logging.info("creating data frame")
    return rdd

def num_of_lines(log_rdd):
    count_of_lines = log_rdd.count()
    logging.info("number of lines:%s" %count_of_lines)
    return count_of_lines



def reading_file(spark,filepath):
    df = spark.read.text(filepath)
    logging.info("reading an text file")
    return df


def create_dataframe_from_rdd(df):
    df1 = df.withColumn("log message", split(col("value"), ",").getItem(0))\
        .withColumn("timestamp", split(col("value"), ",").getItem(1))\
        .withColumn("downloader_id", split(col("value"), ",").getItem(2)).drop(col("value"))
    df2=df1.withColumn("downloader",split(col("downloader_id"),"--").getItem(0)).\
        withColumn("repository_client",split(col("downloader_id"),"--").getItem(1)).drop("downloader_id")
    df_torr=df2.withColumn("repository_clients",split(col("repository_client"),":").getItem(0)).withColumn("commit_message",split(col("repository_client"),":").getItem(1)).drop("repository_client")
    logging.info("Creating a dataframe")
    return df_torr



def warn_messages(df_torrent):
    df_warn_msg=df_torrent.filter(col("log message")=="WARN")
    warn_count = df_warn_msg.count()
    logging.info("Number of warning messages:%s" %warn_count)
    return warn_count
def api_clients(df_torrent):
    df_processed_repositories=df_torrent.filter(col("repository_clients").like('%api_client%'))
    df_processed_repos=df_processed_repositories.count()
    logging.info("Repositories processed only api clients:%s" %df_processed_repos)
    return df_processed_repos

def most_http_requests(df_torrent):
    df_most_http_requests=df_torrent.filter(col("commit_message").like('%https%'))
    df_most_requests=df_most_http_requests.groupBy("repository_clients").count().orderBy("count", ascending=False).limit(1).drop("count")
    logging.info("most_http_requests")
    return df_most_requests

def most_failed_requests(df_torrent):
    df_most_failed_requests=df_torrent.filter(col("commit_message").like('%Failed%'))
    df_more_failed_requests=df_most_failed_requests.groupBy("repository_clients").count().orderBy("count", ascending=False).limit(1).drop("count")
    logging.info("Most failed requests")
    return df_more_failed_requests

def most_active_hours(df_torrent):
    df_most_active_hour=df_torrent.withColumn("hour", hour("timestamp"))
    df_more_active_hours = df_most_active_hour.groupBy("hour").count().orderBy("count", ascending=False).limit(1).drop("count")
    logging.info("most active hours")
    return df_more_active_hours

def most_active_repositories(df_torrent):
    df_most_active_repositories=df_torrent.filter(col("repository_clients")==" ghtorrent.rb")
    logging.info("most_active_repositories")
    return df_most_active_repositories

