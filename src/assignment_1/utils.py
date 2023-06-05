
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# creating a spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_1").getOrCreate()
    return spark


# creating a data frame for user.csv file
def user_dataframe(spark):
    user_df = spark.read.csv("../../resource/user.csv", header="true", inferSchema="true")
    return user_df


# creating a data frame of transaction.csv file
def transaction_dataframe(spark):
    transaction_df = spark.read.csv("../../resource/transaction.csv", header="true", inferSchema="true")
    return transaction_df


# Performing inner join to join user and transaction dataframes
def join_dataframe(user_df, transaction_df):
    join_df = transaction_df.join(user_df, user_df.user_id == transaction_df.userid, "inner")
    return join_df

# getting the unique locations where the product is sold
def unique_location(join_df):
    product_unique_locations = join_df.groupBy("product_description", "location ").agg(
        countDistinct("location "))
    return product_unique_locations

# getting  list of products by using user_id
def user_products(join_df):
    product_ = join_df.groupBy('userid').agg(collect_set("product_id").alias("Bought_Products"))
    return product_

# finding the total amount of spending by each user on each product
def total_spending(join_df):
    total = join_df.groupBy('userid', 'product_id').agg(sum('price'))
    return total



