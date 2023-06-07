
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# creating a spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_1").getOrCreate()
    return spark


# creating a data frame for user.csv file
def users_data(spark):
    user_df = spark.read.csv("../../resource/user.csv", header="true", inferSchema="true")
    return user_df


# creating a data frame of transaction.csv file
def transc_data(spark):
    transaction_df = spark.read.csv("../../resource/transaction.csv", header="true", inferSchema="true")
    return transaction_df


# Performing inner join to join user and transaction dataframes
def join_dataframe(user_df, transaction_df):
    join_df = transaction_df.join(user_df, user_df.user_id == transaction_df.userid, "inner")
    return join_df

# getting the unique locations where the product is sold
def loc_unique(join_df):
    unique_locations = join_df.groupBy("product_description", "location ").agg(
        countDistinct("location "))
    return unique_locations

# getting  list of products by using user_id
def prod_user(join_df):
    product_user = join_df.groupBy('userid').agg(collect_set("product_id"))
    return product_user

# finding the total amount of spending by each user on each product
def total_spending_user(join_df):
    a = join_df.groupBy('userid', 'product_id').agg(sum('price'))
    return a



