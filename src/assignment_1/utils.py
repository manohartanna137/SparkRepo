from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#creating a spark Session
def Spark_Session():
    spark=SparkSession.builder.appName("Spark Assigment 1").getOrCreate()
    return spark
#creating a data frame which has conetnts of user.csv file
def user_dataframe(spark):
    user_df=spark.read.csv("C:/Users/hp/PycharmProjects/SparkRepo/resource/user.csv",header="true",inferSchema="true")
    return user_df

#creating a data frame which has conetnts of transaction.csv file
def transaction_dataframe(spark):
    transaction_df=spark.read.csv("C:/Users/hp/PycharmProjects/SparkRepo/resource/transaction.csv",header="true",inferSchema="true")
    return transaction_df

# joining the transaction and user data frames using inner join
def join_dataframe(user_df,transaction_df):
    join_df=transaction_df.join(user_df,user_df.user_id ==  transaction_df.userid,"inner")
    return join_df


def unique_location(join_df):
    unique_loca =  join_df.groupBy('location', 'product_description').agg(countDistinct('location'))
    return unique_loca

def user_prod(join_df):
    product_=join_df.groupBy('userid').agg(collect_set("product_id").alias("Bought_Products")).show()
    return product_

#finding the total amount of spending by each user on each product
def total_spend(join_df):
    total=join_df.groupBy('userid', 'product_id').agg(sum('price')).show()
    return total









