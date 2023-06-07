import unittest

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from src.assignment_1.utils import *

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("PySpark test cases").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_unique_location(self):
        schema_user = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)
        ])
        data_user = [(101, "abc.123 @ gmail.com", "hindi", "mumbai"),
                     (102, "abc.123 @ gmail.com", "hindi", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88 @ outlook.com", "tamil", "chennai")
                     ]
        user_df = self.spark.createDataFrame(data=data_user, schema=schema_user)

        transc_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])
        data_transaction= [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "laptop"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge")

                            ]
        transc_df = self.spark.createDataFrame(data=data_transaction, schema=transc_schema)

        expected_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)

        ])

        expect_data = [(101, "abc.123 @ gmail.com", "hindi", "mumbai", 3300104, 1000004, 101, 35000, "fridge"),
                         (101, "abc.123 @ gmail.com", "hindi", "mumbai", 3300101, 1000001, 101, 700, "mouse"),
                         (102, "abc.123 @ gmail.com", "hindi", "usa", 3300102, 1000002, 102, 900, "laptop"),
                         (103, "madan.44@gmail.com", "marathi", "nagpur", 3300103, 1000003, 103, 34000, "tv")]

        expect_df = self.spark.createDataFrame(data=expect_data, schema=expected_schema)
        join_df = join_dataframe(user_df, transc_df)
        join_df.show()

        schema1 = StructType([
            StructField("product_description", StringType(), True),
            StructField("location ", StringType(), True),
            StructField("count(location )", LongType(), True)
        ])
        data1 = [("tv", "nagpur", 1),
                          ("mouse", "mumbai", 1),
                          ("laptop", "usa", 1),
                          ("fridge", "mumbai", 1)]
        expected_df1 = self.spark.createDataFrame(data=data1, schema=schema1)
        loc_df = loc_unique(join_df)
        loc_df.show()

        schema2 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("new_count", LongType(), True)
        ])
        data2 = [(101, 2), (102, 1), (103, 1)]
        expected_df2 = self.spark.createDataFrame(data=data2, schema=schema2)
        product_df2 =prod_user(join_df)
        product_df2.show()


        schema3 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("Total_Amount", LongType(), True)
        ])
        data3 = [(101, "fridge", 35000), (101, "mouse", 700), (102, "laptop", 900), (103, "tv", 34000)]
        expected_df = self.spark.createDataFrame(data=data3, schema=schema3)
        spending_df = total_spending_user(join_df)
        spending_df.show()

