import unittest
from src.assignment_1.utils import *

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("PySpark test cases").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_join_dataframe(self):
        schema_user = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)
        ])
        data_user = [(201,"manohar@gmail.com","telugu","mumbai"),(202,"manohar@gmail.com","telugu","usa"),(203,"malvia@gmail.com","marathi","nagpur"),(204,"anonymous@outlook.com","tamil","chennai")]
        user_df = self.spark.createDataFrame(data=data_user, schema=schema_user)

        transc_schema = StructType([
            StructField("transaction_id", IntegerType(), True),\
            StructField("product_id", IntegerType(), True),\
            StructField("userid", IntegerType(), True),\
            StructField("price", IntegerType(), True),\
            StructField("product_description", StringType(), True)
        ])
        data_transaction= [(3301, 10001, 201, 800, "mouse"),\
                            (3302, 10002, 202, 900, "laptop"),\
                            (3303, 10003, 203, 80000, "tv"),\
                            (3304, 10004, 201, 55000, "fridge")]
        transc_df = self.spark.createDataFrame(data=data_transaction, schema=transc_schema)

        expected_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)])

        expect_data = [(3301,10001,201,800,"mouse",201,"manohar@gmail.com","telugu","mumbai"),
                         (3304,10004,201,55000,"fridge",201,"manohar@gmail.com","telugu","mumbai"),
                         (3302,10002,202,900,"laptop",202,"manohar@gmail.com","telugu","usa"),
                         (3303,10003,203,80000,"tv",203,"malvia@gmail.com","marathi","nagpur")]

        expect_df = self.spark.createDataFrame(data=expect_data, schema=expected_schema)
        #user_df,transc_df="user_df","transc_df"
        user_id = "user_id"
        userid = "userid"
        join_df = join_dataframe(user_df, transc_df, user_id, userid, "inner")


        self.assertEqual((join_df.collect()),(expect_df.collect()))
    def test_unique_locations(self):
        actual_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)])

        actual_data = [(3301, 10001, 201, 800, "mouse", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3304, 10004, 201, 55000, "fridge", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3302, 10002, 202, 900, "laptop", 202, "manohar@gmail.com", "telugu", "usa"),
                       (3303, 10003, 203, 80000, "tv", 203, "malvia@gmail.com", "marathi", "nagpur")]

        df= self.spark.createDataFrame(data=actual_data, schema=actual_schema)
        schema1 = StructType([
            StructField("product_description", StringType(), True),
            StructField("location ", StringType(), True),
            StructField("count(location )", LongType(), True)
        ])
        data1 = [("tv","nagpur",1),
                          ("mouse","mumbai",1),
                          ("laptop","usa",1),
                          ("fridge","mumbai",1)]
        expected_df = self.spark.createDataFrame(data=data1, schema=schema1)
        actual_df = loc_unique(df)
        self.assertEqual(actual_df.collect(),expected_df.collect())

    def test_prod_user(self):
        actual_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)])

        actual_data = [(3301, 10001, 201, 800, "mouse", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3304, 10004, 201, 55000, "fridge", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3302, 10002, 202, 900, "laptop", 202, "manohar@gmail.com", "telugu", "usa"),
                       (3303, 10003, 203, 80000, "tv", 203, "malvia@gmail.com", "marathi", "nagpur")]

        df = self.spark.createDataFrame(data=actual_data, schema=actual_schema)


        expected_schema = StructType([
            StructField("userid", IntegerType(), True),
            StructField("new_count", LongType(), True)
        ])
        actual_df = prod_user(df)
        expected_data = [(101, 2), (102, 1), (103, 1)]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(expected_df.collect(), expected_df.collect())


    def test_total_spending_user(self):
        actual_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)])

        actual_data = [(3301, 10001, 201, 800, "mouse", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3304, 10004, 201, 55000, "fridge", 201, "manohar@gmail.com", "telugu", "mumbai"),
                       (3302, 10002, 202, 900, "laptop", 202, "manohar@gmail.com", "telugu", "usa"),
                       (3303, 10003, 203, 80000, "tv", 203, "malvia@gmail.com", "marathi", "nagpur")]

        df = self.spark.createDataFrame(data=actual_data, schema=actual_schema)
        spending_df = total_spending_user(df)

        expected_schema = StructType([
            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("Total_Amount", LongType(), True)
        ])
        expected_data = [(101, "fridge", 35000), (101, "mouse", 700), (102, "laptop", 900), (103, "tv", 34000)]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        spending_df = total_spending_user(df)
        self.assertEqual(expected_df.collect(), expected_df.collect())

