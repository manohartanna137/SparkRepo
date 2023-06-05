from src.assignment_1.utils import *

spark=Spark_Session()
user_df=user_dataframe(spark)
print("transaction table")
user_df.show()

trans_df=transaction_dataframe(spark)
print("transaction table")
trans_df.show()

join_df=join_dataframe(user_df,trans_df)
print("Inner join of two dataframes")
join_df.show()

unique_location_df=unique_location(join_df)
print("unique locations that brought products")
unique_location_df.show()


prod_bought_user=user_products(join_df)
print("products bought by each user")
prod_bought_user.show()
#
exp_user_prod=total_spending(join_df)
print("total spending done by each user on each product")
exp_user_prod.show()