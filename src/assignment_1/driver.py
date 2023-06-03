from src.assignment_1.utils import *

spark=Spark_Session()
user_df=user_dataframe(spark)
print("transaction table")
user_df.show()

trans_df=transaction_dataframe(spark)
print("transaction table")
trans_df.show()

join_df=join_dataframe(user_df,trans_df)
join_df.show()

unique_location(join_df)
print("unique contries that brought products")
unique_location.show()

prod_bought_user=user_prod(join_df)
print("products bought by each user")
prod_bought_user.show()

exp_user_prod=total_spend(join_df)
print("total spending done by each user on each product")
exp_user_prod.show()