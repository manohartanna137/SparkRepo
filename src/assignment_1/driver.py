from src.assignment_1.utils import *

spark=Spark_Session()
user_df = users_data(spark)
user_df.show()

trans_df=transc_data(spark)
trans_df.show()

join_df=join_dataframe(user_df,trans_df)
join_df.show()

df_unique_loc_=loc_unique(join_df)
df_unique_loc_.show()


user_product=prod_user(join_df)
user_product.show()

exp_user = total_spending_user(join_df)
exp_user.show()