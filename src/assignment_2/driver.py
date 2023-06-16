
from src.assignment_2.utils import *

spark=spark_session()

filepath= "../../resource/ghtorrent-logs.txt"
rdd=creating_rdd(spark,filepath)


num_lines=num_of_lines(rdd)


df=reading_file(spark,filepath)

df_torrent = create_dataframe_from_rdd(df)

df_warn=warn_messages(df_torrent)

df_api_clients=api_clients(df_torrent)


df_most_http_requests=most_http_requests(df_torrent)
df_most_http_requests.show()
#
df_most_failed_requests=most_failed_requests(df_torrent)
df_most_failed_requests.show()
#
df_most_active_hours=most_active_hours(df_torrent)
df_most_active_hours.show()
#
df_most_active_repositories=most_active_repositories(df_torrent)
df_most_active_repositories.show()






