
from src.assignment_2.utils import *

file_path ="C:/Users/hp/PycharmProjects/SparkRepo/resource/ghtorrent-logs.txt"
log_rdd = load_log_rdd(file_path)
print(log_rdd)

num_lines = num_of_lines(log_rdd)
print("Number of lines:", num_lines)

num_warnings = warning_messages(log_rdd)
print('Number of warnings:', num_warnings)
num_api_lines = process_repositories(log_rdd)
print("Number of api client lines:", num_api_lines)

num_api_lines = log_rdd.filter(lambda line: 'api_client' in line)

more_request = most_requests(num_api_lines)
print(more_request)

most_failed_requests_client = failed_requests(log_rdd)
print(most_failed_requests_client)

most_active_repo_count = active_repository(log_rdd)
print(most_active_repo_count)



