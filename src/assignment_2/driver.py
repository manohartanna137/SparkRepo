
from src.assignment_2.utils import *

file_path = "../../resource/ghtorrent-logs.txt"

log_rdd = load_rdd(file_path)
print(log_rdd)

line_count = count_lines(log_rdd)
print("Number of lines:", line_count)

warning_count = count_warning_messages(log_rdd)
print('Number of warnings:', warning_count)

api_client_lines = count_processed_repositories(log_rdd)
print("Number of api client lines:", api_client_lines)

api_client_lines = log_rdd.filter(lambda line: 'api_client' in line)
most_http_requests_client = client_with_most_http_requests(api_client_lines)
print(most_http_requests_client)

most_failed_requests_client = client_with_most_failed_requests(log_rdd)
print(most_failed_requests_client)

most_active_repo_count = count_most_active_repository(log_rdd)
print(most_active_repo_count)



