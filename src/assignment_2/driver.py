from src.assignment_2.utils import *
file_path = "../../resource/ghtorrent-logs.txt"

print('hello')

log_rdd = load_log_rdd(file_path)
print(log_rdd)

line_count = count_lines(log_rdd)
print("Number of lines:", line_count)


warning_count = count_warning_messages(log_rdd)
print('Number of warnings:',warning_count)


api_client_lines = count_processed_repositories(log_rdd)
print("Number of api client lines:",api_client_lines_count)


most_requests_client = client_with_most_requests(api_client_lines)
print("Client with most failed requests:", failed_rdd[0])

most_failed_requests_client = client_with_most_failed_requests(log_rdd)
print("client_with_most_requests:",failed_rdd[0])


most_active_repo_count = count_most_active_repository(log_rdd)
print("count of gh torrent-rb:", most_active_repo)
print(most_active_repo_count)


