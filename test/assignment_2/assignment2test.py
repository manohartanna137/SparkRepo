import unittest
from pyspark import SparkContext
from src.assignment_2.utils import *


class GHTorrentLogAnalysisTest(unittest.TestCase):

    def setUp(self):
        self.file_path = "../../resource/ghtorrent-logs.txt"
        self.log_rdd = load_log_rdd(self.file_path)
        self.api_client_lines = self.log_rdd.filter(lambda line: 'api_client' in line)

    def test_count_lines(self):
        line_count = count_lines(self.log_rdd)
        self.assertEqual(line_count, 281234)
    def test_count_warning_messages(self):
        warning_count = count_warning_messages(self.log_rdd)
        self.assertEqual(warning_count, 3811)

    def test_count_processed_repositories(self):
        processed_repositories_count = count_processed_repositories(self.log_rdd)
        self.assertEqual(processed_repositories_count, 37596)

    def test_client_with_most_requests(self):
        most_requests_client = client_with_most_requests(self.api_client_lines)
        self.assertEqual(most_requests_client, "api_client.rb: Unauthorised request with token: 46f11b5791b7db9077f4d9a9ab27f93e89dccad4")

    def test_client_with_most_failed_requests(self):
        most_failed_requests_client = client_with_most_failed_requests(self.log_rdd)
        self.assertEqual(most_failed_requests_client,"api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1749")

    def test_count_most_active_repository(self):
        most_active_repo_count = count_most_active_repository(self.log_rdd)
        self.assertEqual(most_active_repo_count,166397)


