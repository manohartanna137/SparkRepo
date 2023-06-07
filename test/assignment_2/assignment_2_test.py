import unittest
from pyspark import SparkContext
from src.assignment_2.utils import *


class Test(unittest.TestCase):

    def setUp(self):
        self.file_path = "C:/Users/hp/PycharmProjects/SparkRepo/resource/ghtorrent-logs.txt"
        self.log_rdd =  load_log_rdd(self.file_path)
        self.api_client_lines = self.log_rdd.filter(lambda x: 'api_client' in x)

    def test_num_of_lines(self):
        num_lines = num_of_lines(self.log_rdd)
        self.assertEqual(num_lines, 281234)
    def test_warning_messages(self):
        warning_count = warning_messages(self.log_rdd)
        self.assertEqual(warning_count, 3811)

    def test_processed_repositories(self):
        processed_count = process_repositories(self.log_rdd)
        self.assertEqual(processed_count, 37596)

    def test_most_requests(self):
        requests_client = most_requests(self.api_client_lines)
        self.assertEqual(requests_client, "api_client.rb: Unauthorised request with token: 46f11b5791b7db9077f4d9a9ab27f93e89dccad4")

    def test_failed_requests(self):
        most_failed_requests_client =failed_requests(self.log_rdd)
        self.assertEqual(most_failed_requests_client,"api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1749")

    def test_active_repository(self):
        active_repo_= active_repository(self.log_rdd)
        self.assertEqual(active_repo_,166397)


