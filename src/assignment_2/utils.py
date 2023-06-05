
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

sc = SparkContext("local", "Spark Assignment2")
# creating a function to load the file to a rdd
def load_rdd(file_path):
    log_rdd = sc.textFile(file_path)
    return log_rdd


# creating a function to count the number of lines in the RDD.
def count_lines(log_rdd):
    line_count = log_rdd.count()
    return line_count


# creating a function to count the number of warning messages.
def count_warning_messages(log_rdd):
    warning_count = log_rdd.filter(lambda line: line.split(',')[0] == 'WARN').count()
    return warning_count


# creating a function to count the repositories which were processed in total.
def count_processed_repositories(log_rdd):
    api_client_lines = log_rdd.filter(lambda line: 'api_client' in line)
    api_client_lines_count = api_client_lines.count()
    return api_client_lines_count


# creating a function to display which client did most HTTP requests.
def client_with_most_http_requests(api_client_lines):
    most_requests_client = api_client_lines.map(lambda line: (line.split('--')[1].strip(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .max(key=lambda x: x[1])
    return most_requests_client[0]


# creating a function to display which client did most FAILED HTTP requests.
def client_with_most_failed_requests(log_rdd):
    failed_rdd = log_rdd.filter(lambda line: 'Failed' in line).map(lambda line: (line.split('--')[1].strip(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .max(key=lambda x: x[1])
    return failed_rdd[0]


# creating a function to display the most active repo.
def count_most_active_repository(log_rdd):
    most_active_repo = log_rdd.filter(lambda x: 'ghtorrent.rb' in x).count()
    return most_active_repo
