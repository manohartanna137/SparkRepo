
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
sc = SparkContext("local", "Assignment2")
def load_log_rdd(file_path):
    log_rdd = sc.textFile(file_path)
    return log_rdd


def num_of_lines(log_rdd):
    count_ = log_rdd.count()
    return count_

def warning_messages(log_rdd):
    count_warn = log_rdd.filter(lambda x: x.split(',')[0] == 'WARN').count()
    return count_warn


def process_repositories(log_rdd):
    api_client = log_rdd.filter(lambda x: 'api_client' in x)
    api_client_lines_count = api_client.count()
    return api_client_lines_count


def most_requests(num_of_lines):
    client = num_of_lines.map(lambda xy: (xy.split('--')[1].strip(), 1)).reduceByKey(lambda a, b: a + b).max(key=lambda x: x[1])
    return client[0]


def failed_requests(log_rdd):
    rdd = log_rdd.filter(lambda xy : 'Failed' in xy).map(lambda line: (line.split('--')[1].strip(), 1)).reduceByKey(lambda a, b: a + b) .max(key=lambda x: x[1])
    return rdd[0]


def active_repository(log_rdd):
    active_repo = log_rdd.filter(lambda x: 'ghtorrent.rb' in x).count()
    return active_repo






