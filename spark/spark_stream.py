import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    # create keyspace
    
    
def create_table(session):
    # create table
    
    
    
def insert_data(session):
    # insert data
    
    
def create_spark_connection():
    # create spark connection
    try:
        s_conn = SparkSession.builder \
            .appName("motogp") \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1',
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .master("local[*]") \
            .getOrCreate()
    
    
def create_cassandra_connection():
    # create cassandra connection
    
    
if __name__ == "__main__":
    spark_conn = create_spark_connection()