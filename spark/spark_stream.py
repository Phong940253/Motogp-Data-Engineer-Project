import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    # create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS motogp
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
    
    
def create_table(session):
    # create table
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.results (
            id UUID PRIMARY KEY,
            season STRING,
            event STRING,
            category STRING,
            session STRING,
            timestamp TIMESTAMP,
            rider STRING 
            );
            
        """)
    
    
    
def insert_data(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    id = kwargs.get("id")
    season = kwargs.get("season")
    event = kwargs.get("event")
    category = kwargs.get("category")
    session = kwargs.get("session")
    timestamp = kwargs.get("timestamp")
    rider = kwargs.get("rider")
    
    try:
        session.execute("""
            INSERT INTO motogp.results (id, season, event, category, session, timestamp, rider)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (id, season, event, category, session, timestamp, rider))
        logging.info(f"Data inserted successfully for rider: {rider}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
    
    
def create_spark_connection():
    # create spark connection
    spark_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("motogp") \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1',
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .master("local[*]") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
    except Exception as e:
        logging.error("Could not create the spark session due to exception: {}".format(e))
        
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "results") \
            .load()
        logging.info("Connected to kafka datastream successfully")
        
    except Exception as e:
        logging.error("Could not connect to kafka datastream due to exception: {}".format(e))
        
    return spark_df
    
    
def create_cassandra_connection():
    # create cassandra connection
    try:
        # connect to cassandra cluster
        cluster = Cluster(['localhost'])
        
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error("Could not create the cassandra connection due to exception: {}".format(e))
        return None
    
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType()),
        StructField("season", StringType()),
        StructField("event", StringType()),
        StructField("category", StringType()),
        StructField("session", StringType()),
        StructField("timestamp", StringType()),
        StructField("rider", StringType())
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    print(sel)
    return sel
    
    
if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        #connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                            .option("checkpointLocation", "/tmp/checkpoint") \
                            .option("keyspace", "motogp") \
                            .option("table", "results") \
                            .start())
            
            streaming_query.awaitTermination()