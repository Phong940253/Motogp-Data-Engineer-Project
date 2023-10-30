import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, FloatType, IntegerType, TimestampType

import os
import sys

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

def create_keyspace(session):
    # create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS motogp
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
    logging.info("Keyspace created successfully")

def create_table(session):
    # create tables
    
    # Table: motogp.seasons
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.seasons (
            season_id UUID PRIMARY KEY,
            year int        );
    """)
    logging.info("Table created successfully")
    
    # Table: motogp.circuits
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.circuits (
            circuit_id UUID PRIMARY KEY,
            name text,
            legacy_id int,
            place text,
            country_iso text
        );
    """)
    
    # Table: motogp.events
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.events (
            event_id UUID PRIMARY KEY,
            season_id uuid,
            name text,
            sponsored_name text,
            date_start date,
            date_end date,
            test boolean,
            toad_api_uuid text,
            short_name text,
            circuit_id uuid,
            country_iso text
        );
    """)
    
    # Table: motogp.categories
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.categories (
            category_id UUID,
            name text PRIMARY KEY,
            legacy_id int
        );
    """)
    
    # Table: motogp.sessions
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.sessions (
            session_id UUID PRIMARY KEY,
            event_id UUID,
            date date,
            type text,
            category_id text,
            circuit_id UUID,
            condition map<text, text>
        );
    """)
    
    # Table: motogp.classifications
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.classifications (
            classification_id UUID PRIMARY KEY,
            session_id text,
            position int,
            rider_id text,
            team_id text,
            constructor_id text,
            average_speed float,
            total_laps int,
            time text,
            status text,
            points int
        );
    """)
    
    # Table: motogp.riders
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.riders (
            rider_id UUID PRIMARY KEY,
            full_name text,
            country_iso text,
            legacy_id int,
            number int,
            riders_api_uuid text
        );
    """)
    
    # Table: motogp.teams
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.teams (
            team_id UUID PRIMARY KEY,
            name text,
            legacy_id int,
        );
    """)
    
    # Table: motogp.constructors
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.constructors (
            constructor_id UUID PRIMARY KEY,
            name text,
            legacy_id int
        );
    """)
    
    # Table: motogp.countries
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.countries (
            country_iso text PRIMARY KEY,
            name text,
            region_iso text
        );
    """)

    # (['country', 'event_files', 'circuit', 'test', 'sponsored_name', 
    # 'date_end', 'toad_api_uuid', 'date_start', 'name', 'legacy_id', 'season', 'short_name', 'id'])



def cs_dfk_classification(spark_df):
    schema = StructType([
        StructField("classification_id", StringType()),
        StructField("session_id", StringType()),
        StructField("position", IntegerType()),
        StructField("rider_id", StringType()),
        StructField("team_id", StringType()),
        StructField("constructor_id", StringType()),
        StructField("total_laps", IntegerType()),
        StructField("status", StringType()),
        StructField("average_speed", FloatType()),
        StructField("time", StringType()),
        StructField("points", IntegerType())
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel
    
def cs_dfk_seasons(spark_df):
    schema = StructType([
        StructField("season_id", StringType()),
        StructField("year", StringType())    
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel
    
# Define a function to transform data into a specific schema
def cs_dfk_events(spark_df):
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("short_name", StringType()),
        StructField("season_id", StringType()),
        StructField("test", StringType()),
        StructField("sponsored_name", StringType()),
        StructField("toad_api_uuid", StringType()),
        StructField("date_start", TimestampType()),
        StructField("date_end", TimestampType()),
        StructField("name", StringType()),
        StructField("country_iso", StringType()),
        StructField("circuit_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_categories(spark_df):
    schema = StructType([
        StructField("category_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_riders(spark_df):
    schema = StructType([
        StructField("rider_id", StringType()),
        StructField("full_name", StringType()),
        StructField("country_iso", StringType()),
        StructField("legacy_id", IntegerType()),
        StructField("number", IntegerType()),
        StructField("riders_api_uuid", StringType()),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_teams(spark_df):
    schema = StructType([
        StructField("team_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", IntegerType()),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_constructors(spark_df):
    schema = StructType([
        StructField("constructor_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_circuits(spark_df):
    schema = StructType([
        StructField("circuit_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", IntegerType()),
        StructField("place", StringType()),
        StructField("country_iso", StringType()),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_countries(spark_df):
    schema = StructType([
        StructField("country_iso", StringType()),
        StructField("name", StringType()),
        StructField("region_iso", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

def cs_dfk_sessions(spark_df):
    schema = StructType([
        StructField("session_id", StringType()),
        StructField("date", TimestampType()),
        StructField("event_id", StringType()),
        StructField("circuit_id", StringType()),
        StructField("category", StringType()),
        StructField("type", StringType()),
        StructField("condition", MapType(StringType(), StringType()))
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel
    
def create_spark_connection():
    # create spark connection
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
            .appName("motogp") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
    except Exception as e:
        logging.error("Could not create the spark session due to exception: {}".format(e))
        
    return spark_conn

def connect_to_kafka(spark_conn, topics):
    # connect to kafka with spark connection
    spark_df = None
    try:
        spark_df = spark_conn.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topics) \
            .load()
        logging.info("Connected to kafka successfully")
    except Exception as e:
        logging.error("Could not connect to kafka due to exception: {}".format(e))
        
    return spark_df
    
    
def create_cassandra_connection():
    # create cassandra connection
    try:
        # connect to cassandra cluster
        cluster = Cluster(['localhost'])
        
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully")
        return cas_session
    except Exception as e:
        logging.error("Could not create the cassandra connection due to exception: {}".format(e))
        return None

def writeToCassandra(writeDF, epochId, table_name):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace="motogp") \
        .mode("append") \
        .option('checkpointLocation', '/tmp/checkpoint') \
        .save()

# Define a function to connect to a Kafka topic and process data
def process_kafka_topic(kafka_topic, cs_dfk, table):
    #create spark connection
    spark = create_spark_connection()
    
    if spark is not None:
    
        # Connect to Kafka
        spark_df = connect_to_kafka(spark, kafka_topic)
        selection_df = cs_dfk(spark_df)  

        streaming_query = (selection_df.writeStream \
                                .foreachBatch(lambda df, epoch: \
                                writeToCassandra(df, epoch, table)) \
                                .start())
            
        return streaming_query

if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
        
        query1 = process_kafka_topic( "season_topic", cs_dfk_seasons, "seasons")
        query2 = process_kafka_topic( "event_topic", cs_dfk_events, "events")
        query3 = process_kafka_topic( "category_topic", cs_dfk_categories, "categories")
        query4 = process_kafka_topic( "session_topic", cs_dfk_sessions, "sessions")
        query5 = process_kafka_topic( "classification_topic", cs_dfk_classification, "classifications")
        query6 = process_kafka_topic( "rider_topic", cs_dfk_riders, "riders")
        query7 = process_kafka_topic( "team_topic", cs_dfk_teams, "teams")
        query8 = process_kafka_topic( "constructor_topic", cs_dfk_constructors, "constructors")
        query9 = process_kafka_topic( "circuit_topic", cs_dfk_circuits, "circuits")
        query10 = process_kafka_topic( "country_topic", cs_dfk_countries, "countries")

        query1.awaitTermination()
        query2.awaitTermination()
        query3.awaitTermination()
        query4.awaitTermination()
        query5.awaitTermination()
        query6.awaitTermination()
        query7.awaitTermination()
        query8.awaitTermination()
        query9.awaitTermination()
        query10.awaitTermination()

