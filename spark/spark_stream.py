import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, DecimalType, IntegerType

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

def create_table(session):
    # create tables
    
    # Table: motogp.seasons
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.seasons (
            season_id UUID PRIMARY KEY,
            year int        );
    """)
    
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
            season_id UUID,
            name text,
            sponsored_name text,
            date_start date,
            date_end date,
            test boolean,
            toad_api_uuid UUID,
            short_name text,
            legacy_ids list<text>,
            circuit_id UUID,
            country_iso text
        );
    """)
    
    # Table: motogp.categories
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.categories (
            category_id UUID PRIMARY KEY,
            name text,
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
            category_id UUID,
            circuit_id UUID,
            conditions map<text, text>
        );
    """)
    
    # Table: motogp.classifications
    session.execute("""
        CREATE TABLE IF NOT EXISTS motogp.classifications (
            classification_id UUID PRIMARY KEY,
            session_id UUID,
            position int,
            rider_id UUID,
            team_id UUID,
            constructor_id UUID,
            average_speed decimal,
            gap map<text, text>,
            total_laps int,
            time text,
            points int,
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
            riders_api_uuid UUID
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

def insert_data_to_seasons(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    id = kwargs.get("id")
    year = kwargs.get("year")
    
    try:
        session.execute("""
            INSERT INTO motogp.season (id, year)
                VALUES (%s, %s)
                IF NOT EXISTS
            """, (id, year))
        logging.info(f"Data inserted successfully for season: {year}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_events(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    season_id = kwargs.get("id")
    name = kwargs.get("name")
    sponsored_name = kwargs.get("sponsored_name")
    date_start = kwargs.get("date_start")
    date_end = kwargs.get("date_end")
    test = kwargs.get("test")
    toad_api_uuid = kwargs.get("toad_api_uuid")
    short_name = kwargs.get("short_name")
    circuit_id = kwargs.get("circuit_id")
    country_iso = kwargs.get("country_iso")
    
    try:
        session.execute("""
            INSERT INTO motogp.event (season_id, name, sponsored_name, date_start, date_end, test, toad_api_uuid, short_name, circuit_id, country_iso)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                IF NOT EXISTS
            """, (season_id, name, sponsored_name, date_start, date_end, test, toad_api_uuid, short_name, circuit_id, country_iso))
        logging.info(f"Data inserted successfully for event: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_circuits(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    circuit_id = kwargs.get("id")
    name = kwargs.get("name")
    legacy_id = kwargs.get("legacy_id")
    place = kwargs.get("place")
    nation = kwargs.get("nation")
    country_iso = kwargs.get("country_iso")
    
    try:
        session.execute("""
            INSERT INTO motogp.circuit (circuit_id, name, legacy_id, place, nation, country_iso)
                VALUES (%s, %s, %s, %s, %s, %s)
                IF NOT EXISTS
            """, (circuit_id, name, legacy_id, place, nation, country_iso))
        logging.info(f"Data inserted successfully for circuit: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_categories(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    category_id = kwargs.get("id")
    name = kwargs.get("name")
    legacy_id = kwargs.get("legacy_id")
    
    try:
        session.execute("""
            INSERT INTO motogp.category (category_id, name, legacy_id)
                VALUES (%s, %s, %s)
                IF NOT EXISTS
            """, (category_id, name, legacy_id))
        logging.info(f"Data inserted successfully for category: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_sessions(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    session_id = kwargs.get("id")
    event_id = kwargs.get("event_id")
    category_id = kwargs.get("category_id")
    name = kwargs.get("name")
    legacy_id = kwargs.get("legacy_id")
    
    try:
        session.execute("""
            INSERT INTO motogp.session (session_id, event_id, category_id, name, legacy_id)
                VALUES (%s, %s, %s, %s, %s)
                IF NOT EXISTS
            """, (session_id, event_id, category_id, name, legacy_id))
        logging.info(f"Data inserted successfully for session: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_classifications(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    classification_id = kwargs.get("id")
    session_id = kwargs.get("session_id")
    position = kwargs.get("position")
    rider_id = kwargs.get("rider_id")
    team_id = kwargs.get("team_id")
    constructor_id = kwargs.get("constructor_id")
    average_speed = kwargs.get("average_speed")
    gap = kwargs.get("gap")
    total_laps = kwargs.get("total_laps")
    time = kwargs.get("time")
    points = kwargs.get("points")
    status = kwargs.get("status")
    
    try:
        session.execute("""
            INSERT INTO motogp.classification (classification_id, session_id, position, rider_id, team_id, constructor_id, average_speed, gap, total_laps, time, points, status)
            """, (classification_id, session_id, position, rider_id, team_id, constructor_id, average_speed, gap, total_laps, time, points, status))
        logging.info(f"Data inserted successfully for classification: {classification_id}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_riders(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    rider_id = kwargs.get("id")
    full_name = kwargs.get("full_name")
    country_iso = kwargs.get("country_iso")
    legacy_id = kwargs.get("legacy_id")
    number = kwargs.get("number")
    
    try:
        session.execute("""
            INSERT INTO motogp.rider (rider_id, full_name, country_iso, legacy_id, number)
                VALUES (%s, %s, %s, %s, %s)
                IF NOT EXISTS
            """, (rider_id, full_name, country_iso, legacy_id, number))
        logging.info(f"Data inserted successfully for rider: {full_name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
    
def insert_data_to_teams(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    team_id = kwargs.get("id")
    name = kwargs.get("name")
    legacy_id = kwargs.get("legacy_id")
    season_id = kwargs.get("season_id")
    
    try:
        session.execute("""
            INSERT INTO motogp.team (team_id, name, legacy_id, season_id)
                VALUES (%s, %s, %s, %s)
                IF NOT EXISTS
            """, (team_id, name, legacy_id, season_id))
        logging.info(f"Data inserted successfully for team: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_constructors(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    constructor_id = kwargs.get("id")
    name = kwargs.get("name")
    legacy_id = kwargs.get("legacy_id")
    
    try:
        session.execute("""
            INSERT INTO motogp.constructor (constructor_id, name, legacy_id)
                VALUES (%s, %s, %s)
            """, (constructor_id, name, legacy_id))
        logging.info(f"Data inserted successfully for constructor: {name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
        
def insert_data_to_countries(session, **kwargs):
    # insert data
    print("Inserting data...")
    
    country_iso = kwargs.get("country_iso")
    country_name = kwargs.get("country_name")
    region_iso = kwargs.get("region_iso")
    
    try:
        session.execute("""
            INSERT INTO motogp.country (country_iso, country_name, region_iso)
                VALUES (%s, %s, %s)
            """, (country_iso, country_name, region_iso))
        logging.info(f"Data inserted successfully for country: {country_name}")
        
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")
    
    

def cs_dfk_classification(spark_df):
    schema = StructType([
        StructField("classification_id", StringType()),
        StructField("session_id", StringType()),
        StructField("position", StringType()),
        StructField("rider_id", StringType()),
        StructField("team_id", StringType()),
        StructField("constructor_id", StringType()),
        StructField("average_speed", DecimalType()),
        StructField("gap", MapType(StringType(), StringType())),
        StructField("total_laps", StringType()),
        StructField("time", StringType()),
        StructField("points", IntegerType()),
        
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
        StructField("name", StringType()),
        StructField("country_iso", StringType()),
        StructField("circuit_id", StringType()),
        StructField("date_start", StringType()),
        StructField("date_end", StringType()),
        StructField("season_id", StringType()),
        StructField("sponsored_name", StringType()),
        StructField("test", StringType()),
        StructField("toad_api_uuid", StringType()),
        StructField("short_name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_categories(spark_df):
    schema = StructType([
        StructField("category_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_riders(spark_df):
    schema = StructType([
        StructField("rider_id", StringType()),
        StructField("full_name", StringType()),
        StructField("country_iso", StringType()),
        StructField("legacy_id", StringType()),
        StructField("number", StringType()),
        StructField("riders_api_uuid", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_teams(spark_df):
    schema = StructType([
        StructField("team_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_constructors(spark_df):
    schema = StructType([
        StructField("constructor_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_circuits(spark_df):
    schema = StructType([
        StructField("circuit_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", StringType()),
        StructField("place", StringType()),
        StructField("country_iso", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

# Define a function to transform data into a specific schema
def cs_dfk_countries(spark_df):
    schema = StructType([
        StructField("country_iso", StringType()),
        StructField("name", StringType()),
        StructField("region_iso", StringType())
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
                 .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return sel

def cs_dfk_sessions(spark_df):
    schema = StructType([
        StructField("session_id", StringType()),
        StructField("event_id", StringType()),
        StructField("date", StringType()),
        StructField("type", StringType()),
        StructField("category_id", StringType()),
        StructField("circuit_id", StringType()),
        StructField("conditions", MapType(StringType(), StringType()))
    ])

    
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
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("subscribe", topics) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
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
        return cas_session
    except Exception as e:
        logging.error("Could not create the cassandra connection due to exception: {}".format(e))
        return None

# Define a function to connect to a Kafka topic and process data
def process_kafka_topic(spark, kafka_topic, cs_dfk, table):
    # Connect to Kafka
    spark_df = connect_to_kafka(spark, kafka_topic)
    selection_df = cs_dfk(spark_df)  

    streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'motogp')
                               .option('table', table)
                               .start())
        
    streaming_query.awaitTermination()

if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
        
        process_kafka_topic(spark_conn, "season_topic", cs_dfk_seasons, "seasons")
        process_kafka_topic(spark_conn, "event_topic", cs_dfk_events, "events")
        process_kafka_topic(spark_conn, "category_topic", cs_dfk_categories, "categories")
        process_kafka_topic(spark_conn, "session_topic", cs_dfk_sessions, "sessions")
        process_kafka_topic(spark_conn, "classification_topic", cs_dfk_classification, "classifications")
        process_kafka_topic(spark_conn, "rider_topic", cs_dfk_riders, "riders")
        process_kafka_topic(spark_conn, "team_topic", cs_dfk_teams, "teams")
        process_kafka_topic(spark_conn, "constructor_topic", cs_dfk_constructors, "constructors")
        process_kafka_topic(spark_conn, "circuit_topic", cs_dfk_circuits, "circuits")
        process_kafka_topic(spark_conn, "country_topic", cs_dfk_countries, "countries")
