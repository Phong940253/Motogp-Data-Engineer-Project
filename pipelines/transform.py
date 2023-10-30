from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, FloatType, IntegerType, TimestampType
import logging
import pandas as pd
import os
import sys
import ast
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def convert_all_string_to_object(df):
    """
    Convert all string representations of dictionaries to Python dictionary objects in a Pandas DataFrame.

    Parameters:
    - df: Pandas DataFrame

    Returns:
    - df: Pandas DataFrame with all columns containing string representations of dictionaries converted to Python dictionary objects
    """
    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = df[column].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else x)
            except (SyntaxError, ValueError):
                pass
    return df

# load csv
def load_csv(input_data):
    """
    Load csv file from input data path
    :param input_data: input data path
    :return: dataframe
    """
    

    df = pd.read_csv('dags/'+input_data)
    df = convert_all_string_to_object(df)
    return df

# session
def transform_sessions(df):

    # Handle missing values
    df['number'].fillna(0, inplace=True)
    df['condition'].fillna('N/A', inplace=True)
    df['type'].fillna('N/A', inplace=True)
    df['category_id'] = df['category'].fillna('N/A')
    df['event_id'] = df['event'].fillna('N/A')
    df['circuit_id'] = df['event'].fillna('N/A')

    # Get category_id from category column
    df['category_id'] = df['category_id'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # get event_id from event column
    df['event_id'] = df['event'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # get circuit_id from event column
    df['circuit_id'] = df['event'].apply(lambda x: x['circuit']['id'] if pd.notna(x) else x)

    # Convert the 'date' column to a datetime data type can convert to json for send kafka and fill na
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'].fillna(pd.to_datetime('1900-01-01'), inplace=True)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')


    # Drop unnecessary columns
    df = df[['id', 'date', 'event_id', 'circuit_id', 'category_id', 'type', 'condition']]
    df = df.rename(columns={'id': 'session_id'})

    df.drop_duplicates(subset=['session_id'], inplace=True)

    return df

def cs_dfk_session(spark, spark_df):
    schema = StructType([
        StructField("session_id", StringType()),
        StructField("date", TimestampType()),
        StructField("event_id", StringType()),
        StructField("circuit_id", StringType()),
        StructField("category", StringType()),
        StructField("type", StringType()),
        StructField("condition", MapType(StringType(), StringType()))
    ])
    spark_df = spark.createDataFrame(spark_df, schema)
    return spark_df

# event
def transform_event(df):
    # Handle missing values
    df['name'].fillna('N/A', inplace=True)
    df['date_end'].fillna('N/A', inplace=True)
    df['date_start'].fillna('N/A', inplace=True)
    df['toad_api_uuid'].fillna('N/A', inplace=True)
    df['sponsored_name'].fillna('N/A', inplace=True)
    df['test'].fillna('N/A', inplace=True)
    df['season'].fillna('N/A', inplace=True)
    df['short_name'].fillna('N/A', inplace=True)
    df['id'].fillna('N/A', inplace=True)
    df['country_iso'] = df['country'].fillna('N/A')
    df['circuit_id'] = df['circuit'].fillna('N/A')


    # Get season_id from season column
    df['season_id'] = df['season'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # Get country_id from country column
    df['country_iso'] = df['country'].apply(lambda x: x['iso'] if pd.notna(x) else x)
    # get circuit_id from circuit column
    df['circuit_id'] = df['circuit'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # Convert the 'date_start' and 'date_end' column to a datetime data type can convert to json for send kafka
    df['date_start'] = pd.to_datetime(df['date_start'], errors='coerce')
    df['date_end'] = pd.to_datetime(df['date_end'], errors='coerce')
    df['date_start'].fillna(pd.to_datetime('1900-01-01'), inplace=True)
    df['date_end'].fillna(pd.to_datetime('1900-01-01'), inplace=True)
    df['date_start'] = df['date_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['date_end'] = df['date_end'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Drop unnecessary columns
    df = df[['id', 'short_name', 'season_id', 'test', 'sponsored_name', 'toad_api_uuid', \
              'date_start', 'date_end', 'name', 'country_iso', 'circuit_id', \
            'country', 'circuit']]
    # Rename columns
    df = df.rename(columns={'id': 'event_id'})

    df.drop_duplicates(subset=['event_id'], inplace=True)

    return df

def cs_dfk_event(spark, spark_df):
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
    spark_df = spark.createDataFrame(spark_df, schema)
    return spark_df

def add_session_id_to_classification(df):
    updated_classification = []

    for index, row in df.iterrows():
        response = row['response']
        session_id = row['session_id']

        if not pd.isna(session_id) and not pd.isna(response):
            if 'classification' in response:
                classification_list = response['classification']
                for obj in classification_list:
                    obj['session_id'] = session_id
                updated_classification.extend(classification_list)

    return updated_classification

# classification
def transform_classification(df):
    # Handle missing values
    df = df.fillna({
        'id': 'N/A',
        'position': 0,
        'total_laps': 0,
        'status': 'N/A',
        'average_speed': 0,
        'time': 'N/A',
        'points': 0,
        'session_id': 'N/A',
        'rider': {},
        'team': {},
        'constructor': {}
    })

    # Get rider_id from rider column
    df['rider_id'] = df['rider'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # Get team_id from team column
    df['team_id'] = df['team'].apply(lambda x: x['id'] if pd.notna(x) else x)
    # Get constructor_id from constructor column
    df['constructor_id'] = df['constructor'].apply(lambda x: x['id'] if pd.notna(x) else x)

    # Convert to appropriate data types
    df['position'] = df['position'].astype(int)
    df['total_laps'] = df['total_laps'].astype(int)
    df['average_speed'] = df['average_speed'].astype(float)
    df['points'] = df['points'].astype(int)

    # Convert best_lap to string
    df['best_lap'] = df['best_lap'].astype(str)
    
    # Select and reorder columns
    df = df[['id', 'session_id', 'position', 'rider_id', 'team_id', 'constructor_id', \
              'total_laps', 'status', 'average_speed', 'time', 'points', \
                'rider', 'team', 'constructor']]
    # Rename the 'id' column to 'classification_id'
    df = df.rename(columns={'id': 'classification_id'})
    # Drop duplicates based on classification_id
    df = df.drop_duplicates(subset=['classification_id'])

    return df

def cs_dfk_classification(spark, pd_df):
    # Create the main schema
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
    
    try:
        pd_df = spark.createDataFrame(pd_df, schema=schema)
    except Exception as e:
        print('Could not create spark dataframe: ', e)
    
    return pd_df

# rider
def transform_riders(df):
    # Drop duplicates based on rider_id
    df = df.drop_duplicates(subset=['id'])
    # Handle missing values
    df = df.fillna({
        'number': 0,
        'riders_api_uuid': 'N/A',
    })

    # Extract ISO country code from the "country" column
    df['country_iso'] = df['country'].apply(lambda x: x.get('iso', 'N/A'))

    # convert to int
    df['number'] = df['number'].astype(int)
    df['legacy_id'] = df['legacy_id'].astype(int)

    # Select and reorder columns
    df = df[['id', 'full_name', 'country_iso', 'legacy_id', 'number', 'riders_api_uuid']]
    
    # Rename the 'id' column to 'rider_id'
    df = df.rename(columns={'id': 'rider_id'})

    
    return df

def cs_dfk_riders(spark, pd_df):
    # Create the main schema
    schema = StructType([
        StructField("rider_id", StringType()),
        StructField("full_name", StringType()),
        StructField("country_iso", StringType()),
        StructField("legacy_id", IntegerType()),
        StructField("number", IntegerType()),
        StructField("riders_api_uuid", StringType()),
    ])
    
    try:
        pd_df = spark.createDataFrame(pd_df, schema=schema)
    except Exception as e:
        print('Could not create Spark DataFrame: ', e)
    
    return pd_df

# team
def transform_teams(df):
    # Drop duplicates based on team_id
    df = df.drop_duplicates(subset=['id'])

    # Handle missing values
    df = df.fillna({
        'season': {},
    })

    # convert to int
    df['legacy_id'] = df['legacy_id'].astype(int)

    # Select and reorder columns
    df = df[['id', 'name', 'legacy_id']]
    
    # Rename the 'id' column to 'team_id'
    df = df.rename(columns={'id': 'team_id'})

    
    return df

def cs_dfk_teams(spark, pd_df):
    # Create the main schema
    schema = StructType([
        StructField("team_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", IntegerType()),
    ])
    
    try:
        pd_df = spark.createDataFrame(pd_df, schema=schema)
    except Exception as e:
        print('Could not create Spark DataFrame: ', e)
    
    return pd_df    

# circuit

def transform_circuits(df):
    # Drop duplicates based on circuit_id
    df = df.drop_duplicates(subset=['id'])
    # Handle missing values
    df = df.fillna({
        'nation': ''
    })

    # Select and reorder columns
    df = df[['id', 'name', 'legacy_id', 'place', 'nation']]
    # rename
    df = df.rename(columns={'id': 'circuit_id', 'nation': 'country_iso'})

    return df

def cs_dfk_circuits(spark, pd_df):
    # Create the main schema
    schema = StructType([
        StructField("circuit_id", StringType()),
        StructField("name", StringType()),
        StructField("legacy_id", IntegerType()),
        StructField("place", StringType()),
        StructField("country_iso", StringType()),
    ])
    
    try:
        pd_df = spark.createDataFrame(pd_df, schema=schema)
    except Exception as e:
        print('Could not create Spark DataFrame: ', e)
    
    return pd_df

# category
def transform_categories(df):
    import re
    # Drop duplicates based on category_id
    df.drop_duplicates(subset=['name'], inplace=True)

    # Rename columns
    df = df.rename(columns={"id": "category_id", "name": "name", "legacy_id": "legacy_id"})
    df['name'] = df['name'].apply(lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x))

    return df

# season
def transform_seasons(df):
    # Handle missing values (none to handle in this example)
        
    # Select and reorder columns
    df = df[['id', 'year']]
    # Rename columns
    df = df.rename(columns={"id": "season_id", "year": "year"})
    
    return df

# country
def transform_countries(df):
    # Drop duplicates based on country_id
    df = df.drop_duplicates(subset=['iso'])
    # Handle missing values
    df = df.fillna({
        'name': '',
        'iso': '',
        'region_iso': '',
    })

    # Select and reorder columns
    df = df[['iso', 'name', ]]
    # rename
    df = df.rename(columns={'iso': 'country_iso'})

    return df

# constructor
def transform_constructors(df):
    # Drop duplicates based on constructor_id
    df = df.drop_duplicates(subset=['id'])
    # Handle missing values
    df = df.fillna({
        'name': '',
        'legacy_id': 0,
    })

    # rename
    df = df.rename(columns={'id': 'constructor_id'})

    return df


def process(spark, input_data, table_name):
    """
    Process session data
    :param spark: spark session
    :param input_data: input data path
    :return: None
    """
    try:
        df = load_csv(input_data)
        df = transform_circuits(df)
        df = cs_dfk_circuits(spark,df)
        print(df.schema)
        print(df.show(5))
        session = create_cassandra_connection()
        create_keyspace(session)
        create_table(session)
        streaming_query = df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("confirm.truncate", "true") \
            .options(table=table_name, keyspace="motogp") \
            .save()
        logging.info("Session data processed successfully")
    except Exception as e:
        logging.error("Session data failed to process: ", e)


