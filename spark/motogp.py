import logging

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

        CREATE TABLE IF NOT EXISTS motogp.seasons (
            season_id UUID PRIMARY KEY,
            year int,
            current boolean
        );

        CREATE TABLE IF NOT EXISTS motogp.circuits (
            circuit_id UUID PRIMARY KEY,
            name text,
            legacy_id int,
            place text,
            nation text,
            country_iso text
        );

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

        CREATE TABLE IF NOT EXISTS motogp.categories (
            category_id UUID PRIMARY KEY,
            name text,
            legacy_id int
        );

        CREATE TABLE IF NOT EXISTS motogp.sessions (
            session_id UUID PRIMARY KEY,
            event_id UUID,
            date date,
            type text,
            category_id UUID,
            circuit_id UUID,
            conditions map<text, text>,
        );

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
            status text
        );

        CREATE TABLE IF NOT EXISTS motogp.riders (
            rider_id UUID PRIMARY KEY,
            full_name text,
            country_iso text,
            legacy_id int,
            number int,
            riders_api_uuid UUID
        );

        CREATE TABLE IF NOT EXISTS motogp.teams (
            team_id UUID PRIMARY KEY,
            name text,
            legacy_id int,
            season_id UUID
        );

        CREATE TABLE IF NOT EXISTS motogp.constructors (
            constructor_id UUID PRIMARY KEY,
            name text,
            legacy_id int
        );

        CREATE TABLE IF NOT EXISTS motogp.countries (
            country_iso text PRIMARY KEY,
            country_name text,
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
    
