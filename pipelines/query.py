from cassandra.cluster import Cluster
import pandas as pd

# # Create a connection to your Cassandra cluster
# cluster = Cluster(['localhost'])
# session = cluster.connect("motogp")  # Connect to the "motogp" keyspace

# load csv
def load_csv(file_name):
    df = pd.read_csv('data/'+file_name)
    return df

def save_csv(df, file_name):
    df.to_csv('data/'+file_name, index=False)

# get results from rider, category, classification, season, event, session, team, circuit, country csv files
def load_and_join():
    rider_df = load_csv('rider.csv')
    category_df = load_csv('category.csv')
    classification_df = load_csv('classification.csv')
    season_df = load_csv('season.csv')
    event_df = load_csv('event.csv')
    session_df = load_csv('session.csv')
    team_df = load_csv('team.csv')
    circuit_df = load_csv('circuit.csv')
    country_df = load_csv('country.csv')
    constructor_df = load_csv('constructor.csv')

    # join season, event on season_id although is none
    season_event_df = season_df.merge(event_df, how='left', on='season_id')
    # drop circuit_id
    season_event_df = season_event_df.drop(columns=['circuit_id'])
    # join session, season_event on event_id
    session_season_event_df = session_df.merge(season_event_df, how='left', on='event_id', suffixes=('_session', '_event'))
   
    # join category, session_season_event on category_id
    category_session_season_event_df = session_season_event_df.merge(category_df, how='left', on='category_id', suffixes=('_session', '_category'))
    # join classification, category_session_season_event on session_id
    classification_category_session_season_event_df = classification_df.merge(category_session_season_event_df, how='left', on='session_id', suffixes=('_classification', '_category'))
    # join rider, classification_category_session_season_event on rider_id  
    rider_classification_category_session_season_event_df = rider_df.merge(classification_category_session_season_event_df, how='left', on='rider_id', suffixes=('_rider', '_classification'))
    # join team, rider_classification_category_session_season_event on team_id
    rider_classification_category_session_season_event_df = rider_classification_category_session_season_event_df.merge(team_df, how='left', on='team_id', suffixes=('_rider', '_team'))
    # join constructor, rider_classification_category_session_season_event on constructor_id
    rider_classification_category_session_season_event_df = rider_classification_category_session_season_event_df.merge(constructor_df, how='left', on='constructor_id', suffixes=('_team', '_constructor'))    
    #change column name
    rider_classification_category_session_season_event_df = rider_classification_category_session_season_event_df.rename(columns={'name':'name_event'})
    
    print(rider_classification_category_session_season_event_df[['year', 'name_category']]. \
          sort_values('year', ascending=False).head(10))
    print(rider_classification_category_session_season_event_df.columns)  

    return rider_classification_category_session_season_event_df
# Index(['rider_id', 'full_name', 'country_iso_rider', 'legacy_id_rider',
#        'number', 'riders_api_uuid', 'classification_id', 'session_id',
#        'position', 'team_id', 'constructor_id', 'total_laps', 'status',
#        'average_speed', 'time', 'points', 'rider', 'team', 'constructor',
#        'category_id', 'name_category', 'legacy_id_classification', 'date',
#        'event_id', 'circuit_id', 'type', 'condition', 'season_id', 'year',
#        'short_name', 'test', 'sponsored_name', 'toad_api_uuid', 'date_start',
#        'date_end', 'name_session', 'country_iso_classification', 'country',
#        'circuit', 'name_team', 'legacy_id_team', 'name_constructor',
#        'legacy_id_constructor'],
#       dtype='object')

# get result for each rider and each session with category is MotoGP and season is 2023
def rider_match(year=2023, category='MotoGP', type=['RAC', 'SPR'], df=pd.DataFrame()):
    df = df[(df['year'] == year) & (df['name_category'] == category) & (df['type'].isin(type))]
    # save to csv file rider_match_category_type_year.csv
    save_csv(df, 'rider_match_'+category.lower()+'_'+type[0].lower()+'_'+str(year)+'.csv')


df = load_and_join()
print("--------------------------------")
rider_match(2023, 'MotoGP', ['RAC', 'SPR'], df)
    