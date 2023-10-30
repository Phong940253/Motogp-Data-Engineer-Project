from kafka import KafkaProducer
import time
import pandas as pd
import re
import aiohttp
import asyncio
import time
import json
import logging
from pipelines.transform import transform_categories, transform_classification, transform_circuits, transform_event, transform_riders, transform_seasons, transform_sessions, transform_teams, transform_constructors, transform_countries

async def get_request(session_http, semaphore, url, params=None, more_data=None):
    async with semaphore:
        async with session_http.get(url, params=params) as res:
            if res.status == 200:
                json_response = await res.json()
                if more_data is not None and isinstance(more_data, dict):
                    json_response = {'response': json_response, **more_data}
                return json_response
            else:
                return {"status_code": res.status, "message": res.text }
            


# https://api.motogp.pulselive.com/motogp/v1/results/seasons
# return all seasons
async def get_motogp_all_season_api(session_http, semaphore):
    return await get_request(
        session_http,
        semaphore,
        "https://api.motogp.pulselive.com/motogp/v1/results/seasons",
    )


# https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid=db8dc197-c7b2-4c1b-b3a4-6dc534c023ef&isFinished=true
# return list event of season
async def get_motogp_event_api(session_http, semaphore, season_uuid):
    return await get_request(
        session_http,
        semaphore,
        "https://api.motogp.pulselive.com/motogp/v1/results/events",
        params={"seasonUuid": season_uuid, "isFinished": "true"},
    )

    # [
    #     {
    #         name: "Grand Prix of Qatar",
    #         id: event_uuid,
    #         ...
    #     }
    # ]

# https://api.motogp.pulselive.com/motogp/v1/results/categories?eventUuid=bfd8a08c-cbb4-413a-a210-6d34774ea4c5
# return list category of event
async def get_motogp_category_api(session_http, semaphore, event_uuid):
    return await get_request(
        session_http,
        semaphore,
        "https://api.motogp.pulselive.com/motogp/v1/results/categories",
        params={"eventUuid": event_uuid},
    )

    # [
    #     {
    #         name: "MotoGP",
    #         id: category_uuid,
    #         legacy_id: int
    #     }
    # ]


# return list session of event
async def get_motogp_session_api(session_http, semaphore, event_uuid, category_uuid):
    return await get_request(
        session_http,
        semaphore,
        "https://api.motogp.pulselive.com/motogp/v1/results/sessions",
        params={"eventUuid": event_uuid, "categoryUuid": category_uuid},
    )

    # [
    #     {
    #         date: datetime,
    #         number: int,
    #         id: session_id,
    #         ...
    #     }
    # ]


# return result of session
async def get_motogp_result_session_api(session_http, semaphore, session, more_data):
    return await get_request(
        session_http,
        semaphore,
        f"https://api.motogp.pulselive.com/motogp/v1/results/session/{session}/classification",
        params={"test": "false"},
        more_data=more_data
    )


# print(get_motogp_api("344e3645-b719-4709-8d88-698b128b1720"))
# print(get_motogp_session_api("bfd8a08c-cbb4-413a-a210-6d34774ea4c5", "e8c110ad-64aa-4e8e-8a86-f2f152f6a942"))
# print(get_motogp_all_season_api())



def save_csv(data, file_name):
    df = pd.DataFrame(data)
    print(df.head(20))
    df.to_csv('/opt/airflow/data/'+file_name, index=False)

def convert_df_to_list(df):
    return df.to_dict('records')

def transform(data, transform_func):
    df = pd.DataFrame(data)
    transformed_df = transform_func(df)
    return convert_df_to_list(transformed_df)

async def process_season(**kwargs):
    import os
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session_http:
        list_season = await get_motogp_all_season_api(session_http, semaphone)
        list_season = transform(list_season, transform_seasons)
        save_csv(list_season, "season.csv")
        for season in list_season:
            producer.send("season_topic", json.dumps(season).encode("utf-8"))
            
        kwargs["ti"].xcom_push(key="list_season", value=list_season)

async def process_event(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_season = kwargs["ti"].xcom_pull(key="list_season", task_ids="get_season_task")
        tasks = [get_motogp_event_api(session_http, semaphone, season["season_id"]) for season in list_season]
        list_event = await asyncio.gather(*tasks)
        list_event = [event for task in list_event for event in task]

        #list country, circuit
        list_country = []
        list_circuit = []

        list_event = transform(list_event, transform_event)
        save_csv(list_event, "event.csv")
        for event in list_event:
            if event.get("country") is not None:
                list_country.append(event["country"])
            if event.get("circuit") is not None:
                list_circuit.append(event["circuit"])
            
            producer.send("event_topic", json.dumps(event).encode("utf-8"))
            

        #push to xcom
        kwargs["ti"].xcom_push(key="list_country", value=list_country)
        kwargs["ti"].xcom_push(key="list_circuit", value=list_circuit)
        kwargs["ti"].xcom_push(key="list_event", value=list_event)

async def process_category(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_event = kwargs["ti"].xcom_pull(key="list_event", task_ids="get_event_task")
        tasks = [get_motogp_category_api(session_http, semaphone, event["event_id"]) for event in list_event]
        list_category = await asyncio.gather(*tasks)

        list_event_category = list(zip(list_event, list_category))
        list_category = [category for event, categories in list_event_category for category in categories]

        list_category = transform(list_category, transform_categories)
        save_csv(list_category, "category.csv")
        for category in list_category:
            producer.send("category_topic", json.dumps(category).encode("utf-8"))
        
        kwargs["ti"].xcom_push(key="list_event_category", value=list_event_category)

async def process_session(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_event_category = kwargs["ti"].xcom_pull(key="list_event_category", task_ids="get_category_task")
        
        tasks = []
        for event, categories in list_event_category:
            for category in categories:
                tasks.append(get_motogp_session_api(session_http, semaphone, event["event_id"], category["id"]))
        
        list_session = await asyncio.gather(*tasks)
        list_session = [session for task in list_session for session in task]
        
        list_session = transform(list_session, transform_sessions)
        save_csv(list_session, "session.csv")
        for session in list_session:
            producer.send("session_topic", json.dumps(session).encode("utf-8"))
            
        kwargs["ti"].xcom_push(key="list_session", value=list_session)

async def process_classification(**kwargs):
    def add_session_id_to_classification(list_data):
        updated_classifications = []
        for item in list_data:
            if 'response' in item:
                response = item['response']
                session_id = item['session_id']

                if 'classification' in response and response['classification']:
                    classification_list = response['classification']

                    # Update each object in the 'classification' list with 'session_id'
                    for obj in classification_list:
                        obj['session_id'] = session_id

                    # Append the updated 'classification' to the result list
                    updated_classifications.extend(classification_list)

        return updated_classifications

        return updated_classifications
    
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_session = kwargs["ti"].xcom_pull(key="list_session", task_ids="get_session_task")
        tasks = [get_motogp_result_session_api(session_http, semaphone, session["session_id"], more_data={'session_id' :session['session_id']}) for session in list_session]
        list_classification = await asyncio.gather(*tasks)
        list_classification = add_session_id_to_classification(list_classification) 
        #list rider, team, constructor, country
        list_rider = []
        list_team = []
        list_constructor = []
        list_country = []
        list_classification = transform(list_classification, transform_classification)        
        save_csv(list_classification, "classification.csv")
        for obj in list_classification:
            # Assuming obj is a dictionary
            if obj.get("rider") is not None:
                list_rider.append(obj["rider"])
                if obj["rider"].get("country") is not None:
                    list_country.append(obj["rider"]["country"])

            if obj.get("team") is not None:
                list_team.append(obj["team"])

            if obj.get("constructor") is not None:
                list_constructor.append(obj["constructor"])
            producer.send("classification_topic", json.dumps(obj).encode("utf-8"))


        #push to xcom
        kwargs["ti"].xcom_push(key="list_rider", value=list_rider)
        kwargs["ti"].xcom_push(key="list_team", value=list_team)
        kwargs["ti"].xcom_push(key="list_constructor", value=list_constructor)
        kwargs["ti"].xcom_push(key="list_country", value=list_country)
    
def process_rider(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_rider = kwargs["ti"].xcom_pull(key="list_rider", task_ids="get_classification_task")
    
    list_rider = transform(list_rider, transform_riders)
    save_csv(list_rider, "rider.csv")

    for rider in list_rider:
        producer.send("rider_topic", json.dumps(rider).encode("utf-8"))

def process_team(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_team = kwargs["ti"].xcom_pull(key="list_team", task_ids="get_classification_task")
    
    list_team = transform(list_team, transform_teams)
    save_csv(list_team, "team.csv")

    for team in list_team:
        producer.send("team_topic", json.dumps(team).encode("utf-8"))

def process_constructor(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_constructor = kwargs["ti"].xcom_pull(key="list_constructor", task_ids="get_classification_task")
    
    list_constructor = transform(list_constructor, transform_constructors)
    save_csv(list_constructor, "constructor.csv")

    for constructor in list_constructor:
        producer.send("constructor_topic", json.dumps(constructor).encode("utf-8"))

def process_country(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_country = kwargs["ti"].xcom_pull(key="list_country", task_ids="get_classification_task")
    list_temp = kwargs["ti"].xcom_pull(key="list_country", task_ids="get_event_task")
    list_country.extend(list_temp)

    list_country = transform(list_country, transform_countries)
    save_csv(list_country, "country.csv")

    for country in list_country:
        producer.send("country_topic", json.dumps(country).encode("utf-8"))
    
def process_circuit(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_circuit = kwargs["ti"].xcom_pull(key="list_circuit", task_ids="get_event_task")
    
    list_circuit = transform(list_circuit, transform_circuits)
    save_csv(list_circuit, "circuit.csv")
    
    for circuit in list_circuit:
        producer.send("circuit_topic", json.dumps(circuit).encode("utf-8"))


def async_process_season(**kwargs):
    asyncio.run(process_season(**kwargs))
    
def async_process_event(**kwargs):
    asyncio.run(process_event(**kwargs))
    
def async_process_category(**kwargs):
    asyncio.run(process_category(**kwargs))
    
def async_process_session(**kwargs):
    asyncio.run(process_session(**kwargs))
    
def async_process_classification(**kwargs):
    asyncio.run(process_classification(**kwargs))
