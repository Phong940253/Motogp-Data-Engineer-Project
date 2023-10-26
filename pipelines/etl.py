from kafka import KafkaProducer
import time
import pandas as pd
import re
import aiohttp
import asyncio
import time
import json
import logging

async def get_request(session_http, semaphore, url, params=None):
    async with semaphore:
        async with session_http.get(url, params=params) as res:
            return await res.json() if res.status == 200 else {"status_code": res.status, "message": res.text}
            


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
async def get_motogp_result_session_api(session_http, semaphore, session):
    return await get_request(
        session_http,
        semaphore,
        f"https://api.motogp.pulselive.com/motogp/v1/results/session/{session}/classification",
        params={"test": "false"},
    )


# print(get_motogp_api("344e3645-b719-4709-8d88-698b128b1720"))
# print(get_motogp_session_api("bfd8a08c-cbb4-413a-a210-6d34774ea4c5", "e8c110ad-64aa-4e8e-8a86-f2f152f6a942"))
# print(get_motogp_all_season_api())

# Transform
def transform_season(data):#
    return {
        "season_id": data["id"],
        "year": data["year"],
    }

def transform_event(data):#
    return {
        "event_id": data["id"],
        "name": data["name"],
        "country_iso": data["country"]["iso"],
        "circuit_id": data["circuit"]["id"],
        "date_start": data["date_start"],
        "date_end": data["date_end"],
        "season_id": data["season"]["id"],
        "sponsored_name": data["sponsored_name"],
        "test": data["test"],
        "toad_api_uuid": data["toad_api_uuid"],
        "short_name": data["short_name"],
        "legacy_id": data["legacy_id"],
    }


def transform_category(data):#
    return {
        "category_id": data["id"],
        "name": re.sub(r"[^a-zA-Z0-9\s]", "", data["name"]),
        "legacy_id": data["legacy_id"],
    }


def transform_session(data):#
    return {
        "session_id": data["id"],
        "date": data["date"],
        "number": data["number"],
        "event_id": data["event"]["id"],
        "category_id": data["category"]["id"],
        "type": data["type"],
        "circuit_id": data["event"]["circuit"]["id"],
        "condition": data["condition"],
    }


def transform_rider(data):#
    return {
        "rider_id": data["id"],
        "full_name": data["full_name"],
        "country_iso": data["country"]["iso"],
        "legacy_id": data["legacy_id"],
        "number": data["number"],
        "riders_api_uuid": data["riders_api_uuid"],
    }


def transform_team(data):#
    return {
        "team_id": data["id"],
        "name": data["name"],
        "legacy_id": data["legacy_id"],
    }

def transform_constructor(data):#
    return {
        "constructor_id": data["id"],
        "name": data["name"],
        "legacy_id": data["legacy_id"],
    }

def transform_curcuit(data):#
    return {
        "circuit_id": data["id"],
        "name": data["name"],
        "legacy_id": data["legacy_id"],
        "place": data["place"],
        "country_iso": data["nation"],
    }


def transform_country(data):#
    return {
        "country_iso": data["iso"],
        "name": data["name"],
        "region_iso": data["region_iso"],
    }

def transform_classification(data):
    # Define default values for each key
    default_values = {
        "points": 0,  # Default value for "points"
        "gap": None,  # Default value for "gap" (you can change this as needed)
        "time": None,  # Default value for "time" (you can change this as needed)
        "total_laps": None,  # Default value for "total_laps" (you can change this as needed)
        "average_speed": None,  # Default value for "average_speed" (you can change this as needed)
    }

    # Check if the "constructor" key exists and is not None
    constructor_data = data.get("constructor")
    if constructor_data is not None:
        constructor_id = constructor_data.get("id")
    else:
        # Handle the case where "constructor" is None or missing
        constructor_id = None  # You can change this default value as needed
    rider_data = data.get("rider")
    if rider_data is not None:
        rider_id = rider_data.get("id")
    else:
        # Handle the case where "constructor" is None or missing
        rider_id = None
    team_data = data.get("team")
    if team_data is not None:
        team_id = team_data.get("id")
        session_id = team_data.get("season", {}).get("id")
    else:
        # Handle the case where "constructor" is None or missing
        team_id = None
        session_id = None

    transformed_data = {
        "classification_id": data.get("id"),
        "session_id": session_id,
        "rider_id": rider_id,
        "team_id": team_id,
        "constructor_id": constructor_id,
        "position": data.get("position"),
        "point": data.get("points", default_values["points"]),
        "gap": data.get("gap", default_values["gap"]),
        "time": data.get("time", default_values["time"]),
        "total_laps": data.get("total_laps", default_values["total_laps"]),
        "average_speed": data.get("average_speed", default_values["average_speed"]),
    }

    return transformed_data


async def process_season(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session_http:
        list_season = await get_motogp_all_season_api(session_http, semaphone)
        for season in list_season:
            season = transform_season(season)
            producer.send("season_topic", json.dumps(season).encode("utf-8"))
            
        kwargs["ti"].xcom_push(key="list_season", value=list_season)

async def process_event(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_season = kwargs["ti"].xcom_pull(key="list_season", task_ids="get_season_task")
        tasks = [get_motogp_event_api(session_http, semaphone, season["id"]) for season in list_season]
        list_event = await asyncio.gather(*tasks)
        list_event = [event for task in list_event for event in task]

        #list country, circuit
        list_country = []
        list_circuit = []

        for event in list_event:
            list_country.append(event["country"])
            list_circuit.append(event["circuit"])
            #transform event
            event = transform_event(event)
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
        tasks = [get_motogp_category_api(session_http, semaphone, event["id"]) for event in list_event]
        list_category = await asyncio.gather(*tasks)

        list_event_category = list(zip(list_event, list_category))
        for event, categories in list_event_category:
            for category in categories:
                category = transform_category(category)
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
                tasks.append(get_motogp_session_api(session_http, semaphone, event["id"], category["id"]))
        
        list_session = await asyncio.gather(*tasks)
        list_session = [session for task in list_session for session in task]

        for session in list_session:
            session = transform_session(session)
            producer.send("session_topic", json.dumps(session).encode("utf-8"))
            
        kwargs["ti"].xcom_push(key="list_session", value=list_session)

async def process_classification(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_session = kwargs["ti"].xcom_pull(key="list_session", task_ids="get_session_task")
        tasks = [get_motogp_result_session_api(session_http, semaphone, session["id"]) for session in list_session]
        list_classification = await asyncio.gather(*tasks)
        list_classification = [task for task in list_classification ]
        #list rider, team, constructor, country
        list_rider = []
        list_team = []
        list_constructor = []
        list_country = []
        list_classification = [c for c in list_classification]

        for classification in list_classification:
            if classification.get("classification") is not None:
                classification = classification["classification"]
                for obj in classification:
                    # Assuming obj is a dictionary
                    if obj.get("rider") is not None:
                        list_rider.append(obj["rider"])
                        if obj["rider"].get("country") is not None:
                            list_country.append(obj["rider"]["country"])

                    if obj.get("team") is not None:
                        list_team.append(obj["team"])

                    if obj.get("constructor") is not None:
                        list_constructor.append(obj["constructor"])
                    #transform classification
                    try: 
                        obj = transform_classification(obj)
                        producer.send("classification_topic", json.dumps(obj).encode("utf-8"))
                    except Exception as e:
                        logging.error("Error: %s", e)
                        logging.error("data: ", obj)
                    
        #push to xcom
        kwargs["ti"].xcom_push(key="list_rider", value=list_rider)
        kwargs["ti"].xcom_push(key="list_team", value=list_team)
        kwargs["ti"].xcom_push(key="list_constructor", value=list_constructor)
        kwargs["ti"].xcom_push(key="list_country", value=list_country)
    
def process_rider(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_rider = kwargs["ti"].xcom_pull(key="list_rider", task_ids="get_classification_task")
    for rider in list_rider:
        rider = transform_rider(rider)
        producer.send("rider_topic", json.dumps(rider).encode("utf-8"))

def process_team(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_team = kwargs["ti"].xcom_pull(key="list_team", task_ids="get_classification_task")
    for team in list_team:
        team = transform_team(team)
        producer.send("team_topic", json.dumps(team).encode("utf-8"))

def process_constructor(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_constructor = kwargs["ti"].xcom_pull(key="list_constructor", task_ids="get_classification_task")
    for constructor in list_constructor:
        constructor = transform_constructor(constructor)
        producer.send("constructor_topic", json.dumps(constructor).encode("utf-8"))

def process_country(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_country = kwargs["ti"].xcom_pull(key="list_country", task_ids="get_classification_task")
    list_temp = kwargs["ti"].xcom_pull(key="list_country", task_ids="get_event_task")
    list_country.extend(list_temp)

    for country in list_country:
        country = transform_country(country)
        producer.send("country_topic", json.dumps(country).encode("utf-8"))
    
def process_circuit(**kwargs):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    list_circuit = kwargs["ti"].xcom_pull(key="list_circuit", task_ids="get_event_task")
    for circuit in list_circuit:
        circuit = transform_curcuit(circuit)
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

    
# start_time = time.time()
# # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# asyncio.run(main())
# print("--- %s seconds ---" % (time.time() - start_time))

# start_time = time.time()
# main1()
# print("--- %s seconds ---" % (time.time() - start_time))



# async def process_season(season):
#     events = await get_motogp_event_api(season["id"])
#     for event in events:
#         event_transformed = transform_event(event)
#         await send_to_kafka(event_transformed, "event_topic")
#         await process_event(event)

# async def process_event(event):
#     categories = await get_motogp_category_api(event["id"])
#     event_transformed = transform_event(event)
#     await send_to_kafka(event_transformed, "event_topic")
#     for category in categories:
#         category_transformed = transform_category(category)
#         await send_to_kafka(category_transformed, "category_topic")
#         await process_category(category)

# async def process_category(category):
#     sessions = await get_motogp_session_api(category["id"])
#     category_transformed = transform_category(category)
#     await send_to_kafka(category_transformed, "category_topic")
#     for session in sessions:
#         session_transformed = transform_session(session)
#         await send_to_kafka(session_transformed, "session_topic")
#         await process_session(session)

# async def process_session(session):
#     classification = await get_motogp_result_session_api(session["id"])
#     session_transformed = transform_session(session)
#     await send_to_kafka(session_transformed, "session_topic")
#     for obj in classification:
#         classi = {
#             "id": obj["id"],
#             "rider_id": obj["rider"]["id"],
#             # Add other fields here
#         }
#         await send_to_kafka(classi, "classification_topic")
#         await process_rider(obj["rider"])
#         # Add sending for constructor and team here if needed

# async def process_rider(rider):
#     rider_transformed = transform_rider(rider)
#     await send_to_kafka(rider_transformed, "rider_topic")

# async def main():
#     seasons = await get_motogp_all_season_api()
#     tasks = [process_season(season) for season in seasons]
#     await asyncio.gather(*tasks)




# import random

# # choose 1 random season from get_motogp_all_season_api()
# season_gp = random.choice(get_motogp_all_season_api())
# print(season_gp.keys())
# season_id = season_gp["id"]
# print(season_id)

# # choose 1 random event from get_motogp_event_api(session_id)
# event_gp = random.choice(get_motogp_event_api(season_id))
# print(event_gp.keys())
# event_id = event_gp["id"]
# print(event_id)

# # choose 1 random category from get_motogp_category_api(event_id)
# category_gp = random.choice(get_motogp_category_api(event_id))
# print(category_gp.keys())
# category_id = category_gp["id"]
# print(category_id)

# # choose 1 random session from get_motogp_session_api(event_id, category_id)
# session_gp = random.choice(get_motogp_session_api(event_id, category_id))
# print(session_gp.keys())
# session_id = session_gp["id"]
# print(session_id)

# # print result of session
# print(get_motogp_result_session_api(session_id))
