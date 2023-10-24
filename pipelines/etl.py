from kafka import KafkaProducer
import time
import pandas as pd
import re
import aiohttp
import asyncio
import time
import json

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
        "https://api.motogp.pulselive.com/motogp/v1/results/session/{session}/classification",
        params={"test": "false"},
    )


# print(get_motogp_api("344e3645-b719-4709-8d88-698b128b1720"))
# print(get_motogp_session_api("bfd8a08c-cbb4-413a-a210-6d34774ea4c5", "e8c110ad-64aa-4e8e-8a86-f2f152f6a942"))
# print(get_motogp_all_season_api())

# Transform
def transform_event(data):
    return {
        "id": data["id"],
        "name": data["name"],
        "country": data["country"]["iso"],
        "circuit": data["circuit"]["id"],
        "date_start": data["date_start"],
        "date_end": data["date_end"],
        "season_id": data["season"]["id"],
        "sponsored_name": data["sponsored_name"],
        "test": data["test"],
        "toad_api_uuid": data["toad_api_uuid"],
        "short_name": data["short_name"],
        "legacy_id": data["legacy_id"],
    }


def transform_category(data):
    return {
        "id": data["id"],
        "name": re.sub(r"[^a-zA-Z0-9\s]", "", data["name"]),
        "legacy_id": data["legacy_id"],
    }


def transform_session(data):
    return {
        "id": data["id"],
        "date": data["date"],
        "number": data["number"],
        "event_id": data["event"]["id"],
        "category_id": data["category"]["id"],
        "type": data["type"],
        "circuit_id": data["event"]["circuit"]["id"],
        "condition": data["condition"],
    }


def transform_rider(data):
    return {
        "rider_id": data["id"],
        "full_name": data["full_name"],
        "country": data["country"]["iso"],
        "legacy_id": data["legacy_id"],
        "number": data["number"],
        "riders_api_uuid": data["riders_api_uuid"],
    }


def transform_team(data):
    return {
        "team_id": data["id"],
        "name": data["name"],
        "legacy_id": data["legacy_id"],
    }


producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

async def send_to_kafka(data, topic):
    producer.send(topic, json.dumps(data).encode("utf-8"), )

async def process_season(**kwargs):
    semaphone = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session_http:
        list_season = await get_motogp_all_season_api(session_http, semaphone)
        for season in list_season:
            await send_to_kafka(season, "season_topic")
        kwargs["ti"].xcom_push(key="list_season", value=list_season)

async def process_event(**kwargs):
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_season = kwargs["ti"].xcom_pull(key="list_season", task_ids="get_season_task")
        tasks = [get_motogp_event_api(session_http, semaphone, season["id"]) for season in list_season]
        list_event = await asyncio.gather(*tasks)
        print(len(list_event))
        list_event = [event for task in list_event for event in task]
        print(len(list_event))
        print(type(list_event))
        for event in list_event:
            event = transform_event(event)
            await send_to_kafka(event, "event_topic")
        kwargs["ti"].xcom_push(key="list_event", value=list_event)

async def process_category(**kwargs):
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_event = kwargs["ti"].xcom_pull(key="list_event", task_ids="get_event_task")
        tasks = [get_motogp_category_api(session_http, semaphone, event["id"]) for event in list_event]
        list_category = await asyncio.gather(*tasks)
        print(len(list_category))
        print(type(list_category))
        list_event_category = list(zip(list_event, list_category))
        for event, categories in list_event_category:
            for category in categories:
                category = transform_category(category)
                await send_to_kafka({"id": category["id"], "event_id": event["id"], "name": category["name"], "legacy_id": category["legacy_id"]}, "category_topic")
        kwargs["ti"].xcom_push(key="list_event_category", value=list_event_category)

async def process_session(**kwargs):
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_event_category = kwargs["ti"].xcom_pull(key="list_event_category", task_ids="get_category_task")
        
        tasks = []
        for event, categories in list_event_category:
            for category in categories:
                tasks.append(get_motogp_session_api(session_http, semaphone, event["id"], category["id"]))
        
        list_session = await asyncio.gather(*tasks)
        list_session = [session for task in list_session for session in task]
        print(len(list_session))
        print(type(list_session))
        for session in list_session:
            session = transform_session(session)
            await send_to_kafka(session, "session_topic")
        kwargs["ti"].xcom_push(key="list_session", value=list_session)

async def process_classification(**kwargs):
    semaphone = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session_http:
        list_session = kwargs["ti"].xcom_pull(key="list_session", task_ids="get_session_task")
        tasks = [get_motogp_result_session_api(session_http, semaphone, session["id"]) for session in list_session]
        list_classification = await asyncio.gather(*tasks)
        list_classification = [classification for task in list_classification for classification in task]
        print(len(list_classification))
        print(type(list_classification))
        for classification in list_classification:
            await send_to_kafka(classification, "classification_topic")
        kwargs['ti'].xcom_push(key='list_classification', value=list_classification)
    
# async def aprint(**kwargs):
#     await kwargs['ti'].xcom_push(key='test', value=[1,2,3,4])
#     print("123456789")
    
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
