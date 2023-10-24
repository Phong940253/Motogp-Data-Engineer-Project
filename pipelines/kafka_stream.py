import json
# from kafka import KafkaProducer
import time


# https://api.motogp.pulselive.com/motogp/v1/results/seasons
# return all seasons
def get_motogp_all_season_api():
    import requests
    
    base_url = "https://api.motogp.pulselive.com"
    endpoint = "/motogp/v1/results/seasons"
    
    res = requests.get(base_url + endpoint)
    return res.json() if res.status_code == 200 else {"status_code": res.status_code, "message": res.text}
    
    

# https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid=db8dc197-c7b2-4c1b-b3a4-6dc534c023ef&isFinished=true
# return list event of season
def get_motogp_event_api(season_uuid):
    import requests

    base_url = "https://api.motogp.pulselive.com"
    endpoint = "/motogp/v1/results/events"
    params = {
        "seasonUuid": season_uuid,
        "isFinished": True
    }

    res = requests.get(base_url + endpoint, params=params)
    return res.json() if res.status_code == 200 else {"status_code": res.status_code, "message": res.text}

    # [
    #     {
    #         name: "Grand Prix of Qatar",
    #         id: event_uuid,
    #         ...
    #     }
    # ]


# https://api.motogp.pulselive.com/motogp/v1/results/categories?eventUuid=bfd8a08c-cbb4-413a-a210-6d34774ea4c5
# return list category of event
def get_motogp_category_api(event_uuid):
    import requests

    base_url = "https://api.motogp.pulselive.com"
    endpoint = "/motogp/v1/results/categories"
    params = {
        "eventUuid": event_uuid
    }

    res = requests.get(base_url + endpoint, params=params)
    return res.json() if res.status_code == 200 else {"status_code": res.status_code, "message": res.text}

    # [
    #     {
    #         name: "MotoGP",
    #         id: category_uuid,
    #         legacy_id: int
    #     }
    # ]


# return list session of event
def get_motogp_session_api(event_uuid, category_uuid):
    import requests

    base_url = "https://api.motogp.pulselive.com"
    endpoint = "/motogp/v1/results/sessions"
    params = {
        "eventUuid": event_uuid,
        "categoryUuid": category_uuid,
    }

    res = requests.get(base_url + endpoint, params=params)
    return res.json() if res.status_code == 200 else {"status_code": res.status_code, "message": res.text}

    # [
    #     {
    #         date: datetime,
    #         number: int,
    #         id: session_id,
    #         ...
    #     }
    # ]


# return result of session
def get_motogp_result_session_api(session):
    import requests

    base_url = "https://api.motogp.pulselive.com"
    endpoint = f"/motogp/v1/results/session/{session}/classification"
    params = {
        "test": False
    }

    res = requests.get(base_url + endpoint, params=params)
    return res.json() if res.status_code == 200 else {"status_code": res.status_code, "message": res.text}

# print(get_motogp_api("344e3645-b719-4709-8d88-698b128b1720"))
# print(get_motogp_session_api("bfd8a08c-cbb4-413a-a210-6d34774ea4c5", "e8c110ad-64aa-4e8e-8a86-f2f152f6a942"))
# print(get_motogp_all_season_api())

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
