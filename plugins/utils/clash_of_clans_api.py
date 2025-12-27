import requests
import logging
import copy
import os

from dotenv import load_dotenv
load_dotenv()

TOKEN_API = os.getenv('TOKEN_API')

# ------------------
# request template
# ------------------

def send_request(
        base_url: str, 
        headers: dict | None = None, 
        params: dict | None = None
    ) -> dict:
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f'An error occurred: {e}')

# ------------------
# API requests
# ------------------

def get_location_info(
        country_code: str = "RU"
    ):
    base_url = f"https://api.clashofclans.com/v1/locations"
    headers = {
        "Authorization": f"Bearer {TOKEN_API}",
        "Content-Type": "application/json"
    }
    params = {}

    data = send_request(base_url, headers, params)
    for loc in data["items"]:
        if loc["isCountry"]:
            if loc["countryCode"] == country_code:
                return (loc["id"], loc["name"], loc["countryCode"])

    return None

def get_top_n_clans(
        location_id: str,
        limit: int = 10
):
    base_url = f"https://api.clashofclans.com/v1/locations/{location_id}/rankings/clans"
    headers = {
        'Authorization': f'Bearer {TOKEN_API}',
        'Content-Type': 'application/json'
    }
    params = {
        'limit': limit
    }

    data = send_request(base_url, headers, params)
    if data is None or 'items' not in data:
        raise ValueError("API returned unexpected structure")

    return data["items"]

def get_clan_info(
        clan_tag: str
):
    clan_tag = clan_tag.replace('#', '%23')
    base_url = f"https://api.clashofclans.com/v1/clans/{clan_tag}"
    headers = {
        "Authorization": f"Bearer {TOKEN_API}",
        "Content-Type": "application/json"
    }
    params = {}

    data = send_request(base_url, headers, params)
    if data is None:
        raise ValueError("API returned unexpected structure")

    return data

def get_player_info(
        player_tag: str
):
    player_tag = player_tag.replace('#', '%23')
    base_url = f"https://api.clashofclans.com/v1/players/{player_tag}"
    headers = {
        "Authorization": f"Bearer {TOKEN_API}",
        "Content-Type": "application/json"
    }
    params = {}

    data = send_request(base_url, headers, params)
    if data is None:
        raise ValueError("API returned unexpected structure")

    return data

# ------------------
# API scrapping
# ------------------

def get_raw_data(
    country_code: str = "RU",
    top_n_clans: int = 25,
    **context
):
    location_info = get_location_info(country_code)
    if location_info is None:
        raise ValueError(f"cannot find info about specified country using this code: {country_code}")

    top_clans = get_top_n_clans(location_info[0], limit = top_n_clans)

    top_clans_info = {}
    top_clans_member_info = {}
    for clan in top_clans:
        clan_tag = clan["tag"]
        clan_info = get_clan_info(clan_tag)

        top_clans_info[clan_tag] = clan_info
        top_clans_member_info[clan_tag] = []

        for member in clan_info["memberList"]:
            member_tag = member["tag"]
            member_info = get_player_info(member_tag)
            top_clans_member_info[clan_tag].append({member_tag: member_info})
            logging.info(f"sucessfully get info about player with tag {member_tag}")

        logging.info(f"sucessfully get info about clan with tag {clan_tag}")

    return top_clans_info, top_clans_member_info

def preprocess_raw_data(
    **context
):
    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="get_raw_data")
    top_clans_info_copy = copy.deepcopy(top_clans_info)
    top_clans_member_info_copy = copy.deepcopy(top_clans_member_info)

    clan_extra_topics = ["labels", "badgeUrls", "memberList"]
    clan_member_extra_topics = ["clan", "labels"]
    for clan_tag in top_clans_info_copy.keys():       
        try:
            # remove extra topics for clan
            for extra_topic in clan_extra_topics:
                top_clans_info_copy[clan_tag].pop(extra_topic)

            # remove extra topics for clan members
            for member in top_clans_member_info_copy[clan_tag]:
                member_tag = next(iter(member.keys()))
                for extra_topic in clan_member_extra_topics: 
                    member[member_tag].pop(extra_topic)

            logging.info(f"sucessfully removed extra data for clan with tag {clan_tag}")
        except Exception as e:
            raise RuntimeError(f"cannot upload data to minio! error: {e}")
        
    return top_clans_info_copy, top_clans_member_info_copy