import os
import requests

from dotenv import load_dotenv
load_dotenv()

TOKEN_API = os.getenv('TOKEN_API')

# request template
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

# API requests
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