import os
import requests

TOKEN_API = os.getenv('TOKEN_API')


def get_api_data(base_url: str, headers: dict | None = None, params: dict | None = None) -> dict:
    try:
        # Send the GET request with the headers and the parameters
        response = requests.get(base_url, headers=headers, params=params)
        
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
    
        # Access the JSON data from the response
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f'An error occurred: {e}')


def get_clan_rankings():
    base_url = 'https://api.clashofclans.com/v1/locations/32000193/rankings/clans'
    headers = {
        'Authorization': f'Bearer {TOKEN_API}',
        'Content-Type': 'application/json'
    }
    params = {
        'limit': 200
    }

    data = get_api_data(base_url, headers, params)

    if data is None or 'items' not in data:
        raise ValueError("API returned unexpected structure")

    return data["items"]
    