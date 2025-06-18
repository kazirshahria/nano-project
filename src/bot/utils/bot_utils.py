import requests


def json_response(url: str):
    response = requests.request(
        method='GET',
        url=url
    )

    if response.status_code == 200:
        return response.json()
