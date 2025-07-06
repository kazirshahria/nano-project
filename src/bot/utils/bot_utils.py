import json
import requests


def json_response(url: str, headers = None):
    response = requests.request(
        method='GET',
        url=url,
        headers=headers
    )
    content = response.content
    
    if response.status_code == 200:
        return json.loads(content)
