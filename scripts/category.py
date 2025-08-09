import requests
import pandas as pd


api_key = 'AIzaSyABveruujCVaLDrqkcZqZiyvpabXLHmDcY'
REGION = 'PH'



url = 'https://www.googleapis.com/youtube/v3/videoCategories'
params = {
    'part': 'snippet',
    'regionCode': REGION,
    'key': api_key
}

response = requests.get(url, params=params)
data = response.json()

# Parse the category ID mapping
categories = {
    item['id']: item['snippet']['title']
    for item in data['items']
}


print(categories)
