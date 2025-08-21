from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import os
import requests
from database import insert_today_trending, update_categories

api_key = 'AIzaSyABveruujCVaLDrqkcZqZiyvpabXLHmDcY'
youtube = build('youtube','v3', developerKey=api_key)

os.makedirs('../data/raw', exist_ok=True)
os.makedirs('../data/categories', exist_ok=True)
output_dir = os.path.join(os.getcwd(), 'data', 'raw')



def get_trending_videos(region_code='PH',max_results='50'):

    request = youtube.videos().list(
        part='snippet,statistics',
        chart='mostPopular',
        regionCode=region_code,
        maxResults = max_results
    )

    response = request.execute()

    data = []
    for item in response['items']:
        data.append({
            'video_id': item['id'],
            'title': item['snippet']['title'],
            'channel_title': item['snippet']['channelTitle'],
            'category_id': item['snippet']['categoryId'],
            'published_at': item['statistics'].get('viewCount'),
            'comment_count':item['statistics'].get('commentCount'),
            'trend_date': today
        })
    
    df = pd.DataFrame(data)
    
    

    return df


def refresh_categories(region_code='PH'):
    today =  datetime.today().strftime('%Y-%m-%d')
    url = 'https://www.googleapis.com/youtube/v3/videoCategories'
    params = {
        'part': 'snippet',
        'regionCode': region_code,
        'key': api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Parse the category ID mapping
    categories = {
        item['id']: item['snippet']['title']
        for item in data['items']
    }
    df_categories = pd.DataFrame(list(categories.items()), columns=['CategoryId','CategoryName'])
    df_categories['run_date'] = today

    update_categories(df_categories)

    try:
        df_categories.to_csv(os.path.join(os.getcwd(),'data','categories', f'categories_{region_code}.csv'),
                  index=False)
    except Exception as e:
        print(f"Failed to save csv: {e}")

    print(df_categories)





if __name__ == '__main__':
    os.makedirs('../data/raw', exist_ok=True)
    os.makedirs('../data/categories', exist_ok=True)
    output_dir = os.path.join(os.getcwd(), 'data', 'raw')
    
    today =  datetime.today().strftime('%Y-%m-%d')
    region_code='PH'
    df = get_trending_videos(region_code=region_code)
    today =  datetime.today().strftime('%Y-%m-%d')

    try:
        df.to_csv(os.path.join(output_dir, f'{today}_{region_code}_trending.csv'),
                  index=True,
                  index_label='Rank')
    except Exception as e:
        print(f"Failed to save csv: {e}")


