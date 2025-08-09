from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import os


api_key = 'AIzaSyABveruujCVaLDrqkcZqZiyvpabXLHmDcY'

youtube = build('youtube','v3', developerKey=api_key)

os.makedirs('../data/raw', exist_ok=True)
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
    today =  datetime.today().strftime('%Y-%m-%d')
    

    return df

if __name__ == '__main__':
    region_code='PH'
    df = get_trending_videos(region_code=region_code)
    today =  datetime.today().strftime('%Y-%m-%d')

    try:
        df.to_csv(os.path.join(output_dir, f'{today}_{region_code}_trending.csv'),
                  index=True,
                  index_label='Rank')
    except Exception as e:
        print(f"Failed to save csv: {e}")


