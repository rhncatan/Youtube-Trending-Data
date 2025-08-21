import pandas as pd
from database import update_categories, extract_categories
from datetime import datetime
from data_ingest import refresh_categories

import os

def process_trending_topics(df,region_code):
    os.makedirs('data/processed', exist_ok=True)
    output_dir = os.path.join(os.getcwd(),'data','processed')
    df_process = df.copy()
    
    # Extract categories from DB
    df_categories = extract_categories()
    
    if df_categories.empty:
        refresh_categories(region_code=region_code)

    df_process['category_id'] = df_process['category_id'].astype(str)
    df_categories['CategoryId'] = df_categories['CategoryId'].astype(str) 

    # First join
    df_process = df_process.merge(
        df_categories,
        how='left',
        left_on='category_id',
        right_on='CategoryId'
    )

    # Step 1: If any missing CategoryName, try updating categories
    if df_process['CategoryName'].isna().any():
        print("Missing categories found — updating category list...")
        update_categories(df_process)
        
        # Reload categories and join again
        df_categories = extract_categories()
        df_process = df_process.merge(
            df_categories,
            how='left',
            left_on='category_id',
            right_on='CategoryId'
        )

    # Step 2: Impute missing CategoryName to 'Others'
    missing_count = df_process['CategoryName'].isna().sum()
    if missing_count > 0:
        print(f"{missing_count} categories still missing. Imputing as 'Others'.")
        df_process['CategoryName'] = df_process['CategoryName'].fillna('Others')

    # Step 3: Adjust Rank to start at 1
    df_process['Rank'] = df_process['Rank'] + 1

    try:
        today =  datetime.today().strftime('%Y-%m-%d')
        df_process.to_csv(os.path.join(output_dir, f'{today}_{region_code}_trending_p.csv'),
                  index=False)
    except Exception as e:
        print(f"Failed to save csv: {e}")


    return df_process

