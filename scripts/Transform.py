import pandas as pd
from database import update_categories, extract_categories

def process_trending_topics(df):
    df_process = df.copy()
    
    # Extract categories from DB
    df_categories = extract_categories()

    # First join
    df_process = df_process.merge(
        df_categories,
        how='left',
        left_on='category_id',
        right_on='CategoryId'
    )

    # Step 1: If any missing CategoryName, try updating categories
    if df_process['CategoryName'].isna().any():
        print("🔄 Missing categories found — updating category list...")
        update_categories()
        
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
        print(f"⚠ {missing_count} categories still missing. Imputing as 'Others'.")
        df_process['CategoryName'] = df_process['CategoryName'].fillna('Others')

    # Step 3: Adjust Rank to start at 1
    df_process['Rank'] = df_process['Rank'] + 1

    return df_process

