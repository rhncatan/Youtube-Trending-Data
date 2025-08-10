import pandas as pd
from database import insert_today_trending

def load_today_trending(df):
    try:
        insert_today_trending(df)
        return "Successfully inserted for today"
    except Exception as e:
        return e