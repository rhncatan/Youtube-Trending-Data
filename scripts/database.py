import sqlite3
import os
import pandas as pd

db = 'db/yt.db'
def create_tables():
    os.makedirs('db', exist_ok=True)
    
    with sqlite3.connect('db') as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS trending_topics (
            Rank INTEGER,
            video_id TEXT NOT NULL,
            title TEXT NOT NULL,
            channel_title TEXT,
            category_id INTEGER,
            published_at TEXT,
            comment_count INTEGER,
            trend_date TEXT NOT NULL,
            PRIMARY KEY (trend_date, video_id)
        );
        ''')

        conn.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            CategoryId TEXT NOT NULL,
            CategoryName TEXT NOT NULL,
            run_date DATE NOT NULL,
            PRIMARY KEY (CategoryId)
        );
        ''')

        conn.commit()
        print("Tables exist in database.")

def insert_today_trending(df):
    with sqlite3.connect(db) as conn:
        df.to_sql('temporary_table', 
                  conn, 
                  if_exists='replace', 
                  index=False) # write df into the database
        
        conn.execute("""
        INSERT OR REPLACE INTO trending_topics
        (Rank, video_id, title, channel_title, category_id, published_at, comment_count, trend_date)
        SELECT Rank, video_id, title, channel_title, category_id, published_at, comment_count, trend_date
        FROM temporary_table
        """)                                      # update the trendtopics table in the database
        conn.execute('DROP TABLE temporary_table')

        conn.commit()
        print(f"Inserted {len(df)} records into trending_topics")

def update_categories(df):
    with sqlite3.connect(db) as conn:
        df.to_sql('temp',
                  conn,
                  if_exists="replace",
                  index=False)

        conn.execute("""
        REPLACE INTO categories
        (CategoryId, CategoryName, run_date)
        SELECT CategoryId, CategoryName, run_date FROM temp
        """)
        conn.execute('DROP TABLE temp')

        print(f"Updated: {len(df)} records in categories")

def extract_categories():
    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query("SELECT * FROM categories", conn)
        return df

if __name__ == '__main__':
    create_tables()
    print("Database ready.")
