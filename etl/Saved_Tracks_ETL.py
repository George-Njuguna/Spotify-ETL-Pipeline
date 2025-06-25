import psycopg2
import os
from dotenv import load_dotenv
from API import get_spotify_client
from Functions import create_saved_tracks_table , insert_saved_tracks_bulk

load_dotenv()
sp = get_spotify_client()

saved_tracks = sp.current_user_saved_tracks()


saved_tracks_data = [
    {
        'name':item['track']['name'],
        'track_id':item['track']['id'],
        'duration':int(item['track']['duration_ms']),
        'explicit':item['track']['explicit'],
        'popularity':int(item['track']['popularity']),
        'album_name':item['track']['album']['name'],
        'album_id':item['track']['album']['id'],
        'artist_name':item['track']['artists'][0]['name'],
        'artist_id':item['track']['artists'][0]['id']
    }
    for item in saved_tracks['items']
]

 # Loading the data 
try:
    # connecting to the database
    conn = psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('postgre_account'),
        password=os.getenv('postgre_password'),
        host=os.getenv('host'),
        port=os.getenv('port')
    )
    print('✅ Connection made')

    # run your ETL or insert logic here
    create_saved_tracks_table(conn)
    insert_saved_tracks_bulk(conn, saved_tracks_data)

except Exception as e:
    print("❌ ERROR:", e)
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
        print("🔌 CONNECTION CLOSED")

def main():
    print("✅✅ COMPLETED saved tracks ETL")

if __name__ == "__main__":
    main()