import psycopg2
import os
from dotenv import load_dotenv
from API import get_spotify_client
from dateutil.parser import isoparse 
from datetime import datetime, timedelta, timezone
import pytz
from Functions import create_recently_played_table , insert_recent_tracks_bulk

load_dotenv()

area = pytz.timezone("Africa/Nairobi")
today = datetime.now(area).date()

local_2pm = datetime.combine(today, datetime.min.time()) + timedelta(hours=14)
local_2pm = area.localize(local_2pm)


utc_2pm = local_2pm.astimezone(pytz.utc)
timestamp_ms = int(utc_2pm.timestamp() * 1000)

sp = get_spotify_client()

recently_played_tracks = sp.current_user_recently_played(limit = 50 , before=timestamp_ms)
recently_played_tracks1 = sp.current_user_recently_played(limit = 50 , after=timestamp_ms)


recently_played_data = [
    {
        'played_at': isoparse(item['played_at']),
        'album_name':item['track']['album']['name'],
        'album_id':item['track']['album']['id'],
        'artist_name':item['track']['artists'][0]['name'],
        'artist_id':item['track']['artists'][0]['id'],
        'name':item['track']['name'],
        'track_id':item['track']['id'],
        'duration':item['track']['duration_ms'],
        'explicit':bool(item['track']['explicit']),
        'popularity':int(item['track']['popularity'])
    }
    for item in recently_played_tracks['items']
]


recently_played_data1 = [
    {
        'played_at': isoparse(item['played_at']),
        'album_name':item['track']['album']['name'],
        'album_id':item['track']['album']['id'],
        'artist_name':item['track']['artists'][0]['name'],
        'artist_id':item['track']['artists'][0]['id'],
        'name':item['track']['name'],
        'track_id':item['track']['id'],
        'duration':item['track']['duration_ms'],
        'explicit':bool(item['track']['explicit']),
        'popularity':int(item['track']['popularity'])
    }
    for item in recently_played_tracks1['items']
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
    create_recently_played_table(conn)
    insert_recent_tracks_bulk(conn, recently_played_data)
    insert_recent_tracks_bulk(conn, recently_played_data1)

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