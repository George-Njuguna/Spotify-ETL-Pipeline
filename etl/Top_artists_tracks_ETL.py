import psycopg2
import os
from dotenv import load_dotenv
from API import get_spotify_client
from datetime import datetime
import Functions

 # getting the Month
date = datetime.now().date()

load_dotenv()

try:
    sp = get_spotify_client()
    print(" GOT SPOTIFY CLIENT ")
except:
    print("COULDN'T GET SPOTIFY CLIENT : RELOAD CACHE")

top_artists = sp.current_user_top_artists(time_range = 'short_term')
top_tracks=sp.current_user_top_tracks(time_range = 'short_term')

 # Top Artists
top_artists_data = []

for item in top_artists['items']:
   try:
      genre = item['genres'][0]

   except:
      genre = 'no_genre'

   top_artists_data.append(
    {
      'name' : item['name'],
      'id' : item['id'],
      'popularity' : int(item['popularity']),
      'followers' : int(item['followers']['total']),
      'genre' : genre,
      'date': date
    }
   )

 # Top Tracks
top_tracks_data = [
    {
        'name':item['name'],
        'track_id':item['id'],
        'duration':int(item['duration_ms']),
        'explicit':item['explicit'],
        'popularity':int(item['popularity']),
        'album_name':item['album']['name'],
        'album_id':item['album']['id'],
        'artist_name':item['artists'][0]['name'],
        'artist_id':item['artists'][0]['id'],
        'date': date   
     
    }
    for item in top_tracks['items']
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

     # Creating/checking if the table exists
    Functions.create_top_artist_table(conn)
    Functions.create_top_tracks_table(conn)

     # Loading the data
    Functions.insert_top_artists_bulk(conn, top_artists_data)
    Functions.insert_top_tracks_bulk(conn, top_tracks_data)

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