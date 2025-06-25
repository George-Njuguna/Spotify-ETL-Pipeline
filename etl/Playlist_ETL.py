import psycopg2
import os
from dotenv import load_dotenv
from API import get_spotify_client
from Functions import create_playlist_table , create_followed_artist_table , create_saved_albums_table , insert_followed_artists_bulk , insert_playlists_bulk , insert_saved_albums_bulk

load_dotenv()
sp = get_spotify_client()

 # Getting the data from spotify
playlists = sp.current_user_playlists()
followed_artists = sp.current_user_followed_artists()
saved_albums = sp.current_user_saved_albums()

 # Loading and transforming the data
 #playlists
playlist_data = [
    {
        'id': item['id'],
        'name': item['name'],
        'owner_id': item['owner']['id'],
        'public': bool(item['public']),
        'tracks': int(item['tracks']['total'])
    }
    for item in playlists['items']
]

# Followed_artists
followed_artists_data = []

for item in followed_artists['artists']['items']:
   try:
      genre = item['genres'][0]

   except:
      genre = 'no_genre'

   followed_artists_data.append({
      'name' : item['name'],
      'id' : item['id'],
      'popularity' : int(item['popularity']),
      'followers' : int(item['followers']['total']),
      'genre' : genre

   })

# Saved Albums
saved_albums_data = [
    {
        'name':item['album']['name'],
        'id':item['album']['id'],
        'total_tracks':int(item['album']['total_tracks']),
        'artist_name':item['album']['artists'][0]['name'],
        'artist_id':item['album']['artists'][0]['id'],
        'popularity':int(item['album']['popularity'])
    }
    for item in saved_albums['items']
]

 #Loading the Data
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

    # Checking if the tables Exist/Creating The Tables
    create_playlist_table(conn)
    create_followed_artist_table(conn)
    create_saved_albums_table(conn)
    
    # Loading The Data
    insert_playlists_bulk(conn, playlist_data)
    insert_followed_artists_bulk(conn, followed_artists_data)
    insert_saved_albums_bulk(conn, saved_albums_data)

except Exception as e:
    print("❌ ERROR:", e)
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
        print("🔌 CONNECTION CLOSED")

def main():
    print("✅✅ COMPLETED playlist , Followed_artists , Saved_albums ETL")

if __name__ == "__main__":
    main()


