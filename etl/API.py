import os
from spotipy import Spotify
from spotipy import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

def get_spotify_client():
    return Spotify(auth_manager = SpotifyOAuth(
        client_id = os.getenv('spotify_client_id'),
        client_secret = os.getenv('spotify_client_secret'),
        redirect_uri = os.getenv('redirect_uri'),
        scope = 'playlist-read-private user-top-read user-library-read user-follow-read user-read-recently-played',
        cache_path = '/opt/airflow/cache/.cache'
    ))
