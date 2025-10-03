import os
from spotipy import Spotify
from spotipy import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

TIMEOUT_SECONDS = 15
MAX_RETRIES = 5

def get_spotify_auth_manager():
    return SpotifyOAuth(
        client_id=os.getenv('spotify_client_id'),
        client_secret=os.getenv('spotify_client_secret'),
        redirect_uri=os.getenv('redirect_uri'),
        scope='playlist-read-private user-top-read user-library-read user-follow-read user-read-recently-played',
        open_browser=False,
        cache_path = '/opt/airflow/cache/.cache'
    )


def get_spotify_client():
    auth_manager = get_spotify_auth_manager()

    return Spotify(
        auth_manager=auth_manager,
        requests_timeout=TIMEOUT_SECONDS, 
        retries=MAX_RETRIES              
    )