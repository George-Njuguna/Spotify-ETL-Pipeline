 # CREATING TABLES
 #Creating playlist Table
def create_playlist_table(conn):
    try: 
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS playlists (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    tracks INTEGER,
                    public BOOLEAN DEFAULT TRUE
                );
            """)
            conn.commit()
            print("✅ Table 'playlists' CREATED/EXISTS).")
    
    except Exception as e:
        print("❌ ERROR Creating table playlist : ", e)
        if conn:
            conn.rollback()

 # Creating the followed_artists_table                
def create_followed_artist_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS followed_artists (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    popularity INTEGER,
                    followers INTEGER,
                    genre TEXT NOT NULL
                );
            """)
            conn.commit()
            print("✅ Table 'followed_artists' CREATED/EXISTS).")
            
    except Exception as e:
        print("❌ ERROR Creating table followed artists : ", e)
        if conn:
            conn.rollback()

 # Creating the Saved_albums_table
def create_saved_albums_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS saved_albums (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    artist_id TEXT,
                    tracks INTEGER,
                    popularity INTEGER
                );
            """)
            conn.commit()
            print("✅ Table 'saved_albums' CREATED/EXISTS).")
            
    except Exception as e:
        print("❌ ERROR Creating table saved albums : ", e)
        if conn:
            conn.rollback()

 # Creating Saved_tracks table              
def create_saved_tracks_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS saved_tracks (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    artist_id TEXT,
                    album_name TEXT NOT NULL,
                    album_id TEXT,
                    duration INTEGER,
                    explicit BOOLEAN DEFAULT TRUE,
                    popularity INTEGER
                );
            """)
            conn.commit()
            print("✅ Table 'saved_tracks' CREATED/EXISTS).")
        
    except Exception as e:
        print("❌ ERROR Creating table saved tracks : ", e)
        if conn:
            conn.rollback()

 # Creating the top tracks Table
def create_top_tracks_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS top_tracks (
                    record_id SERIAL PRIMARY KEY,
                    id TEXT,
                    name TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    artist_id TEXT NOT NULL,
                    album_name TEXT NOT NULL,
                    album_id TEXT,
                    duration INTEGER,
                    explicit BOOLEAN DEFAULT TRUE,
                    popularity INTEGER,
                    date DATE,
                    UNIQUE (id, date)
                );
            """)
            conn.commit()
            print("✅ Table 'top tracks' CREATED/EXISTS).")
            
    except Exception as e:
        print("❌ ERROR Creating table top tracks : ", e)
        if conn:
            conn.rollback()

 # Creating Top artist
def create_top_artist_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS top_artists (
                    record_id SERIAL PRIMARY KEY,
                    id TEXT,
                    name TEXT NOT NULL,
                    popularity INTEGER,
                    followers INTEGER,
                    genre TEXT,
                    date DATE,
                    UNIQUE (id, date)
                );
            """)
            conn.commit()
            print("✅ Table 'top artist' CREATED/EXISTS).")
        
    except Exception as e:
        print("❌ ERROR Creating table top artist : ", e)
        if conn:
            conn.rollback()

 # Creating Recently Played Table             
def create_recently_played_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recently_played_tracks (
                    played_at TIMESTAMPTZ PRIMARY KEY,
                    id TEXT,
                    name TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    artist_id TEXT,
                    album_name TEXT NOT NULL,
                    album_id TEXT,
                    duration INTEGER,
                    explicit BOOLEAN DEFAULT TRUE,
                    popularity INTEGER
                );
            """)
            conn.commit()
            print("✅ Table 'recently played Tracks' CREATED/EXISTS.")
        
    except Exception as e:
        print("❌ ERROR Creating table recently played tracks : ", e)
        if conn:
            conn.rollback()


 # Loading Playlist Data
def insert_playlists_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO playlists (id, name, owner_id, public, tracks)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, [
                (pl['id'], pl['name'], pl['owner_id'], pl['public'], pl['tracks'])
                for pl in data
            ])

            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading playlist data",e)

 # Loading Followed Artist Data
def insert_followed_artists_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO followed_artists (id, name, popularity, followers, genre)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, [
                (pl['id'], pl['name'], pl['popularity'], pl['followers'], pl['genre'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading followed artist data",e)
            
 # Loading saved Albums data
def insert_saved_albums_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO saved_albums (id, name, artist_name, artist_id, tracks, popularity)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, [
                (pl['id'], pl['name'], pl['artist_name'], pl['artist_id'], pl['total_tracks'], pl['popularity'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading saved albums data",e)

 # Loading Saved Tracks Data
def insert_saved_tracks_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO saved_tracks (id, name, artist_name, artist_id, album_name, album_id, duration, explicit, popularity)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, [
                (pl['track_id'], pl['name'], pl['artist_name'], pl['artist_id'], pl['album_name'],  pl['album_id'],  pl['duration'],  pl['explicit'] ,  pl['popularity'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading saved Tracks data",e)

 # Loading Top Tracks Data
def insert_top_tracks_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO top_tracks (id, name, artist_name, artist_id, album_name, album_id, duration, explicit, popularity,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id, date) DO NOTHING;
            """, [
                (pl['track_id'], pl['name'], pl['artist_name'], pl['artist_id'], pl['album_name'],  pl['album_id'],  pl['duration'],  pl['explicit'],  pl['popularity'], pl['date'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading top Tracks data",e)

 # Loading the top artist data
def insert_top_artists_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO top_artists (id, name, popularity, followers, genre, date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id, date) DO NOTHING;
            """, [
                (pl['id'], pl['name'], pl['popularity'], pl['followers'], pl['genre'], pl['date'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading top Artist data",e)

 # Loading recent tracks 
def insert_recent_tracks_bulk(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO recently_played_tracks (played_at, id, name, artist_name, artist_id, album_name, album_id, duration, explicit, popularity)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (played_at) DO NOTHING;
            """, [
                (pl['played_at'], pl['track_id'], pl['name'], pl['artist_name'], pl['artist_id'], pl['album_name'],  pl['album_id'],  pl['duration'],  pl['explicit'] ,  pl['popularity'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading saved albums data",e)