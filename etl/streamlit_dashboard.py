import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import psycopg2
import os
from dotenv import load_dotenv
import datetime
from scipy.stats import gaussian_kde
import plotly.graph_objects as go
from Functions import import_data

load_dotenv()

st.set_page_config(page_title="My Dashboard", layout="wide")

st.markdown("""
<style>
    .stApp {
        background-color: #000000;
    }

    /* Container Styling (#212121 Palette) */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #212121 !important;
        border: 1px solid #282828 !important;
        border-radius: 8px;
    }

    /* Font Face and Global Typography */
    @font-face {
        font-family: 'CircularStd';
        src: url('file:///G:/PYTHON_PROJECTS/Scripts/airflow-docker/CircularStd-BookItalic.woff2') format('woff2');
        font-weight: 800;
        font-style: italic;
    }

    html, body, [class*="css"], div, span, p {
        font-family: 'CircularStd', sans-serif;
    }

    /* KPI / Metric Styling */
    div[data-testid="stMetric"] {
        text-align: center;
    }

    div[data-testid="stMetricLabel"] {
        font-size: 0.99rem;
        font-weight: 800;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.6rem;
        font-weight: 700;
    }

    [data-testid="stMetricLabel"] p, 
    [data-testid="stMetricValue"], 
    h1, h2, h3, p {
        color: white !important;
    }

    /* =====================
    Text Hover Color
    ===================== */

    /* Hover effect for headings and paragraphs */
    h1:hover, h2:hover, h3:hover, p:hover,
    div[data-testid="stMetricLabel"] p:hover,
    div[data-testid="stMetricValue"]:hover {
        color: #1DB954 !important;  /* green on hover */
        transition: color 0.2s ease;
    }
        
    
    /* Slightly lighter header for readability within the black table */
    .stDataFrame [data-testid="stHeader"] {
        background-color: #121212 !important; 
    }

    /* Layout Padding */
    .block-container {
        padding-top: 1rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
</style>
""", unsafe_allow_html=True)


plt.rcParams["figure.facecolor"] = "none"
plt.rcParams["axes.facecolor"] = "none"
plt.rcParams["text.color"] = "white"
plt.rcParams["axes.labelcolor"] = "white"
plt.rcParams["xtick.color"] = "white"
plt.rcParams["ytick.color"] = "white"
plt.rcParams["legend.labelcolor"] = "white"
plt.rcParams["axes.titlecolor"] = "white"

spotify_palette = ["#1DB954", "#005F73", "#0A9396", "#94D2BD", "#E9D8A6", "#EE9B00", "#CA6702", "#BB3E03", "#AE2012", "#9B2226", "#643C94"]



# Cache the connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('postgre_account'),
        password=os.getenv('postgre_password'),
        host="localhost",
        port=os.getenv('port')
    )

# Cache the data load
@st.cache_data(ttl=300)

def load_cached_data(table):
    conn = get_connection()   
    return import_data(table, conn)

# Tables
playlists_df = load_cached_data("playlists")
followed_artists_df = load_cached_data("followed_artists")
saved_albums_df = load_cached_data("saved_albums")
recently_played_df = load_cached_data("recently_played_tracks")
top_artists_df = load_cached_data("top_artists")
top_tracks_df = load_cached_data("top_tracks")
saved_tracks_df = load_cached_data("saved_tracks")
genre_df = load_cached_data("genre")
playlist_songs_df = load_cached_data("playlist_tracks")

dataframes = [playlists_df , followed_artists_df , saved_albums_df, recently_played_df, top_artists_df, top_tracks_df, saved_tracks_df , genre_df , playlist_songs_df]

 # Creating a column duration minutes and changing played at to date 
recently_played_df["duration_minutes"] = pd.to_timedelta(recently_played_df["duration"], unit='ms') / pd.Timedelta(minutes=1) 
recently_played_df['played_at_date'] = recently_played_df['played_at'].dt.tz_localize(None).dt.date

 # creating a table containing totals 
overview_dfs = [playlists_df , followed_artists_df , saved_albums_df, saved_tracks_df ]
cols = ["Playlists", "Followed Artists", "Saved Albums", "Saved_Tracks"]
tot_counts = []

for df , name  in zip(overview_dfs,cols):
    totals = (df.shape)[0]
    tot_counts.append({
        "df" : name ,
        "counts" : totals
    })

tot_df = pd.DataFrame(tot_counts)

 # getting max date and min date 
max_date = recently_played_df["played_at_date"].max()
min_date = recently_played_df["played_at_date"].min()

 # Setting Header
st.title("SPOTIFY WRAPPED DASHBOARD")

 # setting tabs
T1,T2,T3,T4,T5,T6 = st.tabs(["Overview", "Listening Stats", "Playlists", "Albums", "Saved Songs", "Artists"])


 # --------------OVERVIEW--------------
with T1:

    k1, k2, k3, k4 = st.columns([1,1,1,1], border=True)
    k1.metric("Saved Tracks", f"{(saved_tracks_df.shape)[0]:,}")
    k2.metric("Saved Albums", f"{(saved_albums_df.shape)[0]}")
    k3.metric("Playlists", f"{(playlists_df.shape)[0]}")
    k4.metric("Followed Artists", f"{(followed_artists_df.shape)[0]}")


    T1_col1, T1_col2 = st.columns([3, 1])

     # line graph 
    listening_hourly = recently_played_df.resample("d", on="played_at")["duration_minutes"].sum().rolling(3).mean().rename("All").reset_index()
    df_melted = listening_hourly.rename(columns={"All": "Total Listening Time (Minute)", "played_at" : "Day"})

    with T1_col1:
            with st.container(border=True):
                fig = px.line(
                    df_melted,
                    x="Day",
                    y="Total Listening Time (Minute)",
                    line_shape="spline",
                    title="Daily Listening Trend"
                )

                fig.update_traces(
                    line_color='#1DB954',
                    fillcolor='rgba(29, 185, 84, 0.3)',
                    mode="lines+markers",
                    marker=dict(color='#1DB954', size=6)
                )

                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    hovermode="x unified"
                )

                fig.update_layout( 
                    xaxis=dict(showspikes=False),  
                    yaxis=dict(showspikes=False)   
                )

                fig.update_traces(mode="lines+markers")
                fig.update_xaxes(showgrid=False)
                fig.update_xaxes(showline=False)
                fig.update_yaxes(showgrid=False)
                fig.update_yaxes(showticklabels=False)
                fig.update_layout(font=dict(family="CircularStd"))

                st.plotly_chart(fig, width='stretch', theme = None)

    
     # pie 
    with T1_col2:
         with st.container(border=True):
            genre_counts = genre_df["genre"].value_counts().reset_index()
            genre_counts.columns = ['genre', 'count']
            top_10 = genre_counts.head(10)
            others_count = genre_counts.iloc[10:]['count'].sum()
            
             # combining top 10 genres with other genres
            other_row = pd.DataFrame([{'genre': 'Other', 'count': others_count}])
            plot_df = pd.concat([top_10, other_row], ignore_index=True)

            pie_labels = plot_df.index
            pie_values = plot_df.values
            total_count = genre_df["genre"].nunique()
            

            fig = px.pie(
                plot_df,
                names="genre",
                values="count",
                color_discrete_sequence=spotify_palette,
                hole=0.8 
            )

            fig.add_annotation(
                text=f"{total_count:,} Genres",
                x=0.5,            
                y=0.5,             
                showarrow=False,
                font=dict(
                    size=18,
                    color="White"
                )
            ) 

            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                hovermode="x unified"
            )         

            fig.update_traces(
                textinfo='percent+label', 
                hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>"
            )

            fig.update_layout(
                xaxis=dict(showspikes=False),  
                yaxis=dict(showspikes=False)   
            )

            fig.update_traces(textinfo='none', rotation = 160) 
            fig.update_layout(title="Music Genres Overview")
            fig.update_layout(font=dict(family="CircularStd"))

            st.plotly_chart(fig, width="stretch", theme="streamlit")

    T1_col3, T1_col4 = st.columns([3, 1.5])

     # Bar Graph
    with T1_col3:
         with st.container(border=True):
            
            fig_bar = px.bar(
                tot_df,
                x="df",
                y="counts",
                title="Library at a Glance"
            )

            fig_bar.update_traces(
                    marker_color='#1DB954',  
                    marker_line_width=0      
                )
            
            fig_bar.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                hovermode="x unified"
            )

            fig_bar.update_layout(
                xaxis=dict(showspikes=False),  
                yaxis=dict(showspikes=False)   
            )
            
            fig_bar.update_xaxes(showgrid=False)
            fig_bar.update_yaxes(showgrid=False)
            fig_bar.update_yaxes(showline=False)
            fig_bar.update_xaxes(showline=False)
            fig_bar.update_yaxes(showticklabels=False)
            fig_bar.update_layout(font=dict(family="CircularStd"))

            st.plotly_chart(fig_bar, width="stretch", theme="streamlit", key = "overview line graph")

    with T1_col4:
            with st.container():
                table_data = ((recently_played_df.sort_values(by='played_at',ascending = False )).reset_index()).head(20)
                table_data = table_data[["name", "artist_name", "album_name"]]
                st.write("### Recently Played Tracks")
                st.dataframe(
                    table_data, 
                    use_container_width=True,
                    hide_index=True 
                )

 # ----------Map of Days----------
day_dict = {
        7 : "From Past Week",
        14 : "From Past Two Weeks",
        21 : "From Past Three Weeks",
        30 : "From Past Month",
        31 : "From Past Month" 
 }

 # -------------- LISTENING STATS ------------------
with T2:
    with st.expander("⚙ Filters"):
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                start_dt = st.date_input("From", value= None, min_value = min_date , max_value = max_date)
            with col_f2:
                end_dt = st.date_input("To", value= None , min_value = start_dt , max_value = max_date)
        
    if start_dt is not  None and end_dt is None: # only the start date
        filter = recently_played_df['played_at_date'] == start_dt 
        data = recently_played_df[filter]
        day_diff = 1
        prev_date = start_dt - pd.Timedelta(days=day_diff)

        sec_filter = ((recently_played_df['played_at_date'] >= prev_date) & (recently_played_df['played_at_date'] < start_dt))
        sec_data = recently_played_df[sec_filter]

    elif start_dt is not None and end_dt is not None:
        filter = ((recently_played_df['played_at_date'] >= start_dt) & (recently_played_df['played_at_date'] <= end_dt))
        data = recently_played_df[filter]
        day_diff = (end_dt - start_dt).days
        prev_date = start_dt - pd.Timedelta(days=day_diff)

        sec_filter = ((recently_played_df['played_at_date'] >= prev_date) & (recently_played_df['played_at_date'] < start_dt))
        sec_data = recently_played_df[sec_filter]
    
    else:
        filter = ((recently_played_df['played_at_date'] >= min_date) & (recently_played_df['played_at_date'] <= max_date))
        data = recently_played_df
        start_dt = data['played_at_date'].min()
        day_diff = (data['played_at_date'].max() - data['played_at_date'].min()).days
        prev_date = start_dt - pd.Timedelta(days=day_diff)
        sec_filter = ((recently_played_df['played_at_date'] >= prev_date) & (recently_played_df['played_at_date'] < start_dt))
        sec_data = recently_played_df[sec_filter]


    mess = day_dict.get(day_diff, f"From Past {day_diff} Days")

    
    # ------- songs played  -------
    songs_played = (data.shape)[0]
    prev_songs_played = ((songs_played - (sec_data.shape)[0]) / (sec_data.shape)[0])*100 if (sec_data.shape)[0] > 0 else 0

    # ------- Average Popularity -------
    average_popularity = data.loc[:, 'popularity'].mean()
    prev_avg_popularity = ((average_popularity - sec_data.loc[:, 'popularity'].mean()) / sec_data.loc[:, 'popularity'].mean()) * 100 if (sec_data.shape)[0] > 0 else np.nan

    # ------- listening minutes -------
    listening_minutes = data['duration_minutes'].sum() 
    prev_listening_minutes = ((listening_minutes - sec_data['duration_minutes'].sum()) / sec_data['duration_minutes'].sum()) * 100 if (sec_data.shape)[0] > 0 else np.nan

    # ------- Track duration -------
    avg_track_duration = data.loc[:,'duration_minutes'].mean()
    prev_avg_duration = ((avg_track_duration - sec_data.loc[:,'duration_minutes'].mean()) / sec_data.loc[:,'duration_minutes'].mean()) * 100 if (sec_data.shape)[0] > 0 else np.nan

    # ------- unique tracks -------
    unique_tracks = data['id'].nunique()
    prev_unique_tracks = ((unique_tracks - sec_data['id'].nunique()) / sec_data['id'].nunique()) * 100 if (sec_data.shape)[0] > 0 else np.nan

    # ------- unique artists -------
    unique_artists = data['artist_id'].nunique()
    prev_unique_artists = ((unique_artists - sec_data['artist_id'].nunique()) / sec_data['artist_id'].nunique()) * 100 if (sec_data.shape)[0] > 0 else np.nan


     # ------------KPI'S-------------
    k1, k3, k4, k5, k6 = st.columns([1, 1, 1, 1, 1], border = True)

    k1.metric("Songs Played", f"{songs_played}" , f"{prev_songs_played:.2f}% {mess}")
    k3.metric("Total Listening Time(Minutes)", f"{listening_minutes:.2f}", f"{prev_listening_minutes:.2f}%  {mess}")
    k4.metric("Average Track Duration", f"{avg_track_duration:.2f}", f"{prev_avg_duration:.2f}%  {mess}")
    k5.metric("Unique Tracks Played", f"{unique_tracks}", f"{prev_unique_tracks:.2f}%  {mess}")
    k6.metric("Unique Artists listened", f"{unique_artists}", f"{prev_unique_artists:.2f}%  {mess}")

    # ------------- Columns ------------
    T2_col1, T2_col2 = st.columns([3, 1])
    

    with T2_col1:
        with st.container(border=True):
            # ---------- Line Graph ----------
            if day_diff > 3:
                listening_hourly = data.resample("d", on="played_at")["duration_minutes"].sum().rolling(3).mean().rename("All").reset_index()
                df_melted = listening_hourly.rename(columns={"All": "Total Listening Time (Minute)", "played_at" : "Day"})

                fig = px.area(
                df_melted,
                x="Day",
                y="Total Listening Time (Minute)",
                line_shape="spline",
                title="Daily Listening Trend"
                )

                fig.update_traces(
                    line_color='#1DB954',
                    fillcolor='rgba(29, 185, 84, 0.3)',
                    mode="lines+markers",
                    marker=dict(color='#1DB954', size=6)
                )
                
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    hovermode="x unified"
                )

                fig.update_layout( 
                    xaxis=dict(showspikes=False),  
                    yaxis=dict(showspikes=False)   
                )

                fig.update_traces(mode="lines+markers")
                fig.update_xaxes(showgrid=False)
                fig.update_xaxes(showline=False)
                fig.update_yaxes(showgrid=False)
                fig.update_yaxes(showticklabels=False)
                fig.update_layout(font=dict(family="CircularStd"))

                st.plotly_chart(fig, width='stretch', theme="streamlit", key = "listening stats line Graph")

            if day_diff <= 3:

                listening_hourly = data.resample("h", on="played_at")["duration_minutes"].sum().rolling(3).mean().rename("All").reset_index()
                df_melted = listening_hourly.rename(columns={"All": "Total Listening Time (Minute)", "played_at" : "Time of Day"})

                fig = px.bar(
                df_melted,
                x="Time of Day",
                y="Total Listening Time (Minute)",
                title="Hourly Listening Trend"
                )

                fig.update_traces(
                    marker_color='#1DB954',  
                    marker_line_width=0      
                )
            
                fig.update_xaxes(showgrid=False)
                fig.update_yaxes(showgrid=False, showticklabels=False)
                fig.update_layout(
                    font=dict(family="CircularStd"),
                    xaxis_title=None,
                    yaxis_title=None,
                    bargap=0.2  
                )

                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    hovermode="x unified"
                )

                fig.update_layout( 
                    xaxis=dict(showspikes=False),  
                    yaxis=dict(showspikes=False)   
                )

                st.plotly_chart(fig, use_container_width=True, theme="streamlit", key="listening_stats_bar_chart")

                #------------ 
                 
                 





                 
                 
         
        
        