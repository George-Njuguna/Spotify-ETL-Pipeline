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


# Cache the connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('postgre_account'),
        password=os.getenv('postgre_password'),
        host=os.getenv('host'),
        port=os.getenv('port')
    )

# Cache the data load
@st.cache_data(ttl=300)

def load_cached_data(table):
    conn = get_connection()   
    return import_data(table, conn)

# Tables
playlists_df =load_cached_data("playlist_data")
followed_artists_df =load_cached_data("followed_artists_data")
saved_albums_df =load_cached_data("saved_albums_data")
recently_played_df =load_cached_data("recently_played_data")
top_artists_df =load_cached_data("top_artists_data")
top_tracks_df =load_cached_data("top_tracks_data")

