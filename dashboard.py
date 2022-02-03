import streamlit as st
from streamlit_echarts import st_pyecharts
import pandas as pd
import psycopg2
import configparser


def connect_to_database(config):
    """
    Connect to the Redshift database.
    """
    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = conn.cursor()
    return conn, cursor


def query_top_played_song_location(cursor) -> pd.DataFrame:
    """
    Query the top played song location.
    """
    # Query the top played song location
    cursor.execute("""
        SELECT s.title, sp.location, count(sp.songplay_id) as plays
        FROM songplays sp
        JOIN songs s on sp.song_id = s.song_id
        GROUP BY s.title, sp.location
        ORDER BY plays desc
        LIMIT 5;
        """)
    # Fetch the results
    results = cursor.fetchall()
    # Convert the results to a pandas dataframe
    df = pd.DataFrame(results, columns=['Song Title', 'Location', 'Plays'])
    return df

def query_top_played_songs(cursor) -> pd.DataFrame:
    """
    Query the top played songs.
    """
    # Query the top played songs
    cursor.execute("""
        SELECT s.title, count(sp.songplay_id) as plays
        FROM songplays sp
        JOIN songs s on sp.song_id = s.song_id
        GROUP BY s.title
        ORDER BY plays desc
        LIMIT 5;
        """)
    # Fetch the results
    results = cursor.fetchall()
    # Convert the results to a pandas dataframe
    df = pd.DataFrame(results, columns=['Song Title', 'Plays'])
    return df


def query_top_played_artists(cursor) -> pd.DataFrame:
    """
    Query the top played artists.
    """
    # Query the top played artists
    cursor.execute("""
        SELECT a.artist_name, count(sp.songplay_id) as plays
        FROM songplays sp
        JOIN artists a on sp.artist_id = a.artist_id
        GROUP BY a.artist_name
        ORDER BY plays desc
        LIMIT 5;
        """)
    # Fetch the results
    results = cursor.fetchall()
    # Convert the results to a pandas dataframe
    df = pd.DataFrame(results, columns=['Artist Name', 'Plays'])
    return df


def query_most_played_song_each_day_month(cursor) -> pd.DataFrame:
    """
    Query the most played song each day, month.
    """
    # Query the most played song each day, month
    cursor.execute("""
        SELECT date_trunc('day', start_time) as day, date_trunc('month', start_time) as month, s.title, count(sp.songplay_id) as plays
        FROM songplays sp
        JOIN songs s on sp.song_id = s.song_id
        GROUP BY day, month, s.title
        ORDER BY plays desc
        LIMIT 5;
        """)
    # Fetch the results
    results = cursor.fetchall()
    # Convert the results to a pandas dataframe
    df = pd.DataFrame(results, columns=['Day', 'Month', 'Song Title', 'Plays'])
    return df

# Read the config file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Create a connection to the database
conn, cursor = connect_to_database(config)

# Query top played songs
df_top_played_songs = query_top_played_songs(cursor)

# Display dashboard title
st.title('Music Streaming Analytics')
# Display dashboard description
st.markdown('This dashboard shows the top played songs, artists, and locations. '
            'This dashboard is built using Streamlit.')

# Display top played songs and draw a bar chart
st.header('Top Played Songs')
figure, table = st.columns((3, 2))
with table:
    st.dataframe(df_top_played_songs, width=500, height=300)
with figure:
    st.bar_chart(df_top_played_songs.set_index('Song Title'), width=1000, height=500, use_container_width=True)

# Query top played artists
df_top_played_artists = query_top_played_artists(cursor)

# Display top played artists and draw a bar chart
st.header("Most Played Artists")
figure, table = st.columns((3, 2))
with table:
    st.dataframe(df_top_played_artists, width=500, height=300)
with figure:
    st.bar_chart(df_top_played_artists.set_index('Artist Name'), width=1000, height=500, use_container_width=True)

# Display a pie chart of the top played artists

# Query top played song location
df_top_played_song_location = query_top_played_song_location(cursor)

# Display the top played song location
st.header('Top Played Song Location')
st.write(df_top_played_song_location)

# Display footer
st.markdown('**Created by**: [@erick_escobar](https://www.linkedin.com/in/erick-escobar-892b20103/)')



