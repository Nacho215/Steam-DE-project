import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import os
import sys
from sqlalchemy import text
from streamlit_option_menu import option_menu
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.db import default_engine as engine

# Constant definitions
DIR_CLEAN_DATASETS = '../datasets/clean'
MAX_ROWS_PREVIEW_TABLES = 100


# Methods
def get_preview_tables(
    dir_csv_files: str,
    sample_size: int
) -> dict:
    # Tables info
    tables_info = {}
    # Look for csv files in given directory
    for filename in os.listdir(dir_csv_files):
        # Full file path
        file_path = f'{dir_csv_files}/{filename}'
        df_name = filename.split('.')[0]
        # Read only 'sample_size' random rows from each dataframe
        # or the entire ordered dataframe if it has
        # less than 'sample_size' rows
        df = pd.read_csv(file_path)
        if len(df) < sample_size:
            df_sample = df
        else:
            df_sample = df.sample(sample_size)
        # Store table name an data in a dictionary
        tables_info[df_name] = df_sample
    return tables_info


def get_unique_values_list(
    engine,
    column: str,
    table: str
) -> list:
    df_unique_values = pd.read_sql(
        text(
            f'SELECT DISTINCT {column} FROM {table};'
        ),
        engine.connect()
    )
    return [x[0] for x in df_unique_values.values]


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns
    Args:
        df (pd.DataFrame): Original dataframe
    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    _min,
                    _max,
                    (_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].str.contains(user_text_input)]

    return df


# Load preview tables
tables_info = get_preview_tables(
    DIR_CLEAN_DATASETS,
    MAX_ROWS_PREVIEW_TABLES
)
# Load tags list
tags_list = get_unique_values_list(
    engine=engine,
    column='tag',
    table='tags',
)
# Load developers list
languages_list = get_unique_values_list(
    engine=engine,
    column='language',
    table='languages',
)
# Load genres list
genres_list = get_unique_values_list(
    engine=engine,
    column='genre',
    table='genres',
)


# Page config
st.set_page_config(page_title='Steam Games Data', page_icon='video_game', layout='wide')
st.header(':video_game: Steam Games Data')

# Navigation Menu
with st.sidebar:
    selected = option_menu(
        "📋 Main Menu",
        [
            "🔍 Find your game!",
            "🎭 Genres",
            "📐 Tables structure"
        ],
        default_index=1
    )
# Tables structure Menu
if selected == "📐 Tables structure":
    st.header("📐 Tables structure")
    st.text("""These are the database tables structure.\nMax rows per table are capped at 100 for better performance.""")
    apps_container = st.container()
    c1, c2, c3 = st.columns(3)
    column_count = 1
    for table_name, table_data in tables_info.items():
        if table_name == 'apps':
            apps_container.subheader(f'{table_name} table')
            apps_container.dataframe(table_data)
        else:
            match column_count:
                case 1:
                    c1.subheader(f'{table_name} table')
                    c1.dataframe(table_data)
                case 2:
                    c2.subheader(f'{table_name} table')
                    c2.dataframe(table_data)
                case 3:
                    c3.subheader(f'{table_name} table')
                    c3.dataframe(table_data)
            column_count = (column_count + 1) if (column_count < 3) else 1
# Trending Menu
elif selected == "🔍 Find your game!":
    st.header("🔍 Find your game!")
    st.text("Here you can search for your ideal game!\nFilter for a genre, a developer you are fan of, a specifig language or maybe just a nice single player game with discount.")
    with st.container():
        # Columns
        c1, c2 = st.columns([3, 1])
        # Subheader
        c1.subheader("Filtered games")
        # Filters
        c2.subheader('Filters')
        n_games = c2.slider(
            'Top N° Games:',
            min_value=1,
            max_value=5000,
            value=2500,
            step=100
        )
        # Useful columns to sort by
        sort_columns_list = ['ccu', 'average_2weeks_hs', 'owners_max', 'price_usd', 'discount']
        order_by = c2.selectbox(
            'Sorted descending by:',
            sort_columns_list
        )
        selected_genres = c2.multiselect(
            'Only games with ANY of this genres:',
            genres_list
        )
        selected_languages = c2.multiselect(
            'Only games with ANY of this languages:',
            languages_list
        )
        selected_tags = c2.multiselect(
            'Only games with ANY of this tags:',
            tags_list
        )
        # Where clause
        where_clause = ''
        if selected_genres or selected_languages or selected_tags:
            where_clause = 'WHERE'
        if selected_genres:
            genres = ",".join(f"'{genre}'" for genre in selected_genres)
            where_clause += f''' apps.id_app IN (
                            SELECT DISTINCT
                                apps_genres.id_app
                            FROM
                                apps_genres
                                INNER JOIN genres ON apps_genres.id_genre = genres.id_genre
                            WHERE
                                genres.genre IN ({genres}))'''
        if selected_languages:
            if selected_genres:
                where_clause += ' AND'
            langs = ",".join(f"'{lang}'" for lang in selected_languages)
            where_clause += f''' apps.id_app IN (
                            SELECT DISTINCT
                                apps_languages.id_app
                            FROM
                                apps_languages
                                INNER JOIN languages ON apps_languages.id_language = languages.id_language
                            WHERE
                                languages.language IN ({langs}))'''
        if selected_tags:
            if selected_genres or selected_languages:
                where_clause += ' AND'
            tags = ",".join(f"'{tag}'" for tag in selected_tags)
            where_clause += f''' apps.id_app IN (
                            SELECT DISTINCT
                                apps_tags.id_app
                            FROM
                                apps_tags
                                INNER JOIN tags ON apps_tags.id_tag = tags.id_tag
                            WHERE
                                tags.tag IN ({tags}))'''
        # Query
        query = f"""
                    SELECT DISTINCT
                        apps.name AS game_name,
                        apps.developer,
                        apps.publisher,
                        apps.ccu,
                        apps.average_2weeks_hs,
                        apps.owners_max,
                        apps.price_usd,
                        apps.discount
                    FROM apps
                    {where_clause}
                    ORDER BY {order_by} DESC
                    LIMIT {n_games}
                    """
        try:
            # Query and plot the table
            df = pd.read_sql(
                text(query),
                engine.connect()
            )
            c1.dataframe(filter_dataframe(df))
        except Exception as e:
            c1.text(e)
        # Annotation
        c1.subheader("👇 Here you can post-filter on the query results")
# Genres Menu
elif selected == "🎭 Genres":
    with st.container():
        # Columns
        c1, c2 = st.columns([3, 1])
        # Filters
        c2.subheader('Filters')
        n_genres = c2.slider(
            'Top N° genres:',
            min_value=1,
            max_value=20,
            value=10,
            step=1
        )
        price_interval = c2.slider(
            'Price in USD:',
            min_value=0,
            max_value=1500,
            value=(0, 1500),
            step=5
        )
        # Where clause
        where_clause = 'WHERE'
        where_clause += F' a.price_usd BETWEEN {price_interval[0]} AND {price_interval[1]}'
        # MOST POPULAR GENRES
        c1.subheader('Most popular Genres')
        # Query
        query = f"""
                    SELECT g.genre, COUNT(ag.id_app) AS app_count
                    FROM genres g
                    INNER JOIN apps_genres ag ON g.id_genre = ag.id_genre
                    JOIN apps a ON a.id_app = ag.id_app
                    {where_clause}
                    GROUP BY g.genre
                    ORDER BY app_count DESC
                    LIMIT {n_genres};"""
        # Query the database and plot
        try:
            df = pd.read_sql(
                text(query),
                engine.connect()
            ).sort_values('app_count')
            chart = px.bar(df, x='app_count', y='genre', orientation='h')
            c1.plotly_chart(chart)
        except Exception as e:
            st.text(e)
        with st.container():
            # Filters
            st.subheader('Specific Genres Analysis')
            selected_genres = st.multiselect(
                'Show only this genres:',
                genres_list
            )
            # Columns
            c1, c2 = st.columns([2, 2])
            # OWNERS BY GENRE
            c1.subheader('Owners by Genre')
            # PRICE DISTRIBUTION BY GENRE
            c2.subheader('Price distribution by Genre')
            # Where clause
            where_clause = ''
            # Only plot if a genre is selected
            if selected_genres:
                genres = ",".join(f"'{genre}'" for genre in selected_genres)
                where_clause += f''' WHERE genres.genre IN ({genres})'''
                # Query
                query = f"""
                            SELECT genres.genre,
                            SUM(apps.owners_min) AS min_owners,
                            SUM(apps.owners_max) AS max_owners
                            FROM genres
                            JOIN apps_genres ON genres.id_genre = apps_genres.id_genre
                            JOIN apps ON apps.id_app = apps_genres.id_app
                            {where_clause}
                            GROUP BY genres.genre
                            ORDER BY max_owners DESC
                            LIMIT {n_genres};"""
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    # Configurar el gráfico
                    fig = go.Figure(data=[
                        go.Bar(name='Min Owners', x=df['genre'], y=df['min_owners']),
                        go.Bar(name='Max Owners', x=df['genre'], y=df['max_owners'])
                    ])
                    fig.update_layout(barmode='stack')#, height=500, width=800)
                    # Mostrar el gráfico
                    c1.plotly_chart(fig)
                except Exception as e:
                    c1.text(e)
                # Query
                query = f"""
                            SELECT genre, price_usd
                            FROM apps
                            JOIN apps_genres ON apps.id_app = apps_genres.id_app
                            JOIN genres ON apps_genres.id_genre = genres.id_genre
                            {where_clause}
                            """
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    boxplot = px.box(
                        df,
                        x='price_usd',
                        y='genre',
                        orientation='h'
                    )
                    c2.plotly_chart(boxplot)
                except Exception as e:
                    c2.text(e)
            else:
                c1.text('Please select at least 1 Genre')
                c2.text('Please select at least 1 Genre')
