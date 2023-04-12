import os
import sys
from tzlocal import get_localzone
from datetime import datetime
import pandas as pd
import plotly.express as px
from pandas.api.types import (is_categorical_dtype, is_datetime64_any_dtype,
                              is_numeric_dtype, is_object_dtype)
from sqlalchemy import text
from streamlit_option_menu import option_menu
import streamlit as st
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.db import default_engine as engine

# Constant definitions
MAX_ROWS_PREVIEW_TABLES = 100
plotly_color_palette = px.colors.sequential.Greens_r
FILTER_COLUMNS = ['peak_ccu_yesterday', 'average_2weeks_hs', 'owners_max', 'price_usd', 'discount']


# Methods
def get_preview_tables(
    engine,
    sample_size: int
) -> dict:
    """
    Query 'sample_size' rows from all tables in database
    and returns them on a dictionary.

    Args:
        engine (SqlAlchemy.Engine): Engine used to connect with database.
        sample_size (int): Maximum rows to retrieve.

    Returns:
        dict: Tables names as keys, and tables (dataFrames) as values.
    """
    # Tables info
    tables_info = {}
    # Get all table names
    result = pd.read_sql(
        text(
            """SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND table_type='BASE TABLE'
            ORDER BY table_name;"""
        ),
        engine.connect()
    )
    tables_names = [x[0] for x in result.values]
    # Get 'sample size' rows max for each table
    for table_name in tables_names:
        df_table = pd.read_sql(
            text(f"SELECT * FROM {table_name} LIMIT {sample_size};"),
            engine.connect()
        )
        tables_info[table_name] = df_table
    # Return tables names and samples in a dictionary
    return tables_info


def get_unique_values_list(
    engine,
    column: str,
    table: str
) -> list:
    """
    Get all unique values from a column in a given table.

    Args:
        engine (SqlAlchemy.Engine): Engine for database connection.
        column (str): column to retrieve unique values.
        table (str): table where column is located.

    Returns:
        list: unique values for that column in that table.
    """
    df_unique_values = pd.read_sql(
        text(
            f'SELECT DISTINCT {column} FROM {table} ORDER BY {column};'
        ),
        engine.connect()
    )
    return [x[0] for x in df_unique_values.values]


def get_last_update_message(
        engine,
        table_name: str
) -> str:
    """
    Get the timestamps of the last update made on given table.

    Args:
        engine (SqlAlchemy.Engine): Engine used to connect with database.
        table_name (str): name of the table to check for its last update

    Returns:
        str: timestamp in string format
    """
    # Get timestamp
    df_result = pd.read_sql(
        text(
            f"""
            SELECT pg_xact_commit_timestamp(xmin) AS last_update_timestamp
            FROM {table_name}
            ORDER BY last_update_timestamp
            LIMIT 1;"""
        ),
        engine.connect()
    )
    # Get user timezone
    user_tz = get_localzone()
    # Convert to datetime object
    last_update_time = datetime.strptime(
        str(df_result.iloc[0][0]),
        '%Y-%m-%d %H:%M:%S.%f%z'
    ).astimezone(user_tz)
    # Get time elapsed
    time_elapsed = datetime.now(user_tz) - last_update_time
    return f'Last database update: {time_elapsed.days}d {time_elapsed.seconds//3600}h {(time_elapsed.seconds//60)%60}m {time_elapsed.seconds%60}s ago'


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    # Format column names
    for col in df.columns:
        df.rename(columns={col: format_string_value(col)}, inplace=True)

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


def format_string_value(string: str) -> str:
    """
    Receive a string value and returns a
    more descriptive and user friendly version of it.

    Args:
        option (str): string value to transform.

    Returns:
        str: transformed string value.
    """
    match string:
        case 'peak_ccu_yesterday':
            return 'Peak CCU yesterday'
        case 'average_forever_hs':
            return 'Avg hs played forever'
        case 'average_2weeks_hs':
            return 'Avg hs played last 2 weeks'
        case 'median_forever_hs':
            return 'Median hs played forever'
        case 'median_2weeks_hs':
            return 'Median hs played last 2 weeks'
        case 'owners_min':
            return 'Minimum Owners'
        case 'owners_max':
            return 'Owners (max)'
        case 'price_usd':
            return 'Current US Price'
        case 'initial_price_usd':
            return 'Initial US Price'
        case 'apps_count':
            return 'Number of Apps'
        case 'avg_peak_ccu_yesterday':
            return 'Average peak CCU yesterday'
        case 'avg_2weeks_hs':
            return 'Average hours played last 2 weeks'
        case 'avg_owners_max':
            return 'Average Owners (Max)'
        case 'avg_price_usd':
            return 'Average Current Price in USD'
        case 'avg_discount':
            return 'Average Discount (%)'
        case _:
            return string.capitalize()


def show_glossary():
    """
    Show a simple glossary explaining most used words.
    """
    with st.expander("**Glossary**", False):
        st.markdown(
            """
            - **App**: Application. 'App' is used and not 'Game' because Steam have a lot of non-games applications.
            - **CCU**: Concurrent users
            - **Owners**: Number of users who have the app in their library. This data comes in interval format, that's why you may see "min" or "max" in some filters.
            - **Avg**: Average
            """
        )


# Load preview tables
tables_info = get_preview_tables(
    engine=engine,
    sample_size=MAX_ROWS_PREVIEW_TABLES
)
# Load tags list
tags_list = get_unique_values_list(
    engine=engine,
    column='tag',
    table='tags',
)
# Load languages list
languages_list = get_unique_values_list(
    engine=engine,
    column='normalized_language',
    table='languages',
)
# Load genres list
genres_list = get_unique_values_list(
    engine=engine,
    column='genre',
    table='genres',
)


# Get last update message
last_update_message = get_last_update_message(engine, 'apps')

# Page config
st.set_page_config(
    page_title='Steam Apps Data',
    page_icon='video_game',
    layout='wide'
)
# Title
st.markdown(
    "<h1 style='text-align: center;'>🎮 Steam Apps Data</h1>",
    unsafe_allow_html=True
)
# Hide Right menu and Footer
st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>"""
    , unsafe_allow_html=True
)


# Navigation Menu
with st.sidebar:
    # Option Menu
    selected = option_menu(
        menu_title="Main Menu",
        menu_icon='house',
        options=[
            "🔍 Find your App!",
            "🎭 Genres",
            "🈯 Languages",
            "🔖 Tags",
            "🔨 Database Structure"
        ],
        default_index=0,
        icons=[],
        styles={
            "container": {"padding": "5!important", "background-color": "#262730"},
            "icon": {"color": "white", "font-size": "25px"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#0E1117"},
            "nav-link-selected": {"background-color": "#306f34"},
        }
    )
    # Last database update information
    st.divider()
    st.markdown(
        f"""
        <div class="footer">
        <p style='text-align: center;'>{last_update_message}</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    # Developer information
    st.divider()
    st.markdown(
        """
        <div class="footer">
        <p style='text-align: center;'>Made with 💚 by <a href="https://github.com/Nacho215/" target="_blank">Nacho 215</a></p>
        </div>
        """,
        unsafe_allow_html=True
    )

# Find your App! Menu
if selected == "🔍 Find your App!":
    # Title and description
    st.markdown(
            """
            ## 🔍 Find your App!
            \nIn this section, you can search for your ideal App! You may filter for:
            \n- a **genre** you love
            \n- a **developer** you are fan of
            \n- a specific **language**
            \n- or maybe just a nice single player game with **discount**
            """
    )
    # Show glossary
    show_glossary()

    with st.container():
        # Filters
        st.subheader('🎚️ Filters')
        # First row
        c1, c2 = st.columns(2)
        n_games = c1.slider(
            'Top N° Apps:',
            min_value=1,
            max_value=5000,
            value=2500,
            step=100
        )
        order_by = c2.selectbox(
            'Sorted descending by:',
            FILTER_COLUMNS,
            format_func=format_string_value
        )
        # Second row
        c1, c2, c3 = st.columns(3)
        selected_genres = c1.multiselect(
            'Genres:',
            genres_list
        )
        selected_languages = c2.multiselect(
            'Languages:',
            languages_list
        )
        selected_tags = c3.multiselect(
            'Tags:',
            tags_list
        )
        # Subheader
        st.subheader("📄 Filtered Apps")
        # Where clause
        where_clause = ''
        if selected_genres or selected_languages or selected_tags:
            where_clause = 'WHERE'
        if selected_genres:
            genres = ",".join(f"'{genre}'" for genre in selected_genres)
            where_clause += f'''
                apps.id_app IN (
                    SELECT apps_genres.id_app
                    FROM apps_genres
                    INNER JOIN genres ON apps_genres.id_genre = genres.id_genre
                    WHERE genres.genre IN ({genres})
                    GROUP BY apps_genres.id_app
                    HAVING COUNT(DISTINCT genres.genre) = {len(selected_genres)}
                )'''
        if selected_languages:
            if selected_genres:
                where_clause += ' AND'
            langs = ",".join(f"'{lang}'" for lang in selected_languages)
            where_clause += f'''
                apps.id_app IN (
                    SELECT DISTINCT apps.id_app
                    FROM apps
                    JOIN apps_languages ON apps.id_app = apps_languages.id_app
                    JOIN languages ON apps_languages.id_language = languages.id_language
                    WHERE languages.normalized_language IN ({langs})
                    GROUP BY apps.id_app
                    HAVING COUNT(DISTINCT languages.id_language) = {len(selected_languages)}
                )'''
        if selected_tags:
            if selected_genres or selected_languages:
                where_clause += ' AND'
            tags = ",".join(f"'{tag}'" for tag in selected_tags)
            where_clause += f'''
                apps.id_app IN (
                    SELECT apps_tags.id_app
                    FROM apps_tags
                    INNER JOIN tags ON apps_tags.id_tag = tags.id_tag
                    WHERE tags.tag IN ({tags})
                    GROUP BY apps_tags.id_app
                    HAVING COUNT(DISTINCT tags.tag) = {len(selected_tags)}
                )'''
        # Query
        query = f"""
                    SELECT DISTINCT
                        apps.name,
                        apps.developer,
                        apps.publisher,
                        apps.peak_ccu_yesterday,
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
            # Query
            df = pd.read_sql(
                text(query),
                engine.connect()
            )
            # Show filter and formatted table
            # st.subheader("👇 Here you can post-filter on the query results")
            st.dataframe(filter_dataframe(df))
        except Exception as e:
            st.text(e)
# Genres Menu
elif selected == "🎭 Genres":
    # Title and description
    st.markdown(
        """
        ## 🎭 Genres Analysis
        Here you can explore which are the most popular **genres** based on some filter.
        In addition, you can do a **specific genre** analysis.
        """
    )
    # Show glossary
    show_glossary()

    with st.container():
        # Columns
        c1, c2 = st.columns([3, 1])
        # Filters
        c2.subheader('🎚️ Filters')
        n_genres = c2.slider(
            'Top N° genres:',
            min_value=1,
            max_value=20,
            value=10,
            step=1
        )
        criteria = c2.selectbox(
            'Based on:',
            options=['apps_count'] + FILTER_COLUMNS,
            format_func=format_string_value
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
        where_clause += f' a.price_usd BETWEEN {price_interval[0]} AND {price_interval[1]}'
        # Change some query parts based on chosen criteria
        select_column = ''
        order_by = ''
        match criteria:
            case 'apps_count':
                select_column = 'COUNT(a.id_app) as apps_count'
                order_by = 'apps_count'
            case 'peak_ccu_yesterday':
                select_column = 'AVG(a.peak_CCU_yesterday) as avg_peak_ccu_yesterday'
                order_by = 'avg_peak_ccu_yesterday'
            case 'average_2weeks_hs':
                select_column = 'AVG(a.average_2weeks_hs) as avg_2weeks_hs'
                where_clause += ' AND a.average_2weeks_hs > 0'
                order_by = 'avg_2weeks_hs'
            case 'owners_max':
                select_column = 'AVG(a.owners_max) as avg_owners_max'
                order_by = 'avg_owners_max'
            case 'price_usd':
                select_column = 'AVG(a.price_usd) as avg_price_usd'
                where_clause += ' AND a.price_usd > 0'
                order_by = 'avg_price_usd'
            case 'discount':
                select_column = 'AVG(a.discount) as avg_discount'
                order_by = 'avg_discount'
        # MOST POPULAR GENRES
        c1.subheader('🏅 Most popular Genres')
        # Query
        query = f"""
                    SELECT g.genre, {select_column}
                    FROM genres g
                    INNER JOIN apps_genres ag ON g.id_genre = ag.id_genre
                    JOIN apps a ON a.id_app = ag.id_app
                    {where_clause}
                    GROUP BY g.genre
                    ORDER BY {order_by} DESC
                    LIMIT {n_genres};"""
        # Query the database and plot
        try:
            df = pd.read_sql(
                text(query),
                engine.connect()
            )
            df.sort_values(df.columns[1], inplace=True)
            x_label = select_column.split()[-1]
            fig = px.bar(
                df,
                x=x_label,
                y='genre',
                orientation='h',
                color_discrete_sequence=plotly_color_palette,
                labels={
                    'apps_count': 'Number of Apps',
                    'avg_peak_ccu_yesterday': 'Average peak CCU yesterday',
                    'avg_2weeks_hs': 'Average hours played last 2 weeks',
                    'avg_owners_max': 'Average Owners (Max)',
                    'avg_price_usd': 'Average Current Price in USD',
                    'avg_discount': 'Average Discount (%)'
                }
            )
            fig.update_layout(yaxis_title=None)
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br><br>' +
                        format_string_value(x_label) + ': %{value}<br>',
            )
            c1.plotly_chart(fig)
        except Exception as e:
            c1.text(e)
        # Specific Genre Analysis
        st.divider()
        with st.container():
            # Filters
            st.subheader('🎯 Specific Genre Analysis')
            selected_genre = st.selectbox(
                'Genre:',
                genres_list
            )
            # Columns
            c1, c2 = st.columns([2, 2])
            # Subheaders
            sh1 = c1.subheader('🔖 Top 10 tags for this Genre')
            sh2 = c2.subheader('💰 Free vs Paid apps for this Genre')
            # Where clause
            where_clause = ''
            # Only plot if a genre is selected
            if selected_genre:
                # TOP 10 TAGS FOR THIS GENRE
                query = f"""
                            SELECT t.tag, COUNT(*) as num_apps
                            from tags t
                            JOIN apps_tags at2  ON t.id_tag  = at2.id_tag
                            where at2.id_app in (
                                select ag.id_app
                                from apps a
                                join apps_genres ag on ag.id_app = a.id_app
                                join genres g on g.id_genre = ag.id_genre
                                where g.genre  = '{selected_genre}'
                            )
                            GROUP BY t.tag
                            ORDER BY num_apps DESC
                            LIMIT 10;"""
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    fig = px.bar(
                        df,
                        x='tag',
                        y='num_apps',
                        orientation='v',
                        color_discrete_sequence=plotly_color_palette,
                        labels={
                            'num_apps': 'Number of Apps',
                            'tag': 'Tag'
                        }
                    )
                    fig.update_layout(
                        xaxis_title=None,
                        showlegend=False
                    )
                    # Customize text
                    fig.update_traces(
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>'
                    )
                    c1.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh1.subheader(f'🔖 Top 10 tags for {selected_genre} Genre')
                except Exception as e:
                    c1.text(e)
                # FREE VS PAID APPS FOR THIS GENRE
                query = f"""
                            SELECT
                                SUM(CASE WHEN a.price_usd = 0 THEN 1 ELSE 0 END) AS free_apps,
                                SUM(CASE WHEN a.price_usd > 0 THEN 1 ELSE 0 END) AS paid_apps
                            FROM
                                apps AS a
                            JOIN
                                apps_genres AS ag ON a.id_app = ag.id_app
                            JOIN
                                genres AS g ON ag.id_genre = g.id_genre
                            WHERE
                                g.genre = '{selected_genre}'
                            GROUP BY
                                g.genre;
                            """
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    # Pie chart
                    labels = ['Free', 'Paid']
                    values = df.values[0]
                    fig = px.pie(
                        values=values,
                        names=labels,
                        hole=.4,
                        color_discrete_sequence=plotly_color_palette
                    )
                    # Customize text
                    fig.update_traces(
                        textposition='inside',
                        textinfo='label+percent',
                        textfont_size=20,
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>' +
                                'Percentage: %{percent:.2%}<br>'
                    )
                    fig.update_layout(showlegend=False)
                    c2.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh2.subheader(f'💰 Free vs Paid apps for {selected_genre} Genre')
                except Exception as e:
                    c2.text(e)
# Languages Menu
elif selected == "🈯 Languages":
    # Title and description
    st.markdown(
        """
        ## 🈯 Languages Analysis
        In this section you can see in which **languages** are available the most popular Apps.
        In addition, you can do a **specific language** analysis.
        """
    )
    # Show glossary
    show_glossary()

    with st.container():
        # Columns
        c1, c2 = st.columns([3, 1])
        # MOST POPULAR LANGUAGES
        c1.subheader('🏅 Most popular Languages')
        # Filters
        c2.subheader('🎚️ Filters')
        n_languages = c2.slider(
            'Top N° languages:',
            min_value=1,
            max_value=20,
            value=10,
            step=1
        )
        criteria = c2.selectbox(
            'Based on:',
            options=['apps_count'] + FILTER_COLUMNS,
            format_func=format_string_value
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
        where_clause += f' a.price_usd BETWEEN {price_interval[0]} AND {price_interval[1]}'
        # Change some query parts based on chosen criteria
        select_column = ''
        order_by = ''
        match criteria:
            case 'apps_count':
                select_column = 'COUNT(a.id_app) as apps_count'
                order_by = 'apps_count'
            case 'peak_ccu_yesterday':
                select_column = 'AVG(a.peak_CCU_yesterday) as avg_peak_ccu_yesterday'
                order_by = 'avg_peak_ccu_yesterday'
            case 'average_2weeks_hs':
                select_column = 'AVG(a.average_2weeks_hs) as avg_2weeks_hs'
                where_clause += ' AND a.average_2weeks_hs > 0'
                order_by = 'avg_2weeks_hs'
            case 'owners_max':
                select_column = 'AVG(a.owners_max) as avg_owners_max'
                order_by = 'avg_owners_max'
            case 'price_usd':
                select_column = 'AVG(a.price_usd) as avg_price_usd'
                where_clause += ' AND a.price_usd > 0'
                order_by = 'avg_price_usd'
            case 'discount':
                select_column = 'AVG(a.discount) as avg_discount'
                order_by = 'avg_discount'
        # Query
        query = f"""
                    SELECT l.normalized_language, {select_column}
                    FROM languages l
                    INNER JOIN apps_languages al ON l.id_language = al.id_language
                    JOIN apps a ON a.id_app = al.id_app
                    {where_clause}
                    GROUP BY l.normalized_language
                    ORDER BY {order_by} DESC
                    LIMIT {n_languages};"""
        # Query the database and plot
        try:
            df = pd.read_sql(
                text(query),
                engine.connect()
            )
            df.sort_values(df.columns[1], inplace=True)
            x_label = select_column.split()[-1]
            fig = px.bar(
                df,
                x=x_label,
                y='normalized_language',
                orientation='h',
                color_discrete_sequence=plotly_color_palette,
                labels={
                    'apps_count': 'Number of Apps',
                    'avg_peak_ccu_yesterday': 'Average peak CCU yesterday',
                    'avg_2weeks_hs': 'Average hours played last 2 weeks',
                    'avg_owners_max': 'Average Owners (Max)',
                    'avg_price_usd': 'Average Current Price in USD',
                    'avg_discount': 'Average Discount (%)'
                }
            )
            fig.update_layout(yaxis_title=None)
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br><br>' +
                        format_string_value(x_label) + ': %{value}<br>',
            )
            c1.plotly_chart(fig)
        except Exception as e:
            c1.text(e)
        # Specific Language Analysis
        st.divider()
        with st.container():
            # Filters
            st.subheader('🎯 Specific Language Analysis')
            selected_language = st.selectbox(
                'Language:',
                languages_list
            )
            # Columns
            c1, c2 = st.columns(2)
            # Subheaders
            sh1 = c1.subheader('🎭 Top 10 Genres for this Language')
            sh2 = c2.subheader('💰 Free vs Paid apps for this Language')
            # Only plot if a language is selected
            if selected_language:
                # TOP 10 GENRES FOR THIS LANGUAGE
                query = f"""
                            SELECT genres.genre, COUNT(*) as num_apps
                            from genres
                            JOIN apps_genres ON genres.id_genre = apps_genres.id_genre
                            where apps_genres.id_app in (
                                select al.id_app
                                from apps a
                                join apps_languages al on al.id_app = a.id_app
                                join languages l on l.id_language = al.id_language 
                                where l.normalized_language ='{selected_language}'
                            )
                            GROUP BY genres.genre
                            ORDER BY num_apps DESC
                            LIMIT 10;"""
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    fig = px.bar(
                        df,
                        x='genre',
                        y='num_apps',
                        orientation='v',
                        color_discrete_sequence=plotly_color_palette,
                        labels={
                            'num_apps': 'Number of Apps',
                            'genre': 'Genre'
                        }
                    )
                    fig.update_layout(
                        xaxis_title=None,
                        showlegend=False
                    )
                    # Customize text
                    fig.update_traces(
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>'
                    )
                    c1.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh1.subheader(f'🎭 Top 10 Genres for {selected_language} Language')
                except Exception as e:
                    c1.text(e)
                # FREE VS PAID APPS FOR THIS GENRE
                query = f"""
                            SELECT
                                SUM(CASE WHEN a.price_usd = 0 THEN 1 ELSE 0 END) AS free_apps,
                                SUM(CASE WHEN a.price_usd > 0 THEN 1 ELSE 0 END) AS paid_apps
                            FROM
                                apps AS a
                            JOIN
                                apps_languages AS al ON a.id_app = al.id_app
                            JOIN
                                languages AS l ON al.id_language = l.id_language
                            WHERE
                                l.normalized_language ='{selected_language}'
                            GROUP BY
                                l.normalized_language;
                            """
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    # Pie chart
                    labels = ['Free', 'Paid']
                    values = df.values[0]
                    fig = px.pie(
                        values=values,
                        names=labels,
                        hole=.4,
                        color_discrete_sequence=plotly_color_palette
                    )
                    # Customize text
                    fig.update_traces(
                        textposition='inside',
                        textinfo='label+percent',
                        textfont_size=20,
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>' +
                                'Percentage: %{percent:.2%}<br>'
                    )
                    fig.update_layout(showlegend=False)
                    c2.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh2.subheader(f'💰 Free vs Paid apps for {selected_language} Language')
                except Exception as e:
                    c2.text(e)
# Tags Menu
elif selected == "🔖 Tags":
    # Title and description
    st.markdown(
            """
            ## 🔖 Tags Analysis
            \nTags can be applied to a app by the developer, by players with non-limited accounts, and by Steam moderators.
            \nThis allows the community to help mark up Apps with the terms, themes, and genres that help describe the app to others. (See more info: [here](https://partner.steamgames.com/doc/store/tags))
            \nIn this section you can explore the **Most Popular Tags** based on different criterias.
            You can also do a **specific tag** analysis.
            """
    )
    # Show glossary
    show_glossary()

    with st.container():
        # Columns
        c1, c2 = st.columns([3, 1])
        # MOST POPULAR TAGS
        c1.subheader('🏅 Most popular Tags')
        # Filters
        c2.subheader('🎚️ Filters')
        n_tags = c2.slider(
            'Top N° tags:',
            min_value=1,
            max_value=20,
            value=10,
            step=1
        )
        criteria = c2.selectbox(
            'Based on:',
            options=['apps_count'] + FILTER_COLUMNS,
            format_func=format_string_value
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
        # Change some query parts based on chosen criteria
        select_column = ''
        order_by = ''
        match criteria:
            case 'apps_count':
                select_column = 'COUNT(a.id_app) as apps_count'
                order_by = 'apps_count'
            case 'peak_ccu_yesterday':
                select_column = 'AVG(a.peak_CCU_yesterday) as avg_peak_ccu_yesterday'
                order_by = 'avg_peak_ccu_yesterday'
            case 'average_2weeks_hs':
                select_column = 'AVG(a.average_2weeks_hs) as avg_2weeks_hs'
                where_clause += ' AND a.average_2weeks_hs > 0'
                order_by = 'avg_2weeks_hs'
            case 'owners_max':
                select_column = 'AVG(a.owners_max) as avg_owners_max'
                order_by = 'avg_owners_max'
            case 'price_usd':
                select_column = 'AVG(a.price_usd) as avg_price_usd'
                where_clause += ' AND a.price_usd > 0'
                order_by = 'avg_price_usd'
            case 'discount':
                select_column = 'AVG(a.discount) as avg_discount'
                order_by = 'avg_discount'
        # Query
        query = f"""
                    SELECT t.tag, {select_column}
                    FROM tags t
                    INNER JOIN apps_tags at ON t.id_tag = at.id_tag
                    JOIN apps a ON a.id_app = at.id_app
                    {where_clause}
                    GROUP BY t.tag
                    ORDER BY {order_by} DESC
                    LIMIT {n_tags};"""
        # Query the database and plot
        try:
            df = pd.read_sql(
                text(query),
                engine.connect()
            )
            df.sort_values(df.columns[1], inplace=True)
            x_label = select_column.split()[-1]
            fig = px.bar(
                df,
                x=x_label,
                y='tag',
                orientation='h',
                color_discrete_sequence=plotly_color_palette,
                labels={
                    'apps_count': 'Number of Apps',
                    'avg_peak_ccu_yesterday': 'Average peak CCU yesterday',
                    'avg_2weeks_hs': 'Average hours played last 2 weeks',
                    'avg_owners_max': 'Average Owners (Max)',
                    'avg_price_usd': 'Average Current Price in USD',
                    'avg_discount': 'Average Discount (%)'
                }
            )
            fig.update_layout(yaxis_title=None)
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br><br>' +
                        format_string_value(x_label) + ': %{value}<br>',
            )
            c1.plotly_chart(fig)
        except Exception as e:
            st.text(e)
        # Specific Language Analysis
        st.divider()
        with st.container():
            # Filters
            st.subheader('🎯 Specific Tag Analysis')
            selected_tag = st.selectbox(
                'Tag:',
                tags_list
            )
            # Columns
            c1, c2 = st.columns(2)
            # Subheaders
            sh1 = c1.subheader('🎭 Top 10 Genres for this Tag')
            sh2 = c2.subheader('💰 Price distribution for this Tag')
            # Only plot if a tag is selected
            if selected_tag:
                # TOP 10 GENRES FOR THIS TAG
                query = f"""
                            SELECT genres.genre, COUNT(*) as num_apps
                            from genres
                            JOIN apps_genres ON genres.id_genre = apps_genres.id_genre
                            where apps_genres.id_app in (
                                select at.id_app
                                from apps a
                                join apps_tags at on at.id_app = a.id_app
                                join tags t on t.id_tag = at.id_tag
                                where t.tag ='{selected_tag}'
                            )
                            GROUP BY genres.genre
                            ORDER BY num_apps DESC
                            LIMIT 10;"""
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    fig = px.bar(
                        df,
                        x='genre',
                        y='num_apps',
                        orientation='v',
                        color_discrete_sequence=plotly_color_palette,
                        labels={
                            'num_apps': 'Number of Apps',
                            'genre': 'Genre'
                        }
                    )
                    fig.update_layout(
                        xaxis_title=None,
                        showlegend=False
                    )
                    # Customize text
                    fig.update_traces(
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>'
                    )
                    c1.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh1.subheader(f'🎭 Top 10 Genres for {selected_tag} tag')
                except Exception as e:
                    c1.text(e)
                # FREE VS PAID APPS FOR THIS TAG
                query = f"""
                            SELECT
                                SUM(CASE WHEN a.price_usd = 0 THEN 1 ELSE 0 END) AS free_apps,
                                SUM(CASE WHEN a.price_usd > 0 THEN 1 ELSE 0 END) AS paid_apps
                            FROM
                                apps AS a
                            JOIN
                                apps_tags AS at ON a.id_app = at.id_app
                            JOIN
                                tags AS t ON at.id_tag = t.id_tag
                            WHERE
                                t.tag ='{selected_tag}'
                            GROUP BY
                                t.tag;
                            """
                # Query the database and plot
                try:
                    df = pd.read_sql(
                        text(query),
                        engine.connect()
                    )
                    # Pie chart
                    labels = ['Free', 'Paid']
                    values = df.values[0]
                    fig = px.pie(
                        values=values,
                        names=labels,
                        hole=.4,
                        color_discrete_sequence=plotly_color_palette
                    )
                    # Customize text
                    fig.update_traces(
                        textposition='inside',
                        textinfo='label+percent',
                        textfont_size=20,
                        hovertemplate='<b>%{label}</b><br><br>' +
                                'Number of Apps: %{value}<br>' +
                                'Percentage: %{percent:.2%}<br>'
                    )
                    fig.update_layout(showlegend=False)
                    c2.plotly_chart(fig, use_container_width=True)
                    # Update subheader
                    sh2.subheader(f'💰 Free vs Paid apps for {selected_tag} Tag')
                except Exception as e:
                    c2.text(e)
# Tables structure Menu
elif selected == "🔨 Database Structure":
    # Title and description
    st.markdown(
        """
        ## 🔨 Database Structure
        \nThese are the database tables structure.
        \nMax rows per table are capped at 100 for better performance.
        """
    )

    # Create a container for apps table
    # and 3 columns for other tables
    apps_container = st.container()
    c1, c2, c3 = st.columns([1, 1.5, 1], gap="large")
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
                    # Contemplate wide tables
                    wide_table = True if len(table_data.columns) > 2 else False
                    c2.dataframe(table_data, use_container_width=wide_table)
                case 3:
                    c3.subheader(f'{table_name} table')
                    c3.dataframe(table_data)
            column_count = (column_count + 1) if (column_count < 3) else 1
