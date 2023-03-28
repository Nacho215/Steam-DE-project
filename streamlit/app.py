import streamlit as st
import pandas as pd
import plotly.express as px
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


# Load preview tables
tables_info = get_preview_tables(
    DIR_CLEAN_DATASETS,
    MAX_ROWS_PREVIEW_TABLES
)

# Page config
st.set_page_config(page_title='Steam Games Data', page_icon='video_game', layout='wide')
st.header(':video_game: Steam Games Data')

# Navigation Menu
with st.sidebar:
    selected = option_menu(
        "Main Menu",
        ["Data Visualization", 'Tables structure'],
        icons=['bar-chart', 'wrench'],
        menu_icon="cast",
        default_index=1
    )
# Tables structure Menu
if selected == "Tables structure":
    for table_name, table_data in tables_info.items():
        st.subheader(f'{table_name} table')
        st.dataframe(table_data)
# Visualization Menu
elif selected == 'Data Visualization':
    # Top 10 games by peak ccu
    with st.container():
        subheader = st.subheader('')
        a1, a2 = st.columns([3, 1])
        try:
            # Filters
            n_games = a2.slider(
                'Top N° games:',
                min_value=1,
                max_value=20,
                value=10,
                step=1
            )
            filter_category = a2.selectbox(
                'Filter by:',
                ['ccu', 'owners_max']
            )
            # Update subheader
            subheader.subheader(f'Top {n_games} games with higher {filter_category}')
            # Build query
            query = 'select "name", {0} from apps order by {0} desc limit {1};'.format(
                filter_category,
                n_games
            )
            # Query the database
            df_results = pd.read_sql(text(query), engine.connect())
            # Plot bar chart with results
            bar_chart = px.bar(
                df_results.sort_values(filter_category),
                y='name',
                x=filter_category,
                color_discrete_sequence=['#F63366']*len(df_results),
                template='plotly_white',
            )
            a1.plotly_chart(bar_chart)
        except Exception as e:
            st.text(e)
