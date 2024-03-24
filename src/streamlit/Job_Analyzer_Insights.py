import streamlit as st
import pandas as pd
from jobs_data import load_dataframe
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

st.set_page_config(
    page_title="Job Listings Dashboard",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.linkedin.com/in/remisharoon/',
        # 'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)



st.write("Hello Jobs Analyzer")

df = load_dataframe()

location = st.sidebar.multiselect('Location', options=df['location'].unique())
if location:
    df_filtered = df[df['location'].isin(location)]
else:
    df_filtered = df

st.write(df_filtered)


df