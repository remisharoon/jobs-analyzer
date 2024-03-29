from sqlalchemy import create_engine
import streamlit as st
import pandas as pd

connection_string = st.secrets.PostgresDB.connection_string
conn = create_engine(connection_string)

# @st.cache(allow_output_mutation=True)
@st.cache_data(ttl=3600)
def load_dataframe(sql='select * from ja_jobs_raw'):
    SQL_Query = pd.read_sql(sql, conn)
    df = pd.DataFrame(SQL_Query)
    df['date_posted'] = pd.to_datetime(df['date_posted'],errors='coerce')  # Use 'coerce' to handle any invalid parsing as NaT
    df = df.dropna(subset=['date_posted'])  # Optional: Remove rows where conversion failed

    return df
