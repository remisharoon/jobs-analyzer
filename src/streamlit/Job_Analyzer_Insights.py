import streamlit as st
import pandas as pd
from jobs_data import load_dataframe
import sys
import os
import altair as alt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

st.set_page_config(
    page_title="Job Listings Dashboard",
    page_icon="./assets/images/appiconset/16.png",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.linkedin.com/in/remisharoon/',
        # 'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

# A function to return HTML string for each row
def make_clickable(link, name="Link"):
    return f'<a target="_blank" href="{link}">{name}</a>'


# st.write("Jobs Analyzer")

df = load_dataframe()

df = df[['date_posted','title', 'company', 'location', 'desired_tech_skills_inferred','desired_soft_skills_inferred',
            'desired_domain_skills_inferred','domains_inferred', 'job_url','country_inferred']]

df = df.rename(columns={
    'date_posted': 'Date Posted',
    'title': 'Title',
    'company': 'Company',
    'location': 'Location',
    'desired_tech_skills_inferred': 'Desired Tech Skills',
    'desired_soft_skills_inferred': 'Desired Soft Skills',
    'desired_domain_skills_inferred': 'Desired Domain Skills',
    'domains_inferred': 'Domains',
    'job_url': 'Job URL',
    'country_inferred': 'Country'
})

df = df.sort_values(by='Date Posted', ascending=False)

# Apply this function to your job_url column
# df['Job URL'] = df['Job URL'].apply(make_clickable)

with st.sidebar.expander("Filter Options", expanded=True):
    # Place a button in the sidebar for data refresh
    if st.sidebar.button('Refresh Data'):
        st.legacy_caching.clear_cache()  # Reload the data

with st.sidebar.expander("Filter Options", expanded=True):
    location = st.sidebar.multiselect('Country', options=df['Country'].unique(), default=[])
    if location:
        df_filtered = df[df['Country'].isin(location)]
    else:
        df_filtered = df


total_job_listings = len(df)

# Display the KPI
st.metric(label="Total Job Listings", value=total_job_listings)

col1, col2 = st.columns(2)

# Aggregate job counts by country
country_counts = df['Country'].value_counts().reset_index()
country_counts.columns = ['Country', 'Number of Listings']

# Create a bar chart with Altair
country_chart = alt.Chart(country_counts).mark_bar().encode(
    x='Number of Listings:Q',
    y=alt.Y('Country:N', sort='-x'),
    tooltip=['Country', 'Number of Listings']
).properties(
    title="Job Listings by Country",
    width=600,
    height=300  # Adjust based on the number of countries
)
# with col1:
st.altair_chart(country_chart, use_container_width=True)


# Ensure 'Date Posted' is a datetime type for proper resampling
df['Date Posted'] = pd.to_datetime(df['Date Posted'])

# Resample and count job listings by month
time_series = df.resample('M', on='Date Posted').size().reset_index(name='Count')

# Create a line chart with Altair
time_chart = alt.Chart(time_series).mark_line(point=True).encode(
    x='Date Posted:T',
    y='Count:Q',
    tooltip=['Date Posted:T', 'Count']
).properties(
    title="Trend of Job Listings Over Time",
    width=600,
    height=300
)

# with col1:
st.altair_chart(time_chart, use_container_width=True)


st.dataframe(df_filtered, height=600)
