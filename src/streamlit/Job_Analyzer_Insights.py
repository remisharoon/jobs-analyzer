import streamlit as st
import pandas as pd
from jobs_data import load_dataframe
import sys
import os
import altair as alt
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
import re
from datetime import datetime

# Regular expression for validating an Email
email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

def validate_email(email):
    """Validate the email address using a regular expression."""
    if re.match(email_regex, email):
        return True
    return False

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

# Database connection
connection_string = st.secrets.PostgresDB.connection_string
engine = create_engine(connection_string)

# Initialize metadata object
metadata = MetaData()

# Define the table
ja_job_alerts_subscriptions = Table(
    'ja_job_alerts_subscriptions', metadata,
    Column('email', String),
    Column('country', String),
    Column('job_title', String),
    Column('insert_timestamp', DateTime),
)

# Create the table if it doesn't exist
metadata.create_all(engine)

st.set_page_config(
    page_title="Data Job Listings Dashboard",
    page_icon=":bar_chart:",
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


# Calculate total job listings and listings by job title
total_job_listings = len(df)
data_engineer_count = len(df[df['Title'].str.contains("engineer", case=False)])
data_science_count = len(df[df['Title'].str.contains("scien", case=False)])
data_analyst_count = len(df[df['Title'].str.contains("analys", case=False)])

# Display the KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric(label="Total Data Job Listed", value=total_job_listings)
col2.metric(label="Data Engineering Jobs", value=data_engineer_count)
col3.metric(label="Data Science Jobs", value=data_science_count)
col4.metric(label="Data Analysis Jobs", value=data_analyst_count)


# Using markdown to add a visual divider
# st.markdown("---")
# st.markdown("### Subscribe to Job Alerts")

# Using columns to center the button more effectively
col1, col2 = st.columns([1, 3])

with col2:
    with st.expander("🌟🌟🌟🌟🌟  Subscribe To My Data Job Alerts Now!  🌟🌟🌟🌟🌟"):
        with st.form(key='subscribe_form'):
            email = st.text_input("Email")
            country = st.multiselect("Country", options=["United Arab Emirates", "Saudi Arabia", "Qatar", "Oman", "Kuwait", "Bahrain"])
            job_title = st.multiselect("Desired Job Title", options=["Data Engineer", "Data Analyst", "Data Architect", "Data Scientist"])
            submit_button = st.form_submit_button("Submit")
            if submit_button:
                if not validate_email(email):
                    st.error("Invalid email format. Please enter a valid email address.")
                else:
                    # Logic to save data to the database
                    for each_country in country:
                        for each_title in job_title:
                            try:
                                with engine.begin() as connection:
                                    insert_statement = ja_job_alerts_subscriptions.insert().values(
                                        email=email, country=each_country, job_title=each_title, insert_timestamp=datetime.now()
                                    )
                                    connection.execute(insert_statement)
                            except Exception as e:
                                print("Failed to insert data:", e)
                    st.success(f"{email} Subscribed successfully For {country} And {job_title} Job Alerts!", icon="✅")

# Another markdown divider for neatness
# st.markdown("---")


# Aggregate job counts by country
country_counts = df['Country'].value_counts().reset_index()
country_counts.columns = ['Country', 'Number of Listings']

# Create a bar chart with Altair
country_chart = alt.Chart(country_counts).mark_bar().encode(
    x='Number of Listings:Q',
    y=alt.Y('Country:N', sort='-x'),
    tooltip=['Country', 'Number of Listings']
).properties(
    # title="Job Listings by Country",
    width=600,
    height=300  # Adjust based on the number of countries
)


# Pie Chart
pie_chart = alt.Chart(country_counts).mark_arc().encode(
    theta=alt.Theta(field="Number of Listings", type="quantitative"),
    color=alt.Color(field="Country", type="nominal", legend=None),
    tooltip=['Country', 'Number of Listings']
).properties(
    title='Job Listings by Country'
)


with col1:
    st.altair_chart(pie_chart, use_container_width=True)

with col2:
    st.altair_chart(country_chart, use_container_width=True)



# Ensure 'Date Posted' is a datetime type for proper resampling
df['Date Posted'] = pd.to_datetime(df['Date Posted'])

# Sort dataframe by date to ensure correct slicing
df = df.sort_values('Date Posted')

# Filter the DataFrame to keep only the last three months
three_months_ago = pd.Timestamp.today() - pd.DateOffset(months=3)
filtered_df = df[df['Date Posted'] >= three_months_ago]

# Resample and count job listings by day
daily_time_series = filtered_df.resample('D', on='Date Posted').size().reset_index(name='Count')

# Create a line chart with Altair for daily data
daily_chart = alt.Chart(daily_time_series).mark_line(point=True).encode(
    x='Date Posted:T',
    y='Count:Q',
    tooltip=['Date Posted:T', 'Count']
).properties(
    title="Trend of Job Listings Over the Last Three Months (Daily)",
    width=600,
    height=300
)

# Use Streamlit to display the chart, adapting to container width
st.altair_chart(daily_chart, use_container_width=True)


# st.dataframe(df, height=600)


# Your dataframe 'df' should be defined above this

# Create a grid options builder to customize the grid
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination()  # Enable pagination
gb.configure_side_bar()  # Enable the side bar for filtering
gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
gridOptions = gb.build()

# Display the dataframe with AgGrid
grid_response = AgGrid(
    df,
    gridOptions=gridOptions,
    height=600,
    width='100%',
    data_return_mode=DataReturnMode.AS_INPUT,
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=False
)

# # Access updated dataframe if changes are made
# updated_df = grid_response['data']
# st.write("Updated DataFrame:")
# st.dataframe(updated_df)

engine.dispose()