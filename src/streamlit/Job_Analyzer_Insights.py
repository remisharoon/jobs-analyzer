import streamlit as st
import pandas as pd
from jobs_data import load_dataframe
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

st.set_page_config(
    page_title="Job Listings Dashboard",
    page_icon="🧑🏽‍💻",
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

# Place a button in the sidebar for data refresh
if st.sidebar.button('Refresh Data'):
    st.legacy_caching.clear_cache()  # Reload the data

location = st.sidebar.multiselect('Country', options=df['Country'].unique(), default=[])
if location:
    df_filtered = df[df['Country'].isin(location)]
else:
    df_filtered = df

st.dataframe(df_filtered, height=600)
# Now, to display this as an HTML table with clickable links
# st.write(df_filtered.to_html(escape=False, index=False), unsafe_allow_html=True)

# Applying styling to the DataFrame
# styled_df = df_filtered.style.set_properties(**{
#     'background-color': 'black',
#     'color': 'lime',
#     'border-color': 'white'
# })
#
# # Convert the styled DataFrame to HTML for display in Streamlit
# html = styled_df.to_html(escape=False, index=False)
#
# # Use Streamlit's markdown to display the HTML with styling, allowing HTML content
# st.markdown(html, unsafe_allow_html=True)



# df