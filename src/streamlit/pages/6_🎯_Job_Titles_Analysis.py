# JobTitlesAnalysis.py
import streamlit as st
import pandas as pd
from jobs_data import load_dataframe

# Load the data
df = load_dataframe()


def display_job_titles_analysis():
    st.title('Job Titles Analysis')

    # Example of displaying the distribution of job titles
    job_titles_count = df['job_title_inferred'].value_counts().reset_index()
    job_titles_count.columns = ['Job Title', 'Frequency']

    st.write("## Job Title Distribution")
    st.bar_chart(job_titles_count.set_index('Job Title'))


# Call the analysis function
display_job_titles_analysis()
