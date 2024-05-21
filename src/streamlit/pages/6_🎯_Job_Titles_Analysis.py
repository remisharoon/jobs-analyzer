# JobTitlesAnalysis.py
import streamlit as st
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from jobs_data import load_dataframe

# Load the data
df = load_dataframe()


def display_job_titles_analysis_x(data):
    st.title('Job Titles Analysis')

    # Creating a sorted list of unique job titles
    job_titles = sorted(data['job_title_inferred'].dropna().unique())

    # Setting 'Data Engineer' as the default job title, or the first title if not found
    default_title = 'Data Engineer' if 'Data Engineer' in job_titles else job_titles[0]


    # Search box for job titles
    job_title_selected = st.selectbox('Select Job Title:', job_titles, index=job_titles.index(default_title))

    if job_title_selected:
        # Filter data based on selected job title
        filtered_data = data[data['job_title_inferred'] == job_title_selected]

        st.subheader(f"Analysis for '{job_title_selected}'")
        st.write("Total Listings:", len(filtered_data))

        # Detailed Location Distribution
        if 'location' in filtered_data.columns:
            st.subheader("Location Distribution")
            location_counts = filtered_data['location'].value_counts().nlargest(10)
            st.bar_chart(location_counts)

        # Skills Distribution (assuming a 'skills' column exists with comma-separated values)
        if 'skills' in filtered_data.columns:
            st.subheader("Skills Distribution")
            skills_series = filtered_data['skills'].dropna().str.split(',').explode()
            skills_counts = skills_series.value_counts().nlargest(10)
            st.bar_chart(skills_counts)

        # Employer Distribution (assuming an 'employer' column exists)
        if 'company' in filtered_data.columns:
            st.subheader("Top Hiring Companies")
            company_counts = filtered_data['company'].value_counts().nlargest(10)
            st.bar_chart(company_counts)

        # Example analysis 1: Display count of listings for the selected job title
        st.write(f"## Analysis for {job_title_selected}")
        st.write(f"Total Listings for '{job_title_selected}':", len(filtered_data))

        # Example analysis 2: More detailed statistics or visualizations
        st.write("### Detailed Statistics")
        # Additional analysis can be added here, like average salary, locations, etc.

        # Optional: Include visualization
        st.write("### Distribution by Location (Example)")
        location_counts = filtered_data['location'].value_counts().head(10)
        st.bar_chart(location_counts)

    # Example of displaying the distribution of job titles
    job_titles_count = df['job_title_inferred'].value_counts().reset_index()
    job_titles_count.columns = ['Job Title', 'Frequency']

    st.write("## Job Title Distribution")
    st.bar_chart(job_titles_count.set_index('Job Title'))


def display_job_titles_analysis(data):
    st.title('Job Titles Analysis')

    # Creating a sorted list of unique job titles
    job_titles = sorted(data['job_title_inferred'].dropna().unique())
    default_title = 'Data Engineer' if 'Data Engineer' in job_titles else job_titles[0]

    job_title_selected = st.selectbox('Select Job Title:', job_titles, index=job_titles.index(default_title))

    if job_title_selected:
        filtered_data = data[data['job_title_inferred'] == job_title_selected]
        st.subheader(f"Analysis for '{job_title_selected}'")
        st.write("Total Listings:", len(filtered_data))

        if 'location' in filtered_data.columns:
            st.subheader("Location Distribution")
            location_counts = filtered_data['location'].value_counts().nlargest(10)
            fig = px.bar(location_counts, x=location_counts.index, y=location_counts.values,
                         labels={'x': 'Location', 'y': 'Count'})
            st.plotly_chart(fig)

        if 'skills' in filtered_data.columns:
            st.subheader("Skills Distribution")
            skills_series = filtered_data['skills'].dropna().str.split(',').explode()
            skills_counts = skills_series.value_counts().nlargest(10)
            wc = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(skills_counts)
            fig, ax = plt.subplots()
            ax.imshow(wc, interpolation='bilinear')
            ax.axis('off')
            st.pyplot(fig)

        if 'company' in filtered_data.columns:
            st.subheader("Top Hiring Companies")
            company_counts = filtered_data['company'].value_counts().nlargest(10)
            fig = px.pie(company_counts, values=company_counts.values, names=company_counts.index,
                         title='Top Hiring Companies')
            st.plotly_chart(fig)

        # Distribution by Job Titles
        job_titles_count = df['job_title_inferred'].value_counts().reset_index()
        job_titles_count.columns = ['Job Title', 'Frequency']
        st.subheader("Job Title Distribution")
        fig = px.bar(job_titles_count, x='Job Title', y='Frequency')
        st.plotly_chart(fig)

# Call the analysis function
display_job_titles_analysis(df)
