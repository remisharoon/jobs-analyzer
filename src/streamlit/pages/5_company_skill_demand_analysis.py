import streamlit as st
import pandas as pd
import altair as alt
from jobs_data import load_dataframe

# Load the data
df = load_dataframe()

# Ensure the skills column is in list form and explode it
df['desired_tech_skills_inferred'] = df['desired_tech_skills_inferred'].apply(
    lambda x: x.split(',') if isinstance(x, str) else [])
df_exploded = df.explode('desired_tech_skills_inferred')


def plot_companies_hiring_for_skill(skill):
    # Filter data for the selected skill
    skill_df = df_exploded[df_exploded['desired_tech_skills_inferred'] == skill]

    # Count job listings by company
    company_counts = skill_df['company'].value_counts().reset_index()
    company_counts.columns = ['Company', 'Job Listings']

    # Create the chart
    chart = alt.Chart(company_counts).mark_bar().encode(
        x='Job Listings:Q',
        y=alt.Y('Company:N', sort='-x'),
        tooltip=['Company', 'Job Listings']
    ).properties(
        title=f'Companies Hiring for {skill}',
        width=600,
        height=400
    ).interactive()

    st.altair_chart(chart, use_container_width=True)

def display_companies_for_top_skills_0(data, top_n=5):
    # Aggregate the most demanded skills
    skill_counts = data['desired_tech_skills_inferred'].value_counts().head(top_n).reset_index()
    skill_counts.columns = ['Skill', 'Count']

    st.subheader(f'Companies Hiring for Top {top_n} Skills')

    # For each top skill, find and display the companies hiring for it
    for skill in skill_counts['Skill']:
        st.markdown(f"#### {skill}")
        skill_df = data[data['desired_tech_skills_inferred'] == skill]

        # Count job listings by company for the skill
        company_counts = skill_df['company'].value_counts().reset_index()
        company_counts.columns = ['Company', 'Job Listings']

        # Display the data as a table (or use Altair for a chart)
        st.write(company_counts)


def display_companies_for_top_skills_1(data, top_n=5):
    skill_counts = data['desired_tech_skills_inferred'].value_counts().head(top_n).reset_index()
    skill_counts.columns = ['Skill', 'Count']

    st.subheader(f'Companies Hiring for Top {top_n} Skills')

    for skill in skill_counts['Skill']:
        with st.expander(f"{skill}"):
            skill_df = data[data['desired_tech_skills_inferred'] == skill]
            company_counts = skill_df['company'].value_counts().reset_index()
            company_counts.columns = ['Company', 'Job Listings']
            st.write(company_counts)


def display_companies_for_top_skills(data, top_n=5):
    skill_counts = data['desired_tech_skills_inferred'].value_counts().head(top_n).reset_index()
    skill_counts.columns = ['Skill', 'Count']

    st.subheader(f'Companies Hiring for Top {top_n} Skills')

    tab_list = st.tabs([f"{skill}" for skill in skill_counts['Skill']])

    for tab, skill in zip(tab_list, skill_counts['Skill']):
        with tab:
            skill_df = data[data['desired_tech_skills_inferred'] == skill]
            company_counts = skill_df['company'].value_counts().reset_index()
            company_counts.columns = ['Company', 'Job Listings']
            st.write(company_counts)


def main():
    st.title('Analysis of Companies Hiring Specific Skills')

    # Ensuring the column only contains strings and dropping NaN values before creating the list of unique skills
    unique_skills = df_exploded['desired_tech_skills_inferred'].dropna().unique()
    unique_skills = [skill for skill in unique_skills if isinstance(skill, str)]
    unique_skills = sorted(unique_skills)

    selected_skill = st.selectbox('Select a Technology Skill', unique_skills)

    if selected_skill:
        plot_companies_hiring_for_skill(selected_skill)

    # New section for top skills
    display_companies_for_top_skills(df_exploded, top_n=20)

if __name__ == '__main__':
    main()
