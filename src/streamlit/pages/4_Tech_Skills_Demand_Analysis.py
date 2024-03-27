import streamlit as st
import pandas as pd
import altair as alt
from jobs_data import load_dataframe

# Load and prepare the data
df = load_dataframe()
df = df[df['desired_tech_skills_inferred'].notna() & (df['desired_tech_skills_inferred'] != '')]
# Ensure the desired_tech_skills_inferred column is a list; if not, split it
df['desired_tech_skills_inferred'] = df['desired_tech_skills_inferred'].apply(lambda x: x if isinstance(x, list) else str(x).split(','))

# Normalize skill names: convert to lowercase, strip spaces, and then capitalize
df['desired_tech_skills_inferred'] = df['desired_tech_skills_inferred'].apply(
    lambda skills: [skill.lower().strip().capitalize() for skill in skills]
)

# Explode the DataFrame on the desired_tech_skills_inferred column
df_exploded = df.explode('desired_tech_skills_inferred')

# Count the occurrences of each skill
skill_counts = df_exploded['desired_tech_skills_inferred'].value_counts().reset_index()
skill_counts.columns = ['Skill', 'Count']

# Filter for top 20 skills
top_skills = skill_counts.head(20)

def plot_skill_demand(data):
    # Count the occurrences of each skill
    skill_counts = data['desired_tech_skills_inferred'].value_counts().reset_index()
    skill_counts.columns = ['Skill', 'Count']
    # Filter for top 20 skills
    top_skills = skill_counts.head(20)

    # Create the chart
    chart = alt.Chart(top_skills).mark_bar().encode(
        x=alt.X('Count:Q', title='Number of Job Listings'),
        y=alt.Y('Skill:N', sort='-x', title='Technology Skill', axis=alt.Axis(labels=True)),
        color=alt.Color('Skill:N', legend=None),
        tooltip=['Skill:N', 'Count:Q']
    ).properties(
        title='Top 20 Technology Skills Demand',
        width=600,
        height=400
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).interactive()

    st.altair_chart(chart, use_container_width=True)


def main():
    st.title('Technology Skills Demand Analysis')
    plot_skill_demand(df_exploded)

if __name__ == '__main__':
    main()
