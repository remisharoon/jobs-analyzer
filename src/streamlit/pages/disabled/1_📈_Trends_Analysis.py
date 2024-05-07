# import streamlit as st
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# from pandas.api.types import CategoricalDtype
# from jobs_data import load_dataframe
# import plotly.express as px
#
# df = load_dataframe()
#
#
# # Job Postings Over Time
# def job_postings_over_time(data):
#     data['YearMonth'] = data['date_posted'].dt.to_period('M').dt.to_timestamp()
#     postings_count = data.groupby('YearMonth').size().reset_index(name='Count')
#
#     plt.figure(figsize=(10, 6))
#     sns.lineplot(data=postings_count, x='YearMonth', y='Count')
#     plt.title('Job Postings Over Time')
#     plt.xticks(rotation=45)
#     plt.tight_layout()
#     st.pyplot(plt)
#
#
#
# # Technology Skills Demand
# def tech_skills_demand(data):
#     # Assuming 'desired_tech_skills_inferred' column contains comma-separated values
#     tech_skills_series = data['desired_tech_skills_inferred'].dropna().str.split(', ')
#     tech_skills_flat = pd.Series([item for sublist in tech_skills_series for item in sublist])
#     top_tech_skills = tech_skills_flat.value_counts().head(10)
#
#     plt.figure(figsize=(10, 6))
#     sns.barplot(x=top_tech_skills.values, y=top_tech_skills.index)
#     plt.title('Top 10 Technology Skills Demand')
#     plt.xlabel('Number of Job Listings')
#     plt.ylabel('Technology Skills')
#     plt.tight_layout()
#     st.pyplot(plt)
#
#
# # Job Postings Over Time using Plotly
# def job_postings_over_time_plotly(data):
#     data['YearMonth'] = data['date_posted'].dt.to_period('M').dt.to_timestamp()
#     postings_count = data.groupby('YearMonth').size().reset_index(name='Count')
#
#     fig = px.line(postings_count, x='YearMonth', y='Count',
#                   labels={'Count': 'Number of Job Postings', 'YearMonth': 'Date'},
#                   title='Job Postings Over Time')
#     fig.update_xaxes(dtick="M1", tickformat="%b\n%Y", ticklabelmode="period")
#     st.plotly_chart(fig, use_container_width=True)
#
#
# # Technology Skills Demand using Plotly
# def tech_skills_demand_plotly(data):
#     tech_skills_series = data['desired_tech_skills_inferred'].dropna().str.split(', ')
#     tech_skills_flat = pd.Series([item for sublist in tech_skills_series for item in sublist])
#     top_tech_skills = tech_skills_flat.value_counts().head(10).reset_index()
#     top_tech_skills.columns = ['Skill', 'Count']
#
#     fig = px.bar(top_tech_skills, x='Count', y='Skill', orientation='h',
#                  title='Top 10 Technology Skills Demand',
#                  labels={'Count': 'Number of Job Listings', 'Skill': 'Technology Skills'})
#     fig.update_layout(yaxis={'categoryorder': 'total ascending'})
#     st.plotly_chart(fig, use_container_width=True)
#
# # Main app
# def main():
#     st.title('Job Listings Trends Analysis')
#
#     st.header('Job Postings Over Time')
#     job_postings_over_time_plotly(df)
#
#     st.header('Top Technology Skills Demand')
#     tech_skills_demand_plotly(df)
#
#     # You can add more analysis functions following the patterns above
#
#
# if __name__ == "__main__":
#     main()