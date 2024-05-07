# import streamlit as st
# import pandas as pd
# import pydeck as pdk
# from urllib.error import URLError
# import streamlit as st
# import pandas as pd
# import streamlit as st
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# from pandas.api.types import CategoricalDtype
# from jobs_data import load_dataframe
# import plotly.express as px
# import plotly.graph_objects as go
# from fuzzywuzzy import process
#
# df = load_dataframe()
#
# # Normalize skill names for basic advanced matching
# def normalize_skill(skill):
#     return skill.lower().replace(" ", "").replace("-", "").replace("_", "")
#
#
# def fuzzy_match_skills(user_skills, required_skills, threshold=80):
#     matched_skills = []
#     for user_skill in user_skills:
#         if user_skill:  # Check if the user_skill is not empty
#             match, score = process.extractOne(user_skill, required_skills)
#             if score >= threshold:
#                 matched_skills.append(match)
#     skill_gaps = [skill for skill in required_skills if skill not in matched_skills]
#     return matched_skills, skill_gaps
#
#
# def display_skill_gap_analysis(data):
#     st.title('Skill Gap Analysis with Fuzzy Matching')
#
#     user_skills_input = st.text_input('Enter your skills, separated by commas')
#     user_skills = [skill.strip().lower() for skill in user_skills_input.split(',')]
#
#     job_roles = data['job_title_inferred'].unique()
#     desired_role = st.selectbox('Select the job role you are interested in', options=job_roles)
#
#     required_skills_series = data[data['job_title_inferred'] == desired_role]['desired_tech_skills_inferred'].dropna()
#     required_skills = list(
#         set([skill.strip().lower() for sublist in required_skills_series.str.split(',') for skill in sublist]))
#
#     matched_skills, skill_gap = fuzzy_match_skills(user_skills, required_skills)
#
#     # Visualization with Plotly
#     all_skills = list(set(required_skills + user_skills))
#     skills_presence = [skill in matched_skills for skill in all_skills]
#
#     fig = go.Figure(data=[
#         go.Bar(x=all_skills, y=skills_presence, text=["Matched" if present else "Gap" for present in skills_presence],
#                marker_color=["green" if present else "red" for present in skills_presence])])
#     fig.update_layout(title_text=f'Skill Gap Analysis for {desired_role}', title_x=0.5, xaxis_title="Skills",
#                       yaxis=dict(title="Matched Skills", showticklabels=False), showlegend=False)
#     fig.update_traces(textposition='outside')
#     st.plotly_chart(fig, use_container_width=True)
#
#     if skill_gap:
#         st.write("Skills you might need to develop:")
#         st.write(', '.join(skill_gap))
#     else:
#         st.success("You have all the required skills for this role!")
#
#
# def main():
#     display_skill_gap_analysis(df)
#
#
# if __name__ == "__main__":
#     main()