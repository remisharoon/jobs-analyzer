import streamlit as st
import pandas as pd
import altair as alt
# geographic_analysis.py
import streamlit as st
import pandas as pd
import plotly.express as px
from jobs_data import load_dataframe
import plotly.express as px
import plotly.graph_objects as go
from fuzzywuzzy import process

df = load_dataframe()

def plot_country_distribution(data):
    country_counts = data['country_inferred'].value_counts().reset_index()
    country_counts.columns = ['Country', 'Number of Jobs']
    fig = px.bar(country_counts, x='Country', y='Number of Jobs', title='Job Distribution by Country')
    st.plotly_chart(fig, use_container_width=True)


def plot_state_distribution(data, selected_country):
    state_data = data[data['country_inferred'] == selected_country]
    state_counts = state_data['state_inferred'].value_counts().reset_index()
    state_counts.columns = ['State', 'Number of Jobs']
    fig = px.bar(state_counts, x='State', y='Number of Jobs', title=f'Job Distribution in {selected_country} by State')
    st.plotly_chart(fig, use_container_width=True)


def plot_city_distribution(data, selected_state):
    city_data = data[data['state_inferred'] == selected_state]
    city_counts = city_data['city_inferred'].value_counts().reset_index()
    city_counts.columns = ['City', 'Number of Jobs']
    fig = px.bar(city_counts, x='City', y='Number of Jobs', title=f'Job Distribution in {selected_state} by City')
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title('Geographic Analysis of Job Listings')

    countries = df['country_inferred'].unique()
    selected_country = st.selectbox('Select a Country', countries)
    plot_country_distribution(df)

    if selected_country:
        plot_state_distribution(df, selected_country)
        states = df[df['country_inferred'] == selected_country]['state_inferred'].unique()
        selected_state = st.selectbox('Select a State', states)
        if selected_state:
            plot_city_distribution(df, selected_state)


if __name__ == '__main__':
    main()