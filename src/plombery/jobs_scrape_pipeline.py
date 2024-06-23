from asyncio import sleep
from datetime import datetime
import enum
from typing import Optional
from dateutil import tz
from requests_html import HTMLSession
from apscheduler.triggers.interval import IntervalTrigger

from pydantic import BaseModel, Field

from plombery import register_pipeline, task, Trigger, get_logger
import random
import mmh3
import requests
import time
import json
from jobspy import scrape_jobs
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import urllib.request
from config import read_config
import unicodedata
import re


def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")



class InputParams(BaseModel):
    """Showcase all the available input types in Plombery"""


locations = ["United Arab Emirates", "Saudi Arabia", "Qatar", "Oman", "Kuwait", "Bahrain", "Turkey", "Malaysia"]
# Map location to the country name used in 'indeed'
country_indeed_mapping = {
    "United Arab Emirates": "united arab emirates",
    "Saudi Arabia": "saudi arabia",
    "Qatar": "qatar",
    "Oman": "oman",
    "Kuwait": "kuwait",
    "Bahrain": "bahrain",
    "Turkey": "turkey",
    "Malaysia": "malaysia"
}


# Set up the API request
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']
API_KEY = gemini_config['API_KEY']

params = {'key': API_KEY}  # Use the actual API key provided
headers = {'Content-Type': 'application/json'}

neondb_config = read_config()['PostgresDB']
connection_string = neondb_config['connection_string']



def save_to_db(table_name, df: pd.DataFrame):
    engine = create_engine(connection_string)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    engine.dispose()

def query_to_df(query) -> pd.DataFrame:
    engine = create_engine(connection_string)
    df = pd.read_sql_query(query, con=engine)
    engine.dispose()
    return df


def hash_url(url):
    return mmh3.hash(url, signed=False)


def get_raw_data() -> pd.DataFrame:
    # Get the current hour of the day
    current_hour = datetime.now().hour

    # Divide the day into 8 segments (24 hours / 8 segments = 3 hours per segment)
    segment = current_hour // 3

    # Choose location based on the current segment
    location = locations[segment % len(locations)]
    # location = locations[0]
    country_indeed = country_indeed_mapping[location]

    try:
        jobs: pd.DataFrame = scrape_jobs(
            # site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
            # site_name=["indeed", "linkedin", "zip_recruiter"],
            site_name=["linkedin"],
            # search_term="data",
            search_term=' Data Engineer OR Data Analyst OR Data Scientist OR Data Architect ',
            location=location,
            results_wanted=100,
            hours_old=168,
            # be wary the higher it is, the more likey you'll get blocked (rotating proxy can help tho)
            country_indeed=country_indeed,
            # proxy="http://jobspy:5a4vpWtj8EeJ2hoYzk@ca.smartproxy.com:20001",
            linkedin_fetch_description=True,
        )

        jobs['job_hash'] = jobs['job_url'].apply(hash_url)
        # Ensure date_posted is in datetime format
        jobs['date_posted'] = pd.to_datetime(jobs['date_posted'])

        # Populate null date_posted with current date
        jobs['date_posted'] = jobs['date_posted'].fillna(pd.Timestamp.now().normalize())
        jobs['is_deleted'] = 'N'

        #Filter out duplicates
        df = query_to_df("SELECT distinct job_hash FROM (SELECT job_hash FROM ja_jobs_raw_new union all SELECT job_hash FROM ja_jobs_raw) T")
        existing_job_hashes = set(df['job_hash'])
        jobs = jobs[~jobs['job_hash'].isin(existing_job_hashes)]

        # formatting for pandas
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", 50)  # set to 0 to see full job url / desc

        # 1: output to console
        # print(jobs)
    except Exception as e:
        print("Error: " + str(e))
        print(f"Error: {type(e)}")
        jobs = pd.DataFrame()

    return jobs



@task
async def get_jobs_data(params: InputParams) -> pd.DataFrame:
    try:
        jobs: pd.DataFrame = get_raw_data()
        save_to_db('ja_jobs_raw_new', jobs)

        # inferred_jobs = infer_from_rawdata()
        # save_to_db('ja_jobs_raw', inferred_jobs)

    except Exception as e:
        print(e)



def infer_from_rawdata() -> pd.DataFrame:
    jobs = query_to_df("SELECT distinct * FROM ja_jobs_raw_new where job_hash not in (SELECT job_hash FROM ja_jobs_raw) order by date_posted desc limit 50")
    print("jobs count = ", str(len(jobs)))

    # Define key fields to check
    key_fields = [
        'country_inferred',
        'job_title_inferred',
        'company_name_inferred',
        'desired_tech_skills_inferred'
    ]

    # Iterate over each row in the DataFrame
    for index, row in jobs.iterrows():
        # Extract relevant fields
        title = row['title']
        company = row['company']
        location = row['location']
        description = row['description']
        company_url = row['company_url']

        input_text = (str(title) + " " + str(company) + " " + str(location) + " " + str(description) + " " + str(
            company_url)).replace("  ", " ").replace("\n", " ").replace("\t", " ")

        input_text = re.sub('\s+', ' ', input_text)

        # Set up the API request
        # Call Gemini API
        start_time = time.time()  # Start timing

        payload = {
            "contents": [{"parts": [{"text": f"""  Extract and return these fields in a dictionary:
                    1. country
                    2.state
                    3.city
                    4.desired tech skills (as a list)
                    5.desired soft skills (as a list)
                    6.desired domain skills (as a list)
                    7. domains (as a list)
                    8.company sector
                    9.position seniority level
                    10. job type
                    11. job title
                    12. job description
                    13. job requirements
                    14. job responsibilities
                    15. job benefits
                    16. salary (if mentioned)
                    18. company name (if mentioned)
                    19. company description (if mentioned)
                    20. company website (if mentioned)
                    21. company size (if mentioned)
                    22. company industry (if mentioned)
                    23. company headquarters (if mentioned)
                    24. company employees (if mentioned)
                    25. company revenue (if mentioned)
                    , from this text  - {input_text} """}]}]
        }

        try_count = 1
        retry_delay = 5  # sleep for 5 seconds before retrying
        while try_count < 7:
            try:
                response = requests.post(url, json=payload, headers=headers, params=params)
                if response.status_code == 200:
                    result = response.json()
                    # print("result - ", result)
                    result_json_str = result['candidates'][0]['content']['parts'][0]['text']

                    # print("result_dict - ", result_json_str)
                    result_json_str = result_json_str.lstrip("```").rstrip("```")
                    result_json_str = remove_control_characters(result_json_str)

                    # print("result_json_str 2 - ", result_json_str)
                    result_dict = json.loads(result_json_str)
                    # print("result_dict - ", result_dict)
                    end_time = time.time()  # End timing
                    print(f"get time: {end_time - start_time} seconds")

                    fields_list = [
                        'country',
                        'state',
                        'city',
                        'desired tech skills',
                        'desired soft skills',
                        'desired domain skills',
                        'domains',
                        'company sector',
                        'position seniority level',
                        'job type',
                        'job title',
                        'job description',
                        'job requirements',
                        'job responsibilities',
                        'job benefits',
                        'salary',
                        'company name',
                        'company description',
                        'company website',
                        'company size',
                        'company industry',
                        'company headquarters',
                        'company employees',
                        'company revenue'
                    ]
                    field_suffix_list = []
                    for field in fields_list:
                        field_suffix = field.replace(" ", "_") + "_inferred"
                        if field not in result_dict:
                            jobs.at[index, field_suffix] = ""
                        else:
                            value = result_dict[field]
                            # Check if the value is a list
                            if isinstance(value, list):
                                # Convert the list to a string representation
                                try:
                                    value_str = ", ".join(str(item) for item in value)
                                except Exception as e:
                                    print(f"Error converting list {value} to string: {e}")
                                    value_str = "Unknown"
                                jobs.at[index, field_suffix] = value_str
                            else:
                                jobs.at[index, field_suffix] = str(value)
                        field_suffix_list.append(field_suffix)
                    # print(jobs[field_suffix_list])
                    break
                else:
                    print(f"API request failed with status code {response.status_code}. Retrying...")
                    try_count += 1
            except Exception as e:
                print(f"API request failed with exception {e}. Retrying...")
                time.sleep(retry_delay)
                try_count += 1
                retry_delay *= 2

    # Filter out records where any key field is not inferred
    # for field in key_fields:
    #     jobs = jobs[jobs[field].astype(bool)]

    jobs = jobs[jobs['job_title_inferred'].str.len() > 0]
    return jobs



@task
async def ai_infer_raw_data():
    inferred_jobs = infer_from_rawdata()
    save_to_db('ja_jobs_raw', inferred_jobs)



@task
async def load_jobs_analyzer_site():
    print("Loading Jobs Analyzer site...")
    try:
        session = HTMLSession()
        response = session.get("https://jobs-analyzer.streamlit.app/")

        # Render the JavaScript. The timeout can be adjusted or removed.
        response.html.arender(timeout=20)

        print("Page loaded successfully!")
        session.close()
    except Exception as e:
        print(f"Error: {e}")

register_pipeline(
    id="jobs_pipeline",
    description="""This is a very jobby pipeline""",
    tasks=[get_jobs_data, ai_infer_raw_data, load_jobs_analyzer_site],
    # tasks=[load_jobs_analyzer_site],
    triggers=[
        Trigger(
            id="daily8",
            name="Daily8",
            description="Run the pipeline 8 times daily",
            params=InputParams(),
            schedule=IntervalTrigger(
                # days=1,
                hours=3,
                # start_date=datetime(
                #     2023, 1, 1, 22, 30, tzinfo=tz.gettz("Europe/Brussels")
                # ),
            ),
        )
    ],
    params=InputParams,
)
