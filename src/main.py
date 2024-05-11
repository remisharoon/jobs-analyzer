from jobspy import scrape_jobs
import pandas as pd
from connections.neondb_client import get_neon_engine
from sqlalchemy import create_engine, MetaData, Table, select
import mmh3
import requests
import time
import json
from src.config import read_config


def hash_url(url):
    return mmh3.hash(url, signed=False)


# Set up the GeminiPro API request
gemini_config = read_config()['GeminiPro']
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"
params = {'key': gemini_config['API_KEY']}
headers = {'Content-Type': 'application/json'}

table_name = 'ja_jobs_raw'
engine = get_neon_engine()


try:
    jobs: pd.DataFrame = scrape_jobs(
        # site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
        # site_name=["indeed", "linkedin", "zip_recruiter"],
        site_name=["linkedin"],
        search_term=' "Data Engineer" ',
        # location="United Arab Emirates",
        # location="Saudi Arabia",
        location="Qatar",
        results_wanted=40,  # be wary the higher it is, the more likey you'll get blocked (rotating proxy can help tho)
        hours_old=240,
        # country_indeed="united arab emirates",
        # country_indeed="saudi arabia",
        country_indeed="qatar",
        # proxy="http://jobspy:5a4vpWtj8EeJ2hoYzk@ca.smartproxy.com:20001",
        linkedin_fetch_description=True,
    )

    print(jobs.describe())
    print(jobs['date_posted'], jobs['title'], jobs['location'])

    # Ensure date_posted is in datetime format
    jobs['date_posted'] = pd.to_datetime(jobs['date_posted'])

    # Populate null date_posted with current date
    jobs['date_posted'] = jobs['date_posted'].fillna(pd.Timestamp.now().normalize())

    print(jobs['date_posted'])

    jobs['job_hash'] = jobs['job_url'].apply(hash_url)

    metadata = MetaData()
    metadata.reflect(bind=engine)
    if table_name in metadata.tables:
        jobs_table = Table(table_name, metadata, autoload=True, autoload_with=engine)
        sql = "select job_hash from " + table_name
        df = pd.read_sql(sql, con=engine)
        existing_job_hashes = set(df['job_hash'])
        jobs = jobs[~jobs['job_hash'].isin(existing_job_hashes)]

    # formatting for pandas
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", 50)  # set to 0 to see full job url / desc

    # 1: output to console
    print(jobs)
    print(jobs.info())
    print(jobs['date_posted'])

    # 2: output to .csv
    jobs.to_csv("./jobs.csv", index=False)
    print("outputted to jobs.csv")

    # jobs = pd.read_csv("./jobs.csv")

    # Iterate over each row in the DataFrame
    for index, row in jobs.iterrows():
        # Extract relevant fields
        title = row['title']
        company = row['company']
        location = row['location']
        description = row['description']
        company_url = row['company_url']

        input_text = str(title) + " " + str(company) + " " + str(location) + " " + str(description) + " " + str(company_url)

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
        while try_count < 5:
            try:
                response = requests.post(url, json=payload, headers=headers, params=params)
                if response.status_code == 200:
                    result = response.json()
                    # print(result)
                    result_json_str = result['candidates'][0]['content']['parts'][0]['text']
                    # print(result_json_str)
                    result_json_str = result_json_str.lstrip("```").rstrip("```")
                    # print(result_json_str)
                    result_dict = json.loads(result_json_str)
                    # print(result_dict)
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
                    print(jobs[field_suffix_list])
                    break
                else:
                    print(f"API request failed with status code {response.status_code}. Retrying...")
                    try_count += 1
            except Exception as e:
                print(f"API request failed with exception {e}. Retrying...")
                time.sleep(retry_delay)
                try_count += 1
                retry_delay *= 2

    print(jobs['date_posted'])
    print(jobs.info())
    # 2: output to .csv
    jobs.to_csv("./jobs_enriched.csv", index=False)
    print("outputted to jobs.csv")

    # jobs = pd.read_csv("./jobs_enriched.csv")

    try_count = 1
    retry_delay = 5  # sleep for 5 seconds before retrying
    while try_count < 5:
        try:
            jobs.to_sql(name=table_name,con=engine, if_exists='append',index=False)
            print("inserted to db")
            break
        except Exception as e:
            print(e)
            try_count += 1
            time.sleep(retry_delay)
            retry_delay *= 2

except Exception as e:
    print(e)
    engine.dispose()
