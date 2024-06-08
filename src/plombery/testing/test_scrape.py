from src.plombery.jobs_scrape_pipeline import get_raw_data, save_to_db

jobs = get_raw_data()
print("jobs = ", jobs.head())
save_to_db("ja_jobs_raw_new",jobs)