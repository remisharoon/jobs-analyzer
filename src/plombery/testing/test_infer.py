from src.plombery.jobs_scrape_pipeline import infer_from_rawdata, save_to_db

jobs = infer_from_rawdata()
print("jobs = ", jobs.head())
save_to_db("ja_jobs_raw",jobs)