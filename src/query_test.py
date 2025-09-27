
from jobspy import scrape_jobs
import pandas as pd
from connections.neondb_client import get_neon_engine
from sqlalchemy import create_engine, MetaData, Table, select
import mmh3


table_name = 'ja_jobs_raw'
engine = get_neon_engine()

sql = "select job_hash from " + table_name
df = pd.read_sql(sql,con=engine)

print(df)

# Convert the 'job_hash' column of the DataFrame to a set
job_hash_set = set(df['job_hash'])

# Print the set of job hashes
print(job_hash_set)