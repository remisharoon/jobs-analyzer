from sqlalchemy import create_engine
import streamlit as st
import pandas as pd

connection_string = st.secrets.PostgresDB.connection_string
conn = create_engine(connection_string)

sqlQuery = """
				SELECT 
                  site, 
                  job_url, 
                  job_url_direct, 
                  title, 
                  company, 
                  "location", 
                  job_type, 
                  date_posted, 
                  "interval", 
                  min_amount, 
                  max_amount, 
                  currency, 
                  is_remote, 
                  emails, 
                  description, 
                  company_url, 
                  company_url_direct, 
                  company_addresses, 
                  company_industry, 
                  company_num_employees, 
                  company_revenue, 
                  company_description, 
                  logo_photo_url, 
                  banner_photo_url, 
                  ceo_name, 
                  ceo_photo_url, 
                  jr.job_hash, 
                  jcm.target_value as country_inferred, 
                  state_inferred, 
                  city_inferred, 
                  tss.desired_tech_skills_inferred, 
                  desired_soft_skills_inferred, 
                  desired_domain_skills_inferred, 
                  domains_inferred, 
                  company_sector_inferred, 
                  position_seniority_level_inferred, 
                  job_type_inferred, 
                  job_title_inferred, 
                  job_description_inferred, 
                  job_requirements_inferred, 
                  job_responsibilities_inferred, 
                  job_benefits_inferred, 
                  salary_inferred, 
                  company_name_inferred, 
                  company_description_inferred, 
                  company_website_inferred, 
                  company_size_inferred, 
                  company_industry_inferred, 
                  company_headquarters_inferred, 
                  company_employees_inferred, 
                  company_revenue_inferred, 
                  "company industry" 
                FROM 
                  ja_jobs_raw jr 
                left outer join ja_country_names_std_mapping jcm on jr.country_inferred = jcm.source_value
                left outer join (SELECT job_hash, string_agg(desired_tech_skill_standardized, ',') as desired_tech_skills_inferred
								FROM ja_job_tech_skills
								where desired_tech_skill_standardized is not null and desired_tech_skill_standardized != 'TO_BE_DECIDED'
								GROUP BY job_hash) tss on jr.job_hash = tss.job_hash
								where site = 'linkedin'
								and is_deleted = 'N';
                """


# @st.cache(allow_output_mutation=True)
@st.cache_data(ttl=3600)
def load_dataframe(sql=sqlQuery):
    SQL_Query = pd.read_sql(sql, conn)
    df = pd.DataFrame(SQL_Query)
    df['date_posted'] = pd.to_datetime(df['date_posted'],
                                       errors='coerce')  # Use 'coerce' to handle any invalid parsing as NaT
    df = df.dropna(subset=['date_posted'])  # Optional: Remove rows where conversion failed

    return df
