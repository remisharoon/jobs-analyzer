# [Jobs Analyzer](https://jobs-analyzer.streamlit.app/)

This project provides insights into job market trends, leveraging data from the JobSpy library and analytics powered by the Gemini Pro API. It's designed to help users understand the demand for various technology skills, geographical job distribution, and more.

## [Overview](https://jobs-analyzer.streamlit.app/)

Jobs Analyzer uses a sophisticated data pipeline to fetch, analyze, and visualize job listing data. Here's how it works:

1. **Data Acquisition**: The data is sourced from the [JobSpy](https://github.com/Bunsly/JobSpy) library, which aggregates job listings from various platforms.
2. **Data Analysis**: The analysis is powered by the [Gemini Pro API](https://ai.google.dev/), offering deep insights into the job market trends and skill demands.
3. **Data Pipeline**: The entire pipeline is run using [Plombery](https://lucafaggianelli.github.io/plombery/) on a free VM provided by Oracle Cloud, ensuring cost-effective scalability.
4. **Data Storage**: Processed data is stored in a free PostgreSQL instance hosted on [NeonDB](https://neon.tech/) and mirrored into Elasticsearch for fast search and analytical workloads.
5. **Data Distribution**: Each pipeline run exports the latest job inventory to Cloudflare R2 (`me-data-jobs` bucket) as a public JSON feed.
6. **Dashboard**: The results are visualized through a dashboard hosted on Streamlit Cloud, available at [https://jobs-analyzer.streamlit.app/](https://jobs-analyzer.streamlit.app/).

## Getting Started

To explore the job market trends and insights, visit the Jobs Analyzer dashboard. For developers interested in contributing or setting up their version of the pipeline, refer to the setup instructions below.

### Setup Instructions

1. Clone this repository to your local machine or cloud environment.
2. Ensure you have access to the JobSpy library and Gemini Pro API.
3. Set up a free VM on Oracle Cloud and configure it according to the project requirements.
4. Create a free PostgreSQL database on NeonDB and configure the connection parameters in the project.
5. Provision an Elasticsearch cluster (or use an existing one) and set `host`, `username`, `password`, and `jobs_index` in `src/plombery/config/config.ini`.
6. Set up a Cloudflare R2 bucket (default: `me-data-jobs`) and update the credentials plus `JOBS_BUCKET`/`JOBS_EXPORT_KEY` values in `src/plombery/config/config.ini`.
7. Deploy the Streamlit dashboard to Streamlit Cloud, using the provided configuration files.

### R2 Export

- The pipeline writes a `jobs.json` snapshot to Cloudflare R2 using the bucket/key defined in `JOBS_BUCKET` and `JOBS_EXPORT_KEY`.
- The default public URL is `https://6d9a56e137a3328cc52e48656dd30d91.r2.cloudflarestorage.com/me-data-jobs/jobs.json`.
- Update `JOBS_CACHE_CONTROL` if you need different CDN caching behaviour.

## Contributions

Contributions to Jobs Analyzer are welcome! Whether it's adding new features, improving the data analysis, or suggesting UI enhancements for the dashboard, feel free to fork this repository and submit a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
