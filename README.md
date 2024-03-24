# Jobs Analyzer

This project provides insights into job market trends, leveraging data from the JobSpy library and analytics powered by the Gemini Pro API. It's designed to help users understand the demand for various technology skills, geographical job distribution, and more.

## Overview

Jobs Analyzer uses a sophisticated data pipeline to fetch, analyze, and visualize job listing data. Here's how it works:

1. **Data Acquisition**: The data is sourced from the JobSpy library, which aggregates job listings from various platforms.
2. **Data Analysis**: The analysis is powered by the Gemini Pro API, offering deep insights into the job market trends and skill demands.
3. **Data Pipeline**: The entire pipeline is run on a free VM provided by Oracle Cloud, ensuring cost-effective scalability.
4. **Data Storage**: Processed data is stored in a free PostgreSQL instance hosted on NeonDB, optimizing for accessibility and performance.
5. **Dashboard**: The results are visualized through a dashboard hosted on Streamlit Cloud, available at [https://jobs-analyzer.streamlit.app/](https://jobs-analyzer.streamlit.app/).

## Getting Started

To explore the job market trends and insights, visit the Jobs Analyzer dashboard. For developers interested in contributing or setting up their version of the pipeline, refer to the setup instructions below.

### Setup Instructions

1. Clone this repository to your local machine or cloud environment.
2. Ensure you have access to the JobSpy library and Gemini Pro API.
3. Set up a free VM on Oracle Cloud and configure it according to the project requirements.
4. Create a free PostgreSQL database on NeonDB and configure the connection parameters in the project.
5. Deploy the Streamlit dashboard to Streamlit Cloud, using the provided configuration files.

## Contributions

Contributions to Jobs Analyzer are welcome! Whether it's adding new features, improving the data analysis, or suggesting UI enhancements for the dashboard, feel free to fork this repository and submit a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
