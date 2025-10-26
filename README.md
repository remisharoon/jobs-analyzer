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

### Crswtch Scraper Pipeline

- The Crswtch scraper lives in `src/plombery/crswth_crs.py`. It mirrors the Dubizzle flow: scrape, enrich with detail page data, and index into Elasticsearch.
- Configure the behaviour in the `[crstch]` section of `src/plombery/config/config.ini` (listing URL template, page count, back-off timings, and ES index name).
- Ensure the Elasticsearch config contains either `carswitch_index` or set `es_index` inside the `[crstch]` block.
- Run the task manually with `plombery run crswtch_pipeline` or let the scheduled trigger (06:00 Asia/Dubai) execute it daily.
  - The pipeline includes an export step that writes `crswth_listings.json` and uploads it to Cloudflare R2 at `data/crswth_listings.json` within the configured bucket.
  - The scraper attempts to capture posted/published timestamps (created/published/posted/added/discountAppliedAt) when present, normalizes them to `*_iso` fields, and includes them in ES and in the JSON export.
  - For efficiency, listings that already exist in ES (matched by document `_id` == `id`) are skipped to avoid re-fetching the detail page.

### Allsopp & Allsopp Scraper Pipeline

- The residential scraper lives in `src/plombery/allsopp_crs.py`. It now harvests both **sales** and **lettings** inventories, enriches each fresh record with detail-page data, indexes them into Elasticsearch, and ships a combined JSON snapshot to Cloudflare R2.
- Configure the behaviour via the `[allsopp]` block in `src/plombery/config/config.ini`. Use `listing_url`/`pages`/`es_index` for sales and `lettings_listing_url`/`lettings_pages`/`lettings_es_index` for rentals; the delay/retry parameters are shared across both modes.
- Each mode short-circuits pagination when it encounters an ID already present in the respective Elasticsearch index, avoiding redundant detail fetches when older properties resurface.
- Raw CSV dumps are kept under `saved_data/allsopp/<segment>/page_<n>.csv`, while the merged `allsopp_listings.json` (with a `listing_category` flag of `sales` or `lettings`) is written locally and uploaded to `data/allsopp_listings.json` in Cloudflare R2 using the `[cloudflare]` `PROP_BUCKET` (falling back to `BUCKET`) credentials.
- Run the full pipeline with `plombery run allsopp_pipeline` or rely on the scheduled trigger at 05:30 Asia/Dubai.

### DLD Open Data Pipeline

- The Dubai Land Department open-data scraper lives in `src/plombery/dld_open_data.py`. It now scrapes the public “Real Estate Data” webpage (Next.js) instead of the legacy CKAN API and still supports the **Transactions**, **Rents**, **Projects**, **Valuations**, **Land**, **Building**, **Unit**, **Broker**, and **Developer** tabs.
- Configure tab slugs, primary date columns, and Elasticsearch indices inside the `[dld_open_data]` section of `src/plombery/config/config.ini`. The default `page_url` points at `https://dubailand.gov.ae/en/open-data/real-estate-data/`, while `lookback_days` and optional per-dataset `*_buffer_days` control incremental windows when deriving the `FromDate` filters.
- The scraper persists the most recent date per dataset in `saved_data/dld_open_data/state.json`, subtracting a small buffer (default three days) on every run to guard against late-arriving records. Artefacts are stored under `saved_data/dld_open_data/<dataset>/`, and records are indexed with `_dataset`, `_source_url`, and `_extracted_at_iso` metadata for downstream consumers.
- Run `plombery run dld_open_data_pipeline` to ingest immediately or rely on the built-in trigger (06:00 Asia/Dubai). The scraper now automatically retries when the website serves a temporary reCAPTCHA challenge ("I'm not a robot"), backing off between attempts before ultimately raising a `RecaptchaBlockedError` if the block persists.
- To minimise the chance of challenges in the first place, the HTTP client now relies on [`curl_cffi`](https://github.com/yifeikong/curl_cffi) impersonation profiles with HTTP/2 enabled and realistic `sec-ch-*` headers/user agents. This "ultra-modern" fingerprint keeps the pipeline aligned with how a real Chrome 124 browser negotiates TLS.

### Testing

- Run `python -m pytest tests/test_crswtch_parser.py` and `python -m pytest tests/test_allsopp_parser.py` (or `python -m unittest`) to validate the vehicle and property parsers against the embedded fixtures under `tests/fixtures`.
- UI helpers leveraged by the Streamlit dashboards are covered in `tests/test_streamlit_ui.py`; run `python -m pytest tests/test_streamlit_ui.py` to confirm filtering logic stays intact.
- Helper utilities for the DLD scraper are covered in `tests/test_dld_open_data.py`.

### R2 Export

- The pipeline writes a `jobs.json` snapshot to Cloudflare R2 using the bucket/key defined in `JOBS_BUCKET` and `JOBS_EXPORT_KEY`.
- The default public URL is `https://6d9a56e137a3328cc52e48656dd30d91.r2.cloudflarestorage.com/me-data-jobs/jobs.json`.
- Update `JOBS_CACHE_CONTROL` if you need different CDN caching behaviour.

## Contributions

Contributions to Jobs Analyzer are welcome! Whether it's adding new features, improving the data analysis, or suggesting UI enhancements for the dashboard, feel free to fork this repository and submit a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
