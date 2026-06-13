# Plombery Scraper

A scheduled data pipeline platform built on [Plombery](https://github.com/luciano-fiandesiro/plombery) for scraping, enriching, and publishing structured data. It exposes a web UI for monitoring and triggering pipelines, and runs multiple independent scrapers on cron/interval schedules.

## Quick Start

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy the config template and fill in your credentials:

```bash
cp src/config/config.ini.template src/config/config.ini
```

Run the server:

```bash
python src/app.py
```

The dashboard is available at **http://localhost:8080**.

## Pipelines

| Pipeline | File | Schedule | Description |
|---|---|---|---|
| **Jobs Scraper** | `src/jobs_scrape_pipeline.py` | Every 8 hours | Scrapes LinkedIn for Data Engineer / Data Architect roles in UAE, Saudi Arabia, and Qatar via `python-jobspy`. Enriches listings with Gemini AI (skills, company info, job metadata). Persists to PostgreSQL and Elasticsearch, exports JSON to Cloudflare R2. |
| **Jobs Alerts** | `src/jobs_alerts_pipeline.py` | Daily at 07:45 GST | Queries subscriber preferences and sends matching job alert emails via SMTP. |
| **Dimension Standardization** | `src/standardization_pipeline.py` | Every 24 hours | Uses Gemini AI to map raw inferred values (job titles, countries) to standardized reference entries in the database. |
| **Dubizzle Cars** | `src/dubzl_crs.py` | Daily at 05:45 GST | Scrapes used-car listings from Dubizzle UAE (paginated Next.js SSR). Stores in Elasticsearch and exports to R2. |
| **Carswitch Cars** | `src/crswth_crs.py` | Scheduled | Scrapes car listings from Carswitch. Parses streaming HTML payloads, enriches with detail-page data, indexes into Elasticsearch, and exports to R2. |
| **Allsopp Property** | `src/allsopp_crs.py` | Scheduled | Scrapes residential sales listings from allsoppandallsopp.com (Dubai). Stores in Elasticsearch and exports to R2. |
| **99acres Property** | `src/acres99_crs.py` | Scheduled | Scrapes property listings from 99acres (India). Stores in Elasticsearch and exports to R2. |
| **Property Launches (Multi-city)** | `src/property_launches_pipeline.py` | Daily per city | Discovers, enriches, and indexes pre-launch/new-launch projects by city using `[property_launches.<city>]` config sections. Uses shared indices (`property_launches`, `property_completed_projects`) and routes `ready-to-move` inventory into the shared completed index with city partition fields. |
| **Property Projects (Kerala/UAE/KSA)** | `src/property_projects_pipeline.py` | Daily per city | Discovers, enriches, and indexes apartments, villas, and commercial projects across configured Kerala/UAE/KSA cities using `[property_projects.<city>]` config sections into the shared `property_projects` Elasticsearch index. |
| **Market Events (UAE/KSA/Kerala)** | `src/market_events_pipeline.py` | Daily per region | Tracks major global/country/state/city/locality events that can impact property markets (wars, infrastructure, investment, mega events like FIFA/Expo, policy shifts, disasters). Ingests from free sources (Google News RSS, Guardian API, UN News, Fed/ECB press feeds, Al Jazeera, Arab News, Saudi Press Agency, World Bank, GDACS/USGS/EONET, GDELT backfill). Applies source-weighted quality scoring and minimum credibility/quality thresholds before indexing into `market_events`. |

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Plombery Web UI (:8080)             │
│          (FastAPI + Uvicorn + WebSocket)         │
└──────────────────────┬──────────────────────────┘
                       │
          ┌────────────┼────────────────┐
          ▼            ▼                ▼
   ┌────────────┐ ┌──────────┐  ┌──────────────┐
   │   Jobs     │ │  Cars    │  │  Property    │
   │ Pipelines  │ │ Pipelines│  │  Pipelines   │
   └─────┬──────┘ └────┬─────┘  └──────┬───────┘
         │              │               │
         ▼              ▼               ▼
   ┌───────────┐  ┌───────────────────────────┐
   │ Gemini AI │  │      Data Stores          │
   │ (2.0 /    │  │  PostgreSQL (Neon DB)     │
   │  1.5)     │  │  Elasticsearch 7.x        │
   └───────────┘  │  Cloudflare R2 (exports)  │
                  └───────────────────────────┘
```

### Data Flow (Jobs Pipeline)

1. **Scrape** - `python-jobspy` fetches LinkedIn job listings for configured locations.
2. **Deduplicate** - New jobs are filtered against existing hashes in PostgreSQL.
3. **Persist Raw** - Raw records are appended to PostgreSQL and bulk-indexed into Elasticsearch.
4. **AI Enrichment** - Gemini extracts structured fields (skills, seniority, company info) from raw descriptions.
5. **Persist Enriched** - Enriched records are saved to PostgreSQL and Elasticsearch.
6. **Export** - A public JSON snapshot is uploaded to Cloudflare R2 for downstream dashboards.
7. **Alert** - Subscribers receive matching job alerts via email.

## Configuration

All credentials are managed through `src/config/config.ini` (INI format). See `src/config/config.ini.template` for the full list of sections:

- **`[PostgresDB]`** - Neon PostgreSQL connection string
- **`[GeminiPro]`** - Google Gemini API keys
- **`[openrouter]`** - OpenRouter API key (optional)
- **`[elasticsearch]`** - Elasticsearch hosts, auth, index names
- **`[cloudflare]`** - Cloudflare R2 credentials and bucket config
- **`[Sendgrid]`** - SMTP credentials for alert emails
- **`[core]`** - Shared settings (base URLs, etc.)
- **`[property_launches.<city>]`** - City-specific property launch scraping settings (queries, sources, delays, schedule) writing into shared launch/completed indices with city partitioning
- **`[property_projects.<city>]`** - City-specific all-project scraping settings (Kerala/UAE/KSA; apartments/villas/commercial) writing into the shared `property_projects` index
- **`[market_events]`** - Global settings for market-events pipeline (index, source toggles, backfill window, limits)
- **`[market_events.<region>]`** - Region-specific focus geography and queries (`uae`, `ksa`, `india_kerala`) for world/country/state/city/locality events impacting real estate

## Project Structure

```
plombery-scraper/
├── src/
│   ├── app.py                    # Entry point - starts Uvicorn server
│   ├── jobs_scrape_pipeline.py   # Jobs scraping + AI enrichment pipeline
│   ├── jobs_alerts_pipeline.py   # Job alert email pipeline
│   ├── standardization_pipeline.py # Dimension standardization pipeline
│   ├── dubzl_crs.py              # Dubizzle car listings pipeline
│   ├── crswth_crs.py             # Carswitch car listings pipeline
│   ├── allsopp_crs.py            # Allsopp property listings pipeline
│   ├── acres99_crs.py            # 99acres property listings pipeline
│   ├── property_launches_pipeline.py # Multi-city property launches pipeline
│   ├── property_projects_pipeline.py # Multi-region property projects pipeline
│   ├── market_events_pipeline.py # Multi-region market events pipeline
│   ├── config/
│   │   ├── __init__.py           # Config reader (configparser)
│   │   ├── config.ini            # Local config (gitignored)
│   │   └── config.ini.template   # Template with placeholder values
│   ├── connections/
│   │   └── neondb_client.py      # Neon DB connection helper
│   └── utils/
│       ├── ai_infer.py           # Gemini API inference helper
│       └── rdbms_conn.py         # Database query executor
├── deploy/
│   └── plomberly.service         # systemd unit file for production
├── .github/workflows/
│   └── deploy.yml                # CI/CD - rsync to Oracle Cloud
├── requirements.txt
└── .gitignore
```

## Deployment

### Production (Oracle Cloud)

The GitHub Actions workflow (`.github/workflows/deploy.yml`) handles deployment:

1. Triggers on push/merge to `main` or `master`, releases, and manual dispatch.
2. Syncs files to the remote server via `rsync` over SSH.
3. Installs dependencies if `requirements.txt` changed.
4. The app runs as a systemd service (`plomberly.service`).

#### Setting up GitHub secrets

Go to your repository **Settings → Secrets and variables → Actions** and add these secrets:

| Secret | How to get it |
|---|---|
| `SSH_PRIVATE_KEY` | Generate a key pair locally: `ssh-keygen -t ed25519 -C "deploy"`. Copy the **private key** contents (`cat ~/.ssh/id_ed25519`). |
| `SSH_HOST` | The **public IP** or hostname of your Oracle Cloud instance. Find it in the Oracle Cloud Console under **Compute → Instances → [your instance]**, or run `ssh <user>@<public-ip>` to test. You can also get it via OCI CLI: `oci compute instance list --compartment-id <ocid> --query "data[0].\"public-ips\"[0]"`. |
| `SSH_USER` | The SSH username for your instance. For Oracle Cloud Ubuntu images this is typically `ubuntu`. |
| `SSH_PORT` | The SSH port (default is `22`). Check your instance's security list / NSG if you customized it. |
| `REMOTE_PATH` | The absolute path on the remote server where the app lives, e.g. `/home/ubuntu/plombery`. |

After generating the key pair, add the **public key** to the remote server's `~/.ssh/authorized_keys`:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub ubuntu@<SSH_HOST>
```

The systemd service runs:

```
/home/ubuntu/plombery/venv/bin/python /home/ubuntu/plombery/src/app.py
```

To manage the service on the remote server:

```bash
sudo systemctl start plomberly
sudo systemctl stop plomberly
sudo systemctl restart plomberly
sudo systemctl status plomberly
```

### Local Development

```bash
source venv/bin/activate
python src/app.py
```

The server starts with hot-reload enabled (`--reload`), watching for changes in the parent directory.

## Dependencies

Key dependencies:

- **plombery** - Pipeline orchestration with web UI
- **python-jobspy** - LinkedIn / Indeed / Glassdoor job scraper
- **elasticsearch 7.x** - Search and indexing
- **sqlalchemy + psycopg2** - PostgreSQL access (Neon DB)
- **boto3** - Cloudflare R2 (S3-compatible) uploads
- **requests-html** - JavaScript-rendered page loading
- **curl-cffi** - TLS-fingerprint-resistant HTTP client
- **lxml[html_clean]** - Fast HTML parsing
- **google-gemini** (via REST) - LLM-based data extraction
- **json-repair / dirtyjson / python-rapidjson** - Robust JSON parsing for LLM outputs
