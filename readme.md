# Polarapt HCM Real-Time Data Platform

[![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-CC292B?style=flat&logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)

An event-driven, real-time Human Capital Management (HCM) data backbone built on AWS, Snowflake, and SQL Server. Developed for the **SMIT Data Engineering Hackathon (Batch 05)**.

---

## 📌 Executive Summary

Polarapt HCM requires continuous ingestion, routing, and processing of external market data to power three core HCM modules:
1. **Talent Acquisition & Workforce Intelligence (Module 05 & 11):** Live US federal job posting analytics via the USAJOBS Search API.
2. **Compensation & Career Intelligence (Module 04 & 07):** Top US compensation benchmarks scraped from the Bureau of Labor Statistics (BLS).
3. **Global Payroll (Module 04):** Real-time foreign exchange (FX) rates relative to USD for global currency conversions via Open Exchange Rates.

The platform ingests public data feeds **every minute**, enforces strict **Data Contracts** as S3 object metadata, decouples downstream consumers using **SNS/SQS FIFO filtering**, and delivers processed records into specialized target analytical datastores.

---

## 🏗 System Architecture

```
                                  [ TASK 1: INGESTION ]
┌───────────────────┐      ┌─────────────────────────────┐
│  USAJOBS API      │ ───► │  lambda_jobs_ingest         │ ──────┐
└───────────────────┘      └─────────────────────────────┘       │
┌───────────────────┐      ┌─────────────────────────────┐       ▼
│  BLS Web Scraping │ ───► │  lambda_benchmarks_scraper  │ ──► [ S3 Raw Zone ]
└───────────────────┘      └─────────────────────────────┘       │  data-hackathon-smit-*
┌───────────────────┐      ┌─────────────────────────────┐       │  (Contract in Metadata)
│ OpenExchangeRates │ ───► │  lambda_fx_ingest           │ ──────┘
└───────────────────┘      └─────────────────────────────┘
                                                                 │ (S3 ObjectCreated)
                                                                 ▼
                                                        [ SNS Standard Topic ]
                                                          hcm-raw-events
                                                                 │
                                                                 ▼
                                                       ┌───────────────────┐
                                                       │ Contract Router   │
                                                       │ Lambda            │
                                                       └───────────────────┘
                                                                 │ (Republish with source attr)
                                                                 ▼
                                                       [ SNS FIFO Topic ]
                                                     hcm-routed-events.fifo
                                                                 │
                                   ┌─────────────────────────────┼─────────────────────────────┐
                    source=jobs    │                    source=benchmarks                      │ source=fx
                                   ▼                                   ▼                       ▼
                         [ SQS: jobs-queue.fifo ]           [ SQS: benchmarks-queue.fifo ]  [ SQS: fx-queue.fifo ]
                                   │                                   │                       │
                                   ▼                                   ▼                       ▼
                         ┌───────────────────┐               ┌───────────────────┐   ┌───────────────────┐
                         │ Jobs Processor    │               │ Benchmarks Proc.  │   │ FX Processor      │
                         │ Lambda / Glue     │               │ Lambda            │   │ Lambda            │
                         └───────────────────┘               └───────────────────┘   └───────────────────┘
                                   │                                   │                       │
                                   ▼                                   ▼                       ▼
                           [ Snowflake DW ]                    [ S3 Processed Zone ]    [ RDS SQL Server ]
                          HCM_DB.JOBS.POSTINGS                  processed/benchmarks/    dbo.fx_rates
```

### Key Architectural Flow
1. **Ingestion (Task 1):** EventBridge triggers ingestion Lambdas every minute. Files are pushed to `s3://<bucket-name>/raw/<source>/YYYY/MM/DD/HHMM.json`.
2. **Metadata Enriched Contract:** Each ingestion payload includes operational metadata inside the body and directly attached to the S3 object header.
3. **Decoupled Event Router:** S3 sends `ObjectCreated` events to a standard SNS Topic (`hcm-raw-events`). A router Lambda reads the metadata contract (`source`) and republishes to an SNS FIFO Topic (`hcm-routed-events.fifo`) with structured message attributes.
4. **Targeted Delivery:** SNS Subscription Filter Policies route messages into distinct SQS FIFO queues (`jobs-queue.fifo`, `benchmarks-queue.fifo`, `fx-queue.fifo`).
5. **Analytical Sink Processing (Task 2):** Dedicated processor Lambdas consume batch messages, apply schema validation/transformation, write to target datastores, and perform explicit message deletion upon successful commit.

---

## 📁 Repository Structure

```
polarapt-hcm-data-platform/
├── README.md
├── requirements.txt
├── docs/
│   ├── PROJECT_DOCUMENTATION.md
│   └── architecture.png
├── src/
│   ├── ingestion/
│   │   ├── lambda_jobs_ingest.py
│   │   ├── lambda_benchmarks_scraper.py
│   │   └── lambda_fx_ingest.py
│   ├── routing/
│   │   └── lambda_contract_router.py
│   └── processing/
│       ├── lambda_jobs_to_snowflake.py
│       ├── lambda_benchmarks_processor.py
│       └── lambda_fx_processor.py
├── sql/
│   ├── snowflake_setup.sql
│   └── sqlserver_setup.sql
└── policies/
    └── sns_access_policy.json
```

---

## 📋 Technical Data Contracts

Every raw ingested payload carries a standard metadata contract to ensure operational observability across the decoupled processing stages:

| Contract Field | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `source` | String | Data source identifier (`jobs`, `benchmarks`, `fx`) | `jobs` |
| `timestamp` | ISO-8601 | UTC generation timestamp | `2026-10-25T04:17:00Z` |
| `record_count` | Integer | Total records successfully fetched | `500` |
| `response_status` | String | Execution outcome indicator | `success` / `failed_timeout` |

*Note: S3 metadata keys are normalized to lowercase by AWS S3.*

---

## 🗄 Storage & Destination Schemas

### 1. S3 Folder Hierarchy
```text
s3://data-hackathon-smit-<yourname>/
├── raw/
│   ├── jobs/YYYY/MM/DD/HHMM.json
│   ├── benchmarks/YYYY/MM/DD/HHMM.json
│   └── fx/YYYY/MM/DD/HHMM.json
└── processed/
    └── benchmarks/YYYY/MM/DD/HHMM.json
```

### 2. Snowflake Target (`HCM_DB.JOBS.JOB_POSTINGS`)
```sql
CREATE TABLE IF NOT EXISTS HCM_DB.JOBS.JOB_POSTINGS (
    JOB_ID VARCHAR(100) PRIMARY KEY,
    TITLE VARCHAR(500),
    ORGANIZATION VARCHAR(500),
    LOCATION VARCHAR(500),
    SALARY_MIN NUMBER(12,2),
    SALARY_MAX NUMBER(12,2),
    SALARY_INTERVAL VARCHAR(50),
    OPEN_DATE DATE,
    CLOSE_DATE DATE,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3. SQL Server Target (`dbo.fx_rates`)
```sql
CREATE TABLE dbo.fx_rates (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate DECIMAL(18, 6) NOT NULL,
    fetched_at DATETIME2 NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_fx_file_curr UNIQUE(source_file, target_currency)
);
```

---

## 🚀 Deployment & Setup Instructions

### Prerequisites
* AWS Account with standard access to Lambda, EventBridge, S3, SNS, SQS, and Secrets Manager.
* Active Snowflake account and an accessible Microsoft SQL Server instance (AWS RDS or hosted).
* Python 3.12 runtime environment.

### 1. Ingestion Setup
1. Provision S3 bucket: `data-hackathon-smit-<yourname>`.
2. Deploy Ingestion Lambdas from `src/ingestion/`.
3. Configure environment variables in Lambda Console:
   * `USAJOBS_API_KEY`, `USAJOBS_EMAIL`
   * `OXR_APP_ID`
   * `BUCKET_NAME`
4. Attach AWS Lambda Layers for external packages (`requests`, `beautifulsoup4`).
5. Wire EventBridge Schedule Rules (`rate(1 minute)`) to each ingestion function.

### 2. Messaging & Routing Infrastructure
1. Create Standard SNS Topic: `hcm-raw-events`.
2. Configure S3 Event Notification for prefix `raw/` targeting `hcm-raw-events`.
3. Create FIFO SNS Topic: `hcm-routed-events.fifo` (Content-based deduplication enabled).
4. Create SQS FIFO Queues: `jobs-queue.fifo`, `benchmarks-queue.fifo`, `fx-queue.fifo`.
5. Deploy `src/routing/lambda_contract_router.py` subscribed to `hcm-raw-events`.
6. Configure SNS FIFO Subscription Filter Policies:
   * `jobs-queue.fifo`: `{"source": ["jobs"]}`
   * `benchmarks-queue.fifo`: `{"source": ["benchmarks"]}`
   * `fx-queue.fifo`: `{"source": ["fx"]}`

### 3. Target Processing Pipelines
1. Store database credentials safely in **AWS Secrets Manager** (`hcm/snowflake`, `hcm/sqlserver`).
2. Deploy processor functions located in `src/processing/`.
3. Enable EventBridge schedules to poll queues and write back target records.

---

## 🛡 Security & Cost Management

* **Zero Hardcoded Secrets:** AWS Secrets Manager dynamically provides connection parameters during runtime.
* **Least Privilege IAM:** Custom scoped IAM policies isolate S3 prefix write privileges and SQS queue access per Lambda role.
* **Rate Limits Notice:** Open Exchange Rates free tier provides 1,000 monthly requests. It is recommended to keep `fx-ingest-every-minute` disabled when not running active evaluations.

---

## 👥 Authors & Acknowledgments

* **Developer:** Data Engineering Candidate
* **Program:** SMIT Data Engineering Hackathon (Batch 05)
* **Date:** October 25, 2026