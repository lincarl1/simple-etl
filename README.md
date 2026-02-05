# ETL Pipeline - Mock Benefits Engagement

## Overview
Simple ETL data pipeline for Mock Benefits Engagment

**Pipeline**:
1. Extracts raw events and users data
2. Transforms raw events data
3. Joins cleaned raw events data with users and enriches
4. Creates simple daily aggregate: number of events per user per date
5. Outputs the results as Parquet Files

## Requirements
- Python 3.12.3
- pandas
- pyarrow

## How to run code:
1. Create virtual environment

```bash
python3 -m venv .jellyvision
```

2. Activate virtual environment

```bash
source .jellyvision/bin/activate
```

3. Install Dependencies

```bash
pip install -r requirements.txt
```

4. Run program

```bash
python3 etl.py
```

## Next Steps

There are a couple of things that I would consider for enhancement:
1. I would create unit tests (pytest) to ensure that our pipeline functions as expected
2. I would consider moving this ETL pipeline to an orchestration tool (Apache Airflow, Dagster, Prefect, etc.) to make sure that it runs on a schedule
3. I would consider adding an alerting mechanism (e-mail, slack message, etc.) for failures
4. I would consider using cloud technologies for saving the parquets (S3, IBM COS)