# Hands-on Lab: Verifying Data Quality for a Data Warehouse

## Dependencies

- pandas
- tabulate
- psycopg2

## Step 1: Run the setup script.

```bash
bash setup_staging_area.sh
```

> Make sure update the db connection with your own credentials.

## Step 2: Run the command below to generate a sample data quality report.

```bash
python3 generate-data-quality-report.py
```
