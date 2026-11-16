import os
import requests
import json
import pyodbc
import uuid
from datetime import date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.environ["EIA_API_KEY"]
BASE = "https://api.eia.gov/v2"
ROUTE = "electricity/rto/fuel-type-data/data/"
PAGE_SIZE = 2000

#connection info and credentials for SQL authentication
conn = pyodbc.connect(
    "DRIVER={ODBC DRIVER 18 for SQL Server};"
    f"SERVER={os.environ.get('SQL_SERVER', '10.0.2.2,1433')};"
    "Database=Energy;"
    f"UID={os.environ['SQL_USER']};"
    f"PWD={os.environ['SQL_PASSWORD']};"
    "TrustServerCertificate=yes;",
    timeout=30,
)
cur = conn.cursor()
cur.fast_executemany = True #speedup bulk additions to the database

#establish an HTTPS session with automatic retries 
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True,
)))

def get_data(route, params):
    """Fetch every page of data from the EIA API for a given route and parameters."""
    url = f"{BASE}/{route}"
    all_data = []
    offset = 0

    while True:
        query = {
            "api_key": API_KEY,
            "offset": offset,
            "length": PAGE_SIZE,
            **params
        }
        response = requests.get(url, params=query, timeout=(10, 60)) #10s to connect, 60s to read 
        response.raise_for_status()
        payload = response.json()["response"]

        batch = payload["data"]
        all_data.extend(batch)

        total = int(payload["total"])
        offset += PAGE_SIZE
        if offset >= int(payload["total"]) or not batch:
            break

    return all_data

def month_windows(start,end):
    cur = start
    while cur <= end:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) #finds the first day of the next month without needing to know how long the current month is
        yield cur, min(nxt - timedelta(days=1), end)
        cur = nxt

def load_staging(records, batch_id):
    cur.executemany(
        "INSERT INTO stg.eia_hourly_generation_bytype_raw (batch_id, payload) VALUES (?, ?)",
        [(batch_id, json.dumps(rec)) for rec in records],
    )

#Fuel types are defined independent from the parameters dictionary in order to chunk API calls to one fuel at a time
FUEL_TYPES = ["SUN", "WAT", "NG", "NUC", "OIL", "COL", "GEO", "SNB", "WNB", "BAT"]

#EIA parameters for what data to pull and how to sort the pages. 
#This project only filters for California energy data "CAL" to keep the database from getting too large
base_params = {
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]":"CAL",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
}

#SQL Query string to update the target database with the most recent data without creating duplicate rows. 
#Data is loading into the staging database in JSON format and then interpreted into usable rows and columns 
MERGE_SQL = """
MERGE dbo.eia_hourly_generation_bytype AS t
USING (
    SELECT
        CONVERT(DATETIME2(0), period_raw + ':00:00') AS period,
        respondent,
        respondent_name,
        fueltype,
        fuel_name,
        value,
        value_units
    FROM (
        SELECT
            j.period_raw,
            j.respondent,
            j.respondent_name,
            j.fueltype,
            j.fuel_name,
            j.value,
            j.value_units,
            ROW_NUMBER() OVER (
                PARTITION BY j.period_raw, j.respondent, j.fueltype
                ORDER BY r.id DESC
            ) AS rn
        FROM stg.eia_hourly_generation_bytype_raw r
        CROSS APPLY OPENJSON(r.payload) WITH (
            period_raw       VARCHAR(20)   '$.period',
            respondent       VARCHAR(20)   '$.respondent',
            respondent_name  NVARCHAR(200) '$."respondent-name"',
            fueltype         VARCHAR(20)   '$.fueltype',
            fuel_name        NVARCHAR(100) '$."type-name"',
            value            INT           '$.value',
            value_units      VARCHAR(30)   '$."value-units"'
        ) j
        WHERE r.batch_id = ?
    ) x
    WHERE rn = 1
) AS s
ON  t.period     = s.period
AND t.respondent = s.respondent
AND t.fueltype   = s.fueltype
WHEN MATCHED THEN UPDATE SET
    respondent_name = s.respondent_name,
    fuel_name       = s.fuel_name,
    value           = s.value,
    value_units     = s.value_units,
    updated_at      = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (period, respondent, respondent_name, fueltype, fuel_name, value, value_units)
VALUES
    (s.period, s.respondent, s.respondent_name, s.fueltype, s.fuel_name, s.value, s.value_units);
"""

# Main function that makes the API call, loads the JSON data into a staging database, and then runs a SQL query to sort the raw JSON data into the target table
if __name__ == "__main__":
    batch_id = str(uuid.uuid4())
    try:
        total = 0
        for start, end in month_windows(date(2026, 1, 1), date.today()): #grabs all data from the start of 2026 to the current date, limiting each request to one month of data for one fuel type
            for fuel in FUEL_TYPES: 
                params = {
                    **base_params,
                    "facets[fueltype][]": fuel,
                    "start": f"{start}T00",
                    "end": f"{end}T23",
                }
                records = get_data(ROUTE, params)
                if records:
                    load_staging(records, batch_id)
                    conn.commit() #keep progress after each chunk
                total += len(records)
                print(f"{start:%Y-%m} {fuel}: {len(records)} records")

        print(f"Fetched {total} records")
        cur.execute(MERGE_SQL, batch_id)
        conn.commit()
        print(f"Loaded batch {batch_id}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()