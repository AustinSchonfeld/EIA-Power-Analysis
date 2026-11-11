import os
import requests
import json
import pyodbc
import uuid

API_KEY = os.environ["EIA_API_KEY"]
BASE = "https://api.eia.gov/v2"
ROUTE = "electricity/rto/fuel-type-data/data/"
PAGE_SIZE = 5000

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
        response = requests.get(url, params=query, timeout=10)
        response.raise_for_status()
        payload = response.json()["response"]

        batch = payload["data"]
        all_data.extend(batch)

        total = int(payload["total"])
        offset += PAGE_SIZE
        if offset >= total or not batch:
            break

    return all_data

def load_staging(records, batch_id):
    cur.executemany(
        "INSERT INTO stg.eia_hourly_generation_bytype_raw (batch_id, payload) VALUES (?, ?)",
        [(batch_id, json.dumps(rec)) for rec in records],
    )

params = {
    "frequency": "hourly",
    "data[0]": "value",
    "facets[fueltype][]": "SUN",
    "facets[respondent][]":"CAL",
    "start": "2026-01-01",
    "end": "2026-11-10",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc"
}

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

if __name__ == "__main__":
    batch_id = str(uuid.uuid4())
    try:
        records = get_data(ROUTE, params)
        print(f"Fetched {len(records)} records")

        load_staging(records, batch_id)
        cur.execute(MERGE_SQL, batch_id)
        conn.commit()
        print(f"Loaded batch {batch_id}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()