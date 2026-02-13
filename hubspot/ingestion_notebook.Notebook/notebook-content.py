# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

CLIENT_ID = ""
CLIENT_SECRET = ""
REFRESH_TOKEN = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
import builtins
from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import notebookutils
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import os
import threading
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lakehouse_path(lakehouse_name: str) -> str:
    try:
        lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
    except:
        raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")
    
    return lakehouse_details.get('properties', {}).get('abfsPath') + '/Tables'

workspace_info = notebookutils.lakehouse.list() 
if not workspace_info:
    raise Exception("No lakehouses found in the workspace.")

WORKSPACE_ID = workspace_info[0].get('workspaceId') 

STAGING_LAKEHOUSE_PATH = get_lakehouse_path('Staging_Lakehouse')
BRONZE_LAKEHOUSE_PATH = get_lakehouse_path('Bronze_Lakehouse')
SILVER_LAKEHOUSE_PATH = get_lakehouse_path ('Silver_Lakehouse')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp = requests.post(
    "https://api.hubapi.com/oauth/v1/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    },
   timeout=30,
)

print(resp.status_code, resp.text)  

resp.raise_for_status()
ACCESS_TOKEN = resp.json()["access_token"]
print("Access token obtained.")
print(resp.headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

CLIENT_ID = "ecc9ac61-ccbc-4974-be41-b587129ff708"
CLIENT_SECRET = "ee6e5bc8-35f5-45cb-8474-567874e9d7d6"
REFRESH_TOKEN = "eu1-6105-5e5c-4bb3-bdf1-d802309feda0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_access_token():
    token_url = f"https://api.hubapi.com/oauth/v1/token"
    token_data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }
    token_response = requests.post(token_url, data=token_data, timeout=30)
    access_token = token_response.json().get("access_token")

    return access_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ACCESS_TOKEN = get_access_token()
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### all columns 

# CELL ********************

BASE_URL       = "https://api.hubapi.com"
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"


TIMEOUT   = 60

PAGE_SIZE   = 100     # contacts per page
# PAGES       =''      # number of pages to fetch (set 3-4 as you prefer)
ID_CHUNK    = 100     # IDs per batch/read POST call
PROP_CHUNK  = 80      # properties per chunk (tune between 60-100)
PARALLELISM = 6       # number of concurrent property-chunk tasks

table_name = "br_hs_contacts_all_parallelly"
table_path = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def _parse_int(hdrs: dict, name: str, default: int) -> int:
    """Case-insensitive int parse from headers."""
    try:
        return int(hdrs.get(name, hdrs.get(name.lower(), default)))
    except Exception:
        return default

def _compute_rate_limit_sleep(hdrs: dict) -> float:
    interval_ms  = _parse_int(hdrs, "x-hubspot-ratelimit-interval-milliseconds", 10_000) 
    max_interval = _parse_int(hdrs, "x-hubspot-ratelimit-max", 110)
    rem_interval = _parse_int(hdrs, "x-hubspot-ratelimit-remaining", max_interval)

    max_second   = _parse_int(hdrs, "x-hubspot-ratelimit-secondly", 11)
    rem_second   = _parse_int(hdrs, "x-hubspot-ratelimit-secondly-remaining", max_second)

    if rem_second > 0 and rem_interval > 0:
        return 0.0
    if rem_second <= 0:
        return 1.0
    if rem_interval <= 0:
        return max(2.0, interval_ms / 2000.0)
    return 0.05

def hubspot_request_rate_aware_no_retry(method, url, headers, params=None, json=None, timeout=60):
    method = method.upper()
    if method not in ("GET", "POST"):
        raise ValueError("method must be 'GET' or 'POST'")

    resp = requests.request(method, url, headers=headers, params=params, json=json, timeout=timeout)

   
    if resp.status_code == 429:
        pace_sleep = _compute_rate_limit_sleep(resp.headers)
        if pace_sleep > 0:
            time.sleep(pace_sleep)

        resp.raise_for_status()

    if method == "GET":
        resp.raise_for_status()
    else:
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")

    pace_sleep = _compute_rate_limit_sleep(resp.headers)
    if pace_sleep > 0:
        time.sleep(pace_sleep)

    return resp.json(), dict(resp.headers), resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def fetch_contacts_all_pages_rate_aware(contacts_url, headers, limit=100, max_pages=4, timeout=60):
    contact_ids = []
    meta_rows   = []

    after = None
    pages = 0

    while True:
        params = {"limit": limit}
        if after:
            params["after"] = after

        list_json, list_hdrs, status = hubspot_request_rate_aware_no_retry(
            "GET", contacts_url, headers=headers, params=params, timeout=timeout
        )

        results = list_json.get("results", [])
        if not results:
            break

        for r in results:
            cid = r.get("id")
            contact_ids.append(cid)
            meta_rows.append((cid, r.get("createdAt"), r.get("updatedAt"), r.get("archived")))

        nxt = (list_json.get("paging") or {}).get("next")
        after = nxt.get("after") if nxt else None

        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        if not after:
            break

    return contact_ids, meta_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def fetch_all_pages_objects(base_url, object_name, headers, limit=100, properties=None, timeout=60, max_pages=None):

    url = f"{base_url}/crm/v3/objects/{object_name}"
    all_records = []
    after = None
    pages = 0

    while True:
        params = {"limit": limit}
        if properties:
            params["properties"] = ",".join(properties)
        if after:
            params["after"] = after

        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        result = resp.json()

        batch = result.get("results", [])
        all_records.extend(batch)

        next_obj = (result.get("paging") or {}).get("next")
        after = next_obj.get("after") if next_obj else None
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        if not after:
            break

    return all_records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def fetch_all_pages_contacts(contacts_url, headers, limit=100, properties=None, timeout=60):

    all_records = []
    meta_rows   = []
    after = None

    while True:
        params = {"limit": limit}
        if properties:
            params["properties"] = ",".join(properties)
        if after:
            params["after"] = after

        resp = requests.get(contacts_url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        result = resp.json()

        batch = result.get("results", []) or []
        if not batch:
            break

        all_records.extend(batch)

        # Build meta rows
        for r in batch:
            meta_rows.append((
                r.get("id"),
                r.get("createdAt"),
                r.get("updatedAt"),
                r.get("archived"),
            ))

        # Pagination cursor
        next_obj = (result.get("paging") or {}).get("next")
        after = next_obj.get("after") if next_obj else None

        if not after:
            break

    return all_records, meta_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

props_json, props_hdrs, status = hubspot_request_rate_aware_no_retry("GET", PROPS_URL, headers=HEADERS, timeout=TIMEOUT)
ALL_PROPERTIES = [p["name"] for p in props_json.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))

all_records, meta_rows = fetch_all_pages_contacts(
    contacts_url=CONTACTS_URL,
    headers=HEADERS,
    limit=PAGE_SIZE,
    properties=None,  
    timeout=TIMEOUT
)

CONTACT_IDS = [r.get("id") for r in all_records]

print(f"Total contacts collected: {len(CONTACT_IDS)}")
print(f"Total meta rows collected: {len(meta_rows)}")

meta_schema = StructType([
    StructField("id",        StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("archived",  StringType(), True),
])
spark_wide_df = spark.createDataFrame(meta_rows, schema=meta_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

ids_b = sc.broadcast(CONTACT_IDS)

def fetch_chunk(props_to_fetch, id_chunk_size=ID_CHUNK):

    per_id = {}
    for id_chunk in (ids_b.value[i:i+id_chunk_size] for i in range(0, len(ids_b.value), id_chunk_size)):
        body = {"properties": props_to_fetch, "inputs": [{"id": cid} for cid in id_chunk]}
        batch_json, batch_hdrs, status = hubspot_request_rate_aware_no_retry(
            "POST", BATCH_READ_URL, headers=HEADERS, json=body, timeout=TIMEOUT
        )
        results = batch_json.get("results", [])
        for r in results:
            cid = r.get("id")
            props = (r.get("properties") or {})
            # initialize dict per id
            if cid not in per_id:
                per_id[cid] = {}
            # string-ify values for explicit StringType columns
            for k, v in props.items():
                per_id[cid][k] = (str(v) if v is not None else None)

    return {"requested": list(props_to_fetch), "per_id": per_id}

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

prop_chunks = list(chunkify(ALL_PROPERTIES, PROP_CHUNK))
num_slices = builtins.min(len(prop_chunks), PARALLELISM)

chunk_payloads = sc.parallelize(prop_chunks, numSlices=num_slices) \
                   .map(fetch_chunk) \
                   .collect()


added_props = set()
for idx, payload in enumerate(chunk_payloads, start=1):
    requested_props = payload["requested"]
    per_id_map      = payload["per_id"]

    new_cols = [p for p in requested_props if p not in added_props]
    if not new_cols:
        print(f"Chunk {idx}: no new columns to add, skipping.")
        continue

    schema = StructType([StructField("id", StringType(), True)] +
                        [StructField(p, StringType(), True) for p in new_cols])

    # Build rows aligned to schema for all CONTACT_IDS
    data_rows = []
    for cid in CONTACT_IDS:
        row_vals = [cid]
        props_for_id = per_id_map.get(cid, {})
        for p in new_cols:
            row_vals.append(props_for_id.get(p))  # None if missing
        data_rows.append(tuple(row_vals))

    spark_chunk_df = spark.createDataFrame(data_rows, schema=schema)

    # LEFT JOIN on id to enrich
    spark_wide_df = spark_wide_df.join(spark_chunk_df, on="id", how="left")

    added_props.update(new_cols)
    print(f"Joined chunk {idx} with {len(new_cols)} properties. Total columns: {len(spark_wide_df.columns)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

spark_wide_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ###### optimized

# CELL ********************

BASE_URL = "https://api.hubapi.com"
PROPS_URL_DEFAULT    = f"{BASE_URL}/crm/v3/properties/contacts"
BATCH_READ_URL       = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE    = 100     # HubSpot max per page for objects
TIMEOUT      = 60
ID_CHUNK     = 100     # HubSpot batch read allows up to 100 ids
PROP_CHUNK   = 80     # tune based on property count/response size
PARALLELISM  = 16      # tune based on cluster resources

CONFIG_TABLE = f"{BRONZE_LAKEHOUSE_PATH}/br_hs_config"
print(CONFIG_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def hubspot_request_get(url, headers=None, params=None, timeout=60):
    """Simple GET with basic 429 handling (sleep & retry once)."""
    headers = headers or {}
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "2"))
        time.sleep(retry_after)
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json(), resp.headers, resp.status_code

def hubspot_request_post(url, headers=None, json_body=None, timeout=60):
    """Simple POST with basic 429 handling (sleep & retry once)."""
    headers = headers or {}
    resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "2"))
        time.sleep(retry_after)
        resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
    resp.raise_for_status()
    return resp.json(), resp.headers, resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONFIG_TABLE = f"{BRONZE_LAKEHOUSE_PATH}/br_hs_config"

def update_config_date_by_table_name(table_name_value: str, source_value: str, new_last_sync: str):
    dt = DeltaTable.forName(spark, CONFIG_TABLE)

    dt.update(
        condition=(col("table") == table_name_value) & (col("source") == source_value),
        # set={"last_sync": lit(new_last_sync)}
        set={"last_sync": to_timestamp(lit(new_last_sync))}
    )

    print(f"[config] Updated last_sync for table='{table_name_value}', source='{source_value}' → {new_last_sync}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_all_pages_contacts(contacts_url, headers, limit=PAGE_SIZE, properties=None, timeout=TIMEOUT):
    all_records = []
    meta_rows   = []
    after = None

    while True:
        params = {"limit": limit}
        if properties:
            params["properties"] = ",".join(properties)
        if after:
            params["after"] = after

        result, _, _ = hubspot_request_get(contacts_url, headers=headers, params=params, timeout=timeout)

        batch = result.get("results", []) or []
        if not batch:
            break

        all_records.extend(batch)

        # Build meta rows (id, createdAt, updatedAt, archived) – aligns with your working code.
        for r in batch:
            meta_rows.append((
                r.get("id"),
                r.get("createdAt"),
                r.get("updatedAt"),
                r.get("archived"),
            ))

        next_obj = (result.get("paging") or {}).get("next")
        after = next_obj.get("after") if next_obj else None
        if not after:
            break

    return all_records, meta_rows


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_chunk(props_to_fetch, ids_b, headers, timeout=TIMEOUT):
    per_id = {}
    for id_chunk in (ids_b.value[i:i+ID_CHUNK] for i in range(0, len(ids_b.value), ID_CHUNK)):
        body = {"properties": props_to_fetch, "inputs": [{"id": cid} for cid in id_chunk]}
        batch_json, _, _ = hubspot_request_post(BATCH_READ_URL, headers=headers, json_body=body, timeout=timeout)
        results = batch_json.get("results", []) or []
        for r in results:
            cid = r.get("id")
            props = (r.get("properties") or {})
            if cid not in per_id:
                per_id[cid] = {}
            # Stringify values to keep schema stable in bronze, as in your code
            for k, v in props.items():
                per_id[cid][k] = (str(v) if v is not None else None)
    return {"requested": list(props_to_fetch), "per_id": per_id}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_limited(object_url, fields_list, table_name):
    print(f"[limited] Collecting {len(fields_list)} properties from {object_url}")

    # 1) Fetch HubSpot pages with the limited property set
    all_records, meta_rows = fetch_all_pages_contacts(
        contacts_url=object_url,
        headers=HEADERS,
        limit=PAGE_SIZE,
        properties=fields_list,
        timeout=TIMEOUT
    )

    if not all_records:
        raise RuntimeError("No contacts returned from HubSpot (limited mode).")

    df = spark.createDataFrame(all_records)

    # 3) Build column list: meta fields + selected properties (cast to string)
    props_col = F.col("properties")
    meta_cols = ["id", "createdAt", "updatedAt", "archived"]

    # Validate meta columns exist; if not, try to derive them or add nulls
    for c in meta_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    if "properties" not in df.columns:
        df = df.withColumn(
            "properties",
            F.map_from_arrays(
                F.array(*[F.lit(p) for p in fields_list]),
                F.array(*[
                    F.when(F.col(p).isNotNull(), F.col(p).cast(StringType()))
                     .otherwise(F.lit(None).cast(StringType()))
                    if p in df.columns else F.lit(None).cast(StringType())
                    for p in fields_list
                ])
            )
        )

    # 4) Build selected columns
    selected_cols = [F.col(c) for c in meta_cols]
    for p in fields_list:
        # Safely extract from properties map; cast to string
        selected_cols.append(
            F.col("properties").getItem(p).cast(StringType()).alias(p)
        )

    spark_df = df.select(*selected_cols)

    # 5) Ensure all requested property columns exist (fill missing with NULL)
    for p in fields_list:
        if p not in spark_df.columns:
            spark_df = spark_df.withColumn(p, F.lit(None).cast(StringType()))

    # 6) Order columns deterministically
    ordered_cols = meta_cols + fields_list
    spark_df = spark_df.select(*ordered_cols)

    # 7) Write as Delta
    table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
    print(f"[limited] Writing Delta to: {table_path}")

    (spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path))

    return {"contacts": spark_df.count(), "columns": len(spark_df.columns)}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_all(object_url, properties_url, table_name):
    print(f"[all] Discovering properties from: {properties_url}")
    props_json, _, _ = hubspot_request_get(properties_url, headers=HEADERS, timeout=TIMEOUT)
    ALL_PROPERTIES = [p["name"] for p in props_json.get("results", [])]
    print(f"[all] Property count: {len(ALL_PROPERTIES)}")

    print(f"[all] Collecting contact meta from: {object_url}")
    all_records, meta_rows = fetch_all_pages_contacts(
        contacts_url=object_url,
        headers=HEADERS,
        limit=PAGE_SIZE,
        properties=None,  # meta only in page scan
        timeout=TIMEOUT
    )
    if not meta_rows:
        raise RuntimeError("No contacts returned from HubSpot (all mode).")

    CONTACT_IDS = [r.get("id") for r in all_records]
    print(f"[all] Total contacts: {len(CONTACT_IDS)}")

    # Base meta schema
    meta_schema = StructType([
        StructField("id",        StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("updatedAt", StringType(), True),
        StructField("archived",  StringType(), True),
    ])
    spark_wide_df = spark.createDataFrame(meta_rows, schema=meta_schema).persist()

    prop_chunks = list(chunkify(ALL_PROPERTIES, PROP_CHUNK))
    num_slices  = min(len(prop_chunks), PARALLELISM)  # <- use min() directly
    ids_b       = spark.sparkContext.broadcast(CONTACT_IDS)

    def _map_fetch(props_to_fetch):
        return fetch_chunk(props_to_fetch, ids_b=ids_b, headers=HEADERS, timeout=TIMEOUT)

    print(f"[all] Fetching {len(prop_chunks)} property chunks across {num_slices} slices...")
    chunk_payloads = spark.sparkContext.parallelize(prop_chunks, numSlices=num_slices) \
                                       .map(_map_fetch) \
                                       .collect()


    added_props = set()
    for idx, payload in enumerate(chunk_payloads, start=1):
        requested_props = payload["requested"]
        per_id_map      = payload["per_id"]

        new_cols = [p for p in requested_props if p not in added_props]
        if not new_cols:
            print(f"[all] Chunk {idx}: no new columns to add, skipping.")
            continue

        schema = StructType([StructField("id", StringType(), True)] +
                            [StructField(p, StringType(), True) for p in new_cols])

        data_rows = []
        for cid in CONTACT_IDS:
            row_vals = [cid]
            props_for_id = per_id_map.get(cid, {})
            for p in new_cols:
                row_vals.append(props_for_id.get(p))  
            data_rows.append(tuple(row_vals))

        spark_chunk_df = spark.createDataFrame(data_rows, schema=schema)


        spark_wide_df = spark_wide_df.join(F.broadcast(spark_chunk_df), on="id", how="left")

        added_props.update(new_cols)
        if idx % 5 == 0:
            spark_wide_df = spark_wide_df.checkpoint(eager=True)
        print(f"[all] Joined chunk {idx} with {len(new_cols)} properties. Total columns: {len(spark_wide_df.columns)}")

    meta_cols = ["id", "createdAt", "updatedAt", "archived"]
    prop_cols = sorted([c for c in spark_wide_df.columns if c not in meta_cols])
    ordered_cols = meta_cols + prop_cols
    spark_wide_df = spark_wide_df.select(*ordered_cols)

    table_path  = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"
    print(f"[all] Writing Delta to: {table_path}")
    (spark_wide_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path))

    return {"contacts": len(CONTACT_IDS), "columns": len(spark_wide_df.columns)}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_from_config():
    cfg_df = spark.table(CONFIG_TABLE).filter(F.col("isActive") == True)
    cfg_rows = cfg_df.collect()
    if not cfg_rows:
        print("No active config rows found.")
        return


    for row in cfg_rows:
        table_name   = row["table"]
        source       = row["source"]          
        last_sync    = row["last_sync"]
        object_url   = row["URL"]              
        fields_str   = (row["fields"] or "").strip()


        properties_url = PROPS_URL_DEFAULT
        if "properties_url" in row.asDict():
            properties_url = row["properties_url"] or PROPS_URL_DEFAULT

        print(f"=== Running source='{source}' table='{table_name}' ===")
        print(f"object_url={object_url} | properties_url={properties_url} | fields='{fields_str}'")

        if not object_url:
            object_url = "URL"

        if fields_str == "*" or fields_str.lower() == "all":
            stats = run_all(object_url=object_url, properties_url=properties_url, table_name=table_name)
        else:
            fields_list = [f.strip() for f in fields_str.split(",") if f.strip()]
            if not fields_list:
                raise RuntimeError(f"Config row for table='{table_name}' has empty fields list.")
            stats = run_limited(object_url=object_url, fields_list=fields_list, table_name=table_name)

        print(f"Completed table='{table_name}': contacts={stats['contacts']}, columns={stats['columns']}")

# if __name__ == "__main__":
#     run_from_config()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def run_from_config(target_table: str = None):
    cfg_df = spark.read.format("delta").load(CONFIG_TABLE)
    cfg_df = cfg_df.filter(col('isactive') == lit(True))
    for row in cfg_df.collect():
        row_dict       = row.asDict()
        table_name     = row_dict["table"]
        source         = row_dict.get("source")
        last_sync      = row_dict.get("last_sync")
        object_url     = (row_dict.get("URL") or "").strip()    # OBJECT URL from config
        fields_str     = (row_dict.get("fields") or "").strip()
        properties_url =  PROPS_URL_DEFAULT

        print(f"=== Running source='{source}' table='{table_name}' ===")
        print(f"object_url={object_url} | properties_url={properties_url} | fields='{fields_str}'")

        if not object_url:
            raise RuntimeError(f"Config row for table='{table_name}' is missing OBJECT URL in column 'URL'.")

        if fields_str == "*" or fields_str.lower() == "all":
            stats = run_all(object_url=object_url, properties_url=properties_url, table_name=table_name)
        else:
            fields_list = [f.strip() for f in fields_str.split(",") if f.strip()]
            if not fields_list:
                raise RuntimeError(f"Config row for table='{table_name}' has empty fields list.")
            stats = run_limited(object_url=object_url, fields_list=fields_list, table_name=table_name)

        print(f"Completed table='{table_name}': contacts={stats['contacts']}, columns={stats['columns']}")


# Run all active rows
run_from_config()

# Or run a single table by name:
# run_from_config(target_table="br_hs_contacts_all")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
