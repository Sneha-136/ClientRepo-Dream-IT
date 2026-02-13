# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import time
import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from delta.tables import DeltaTable
from pyspark.sql import *
from cryptography.fernet import Fernet
from notebookutils import mssparkutils, lakehouse
from pyspark.sql.types import *
import re
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

def get_lakehouse_path(lakehouse_name: str) -> str:
    try:
        lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
    except:
        raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")
    
    return lakehouse_details.get('properties', {}).get('abfsPath') 
    # + '/Tables'

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


token_resp = requests.post(
    "https://api.hubapi.com/oauth/v1/token",
    headers={"Content-Type": "application/json"},
    data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    },
    timeout=30,
)

if token_resp.status_code != 200:
    raise RuntimeError(f"Token refresh failed: {token_resp.status_code} {token_resp.text}")

ACCESS_TOKEN = token_resp.json()["access_token"]
print("Access token obtained.")


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

print(resp.status_code, resp.text)  # helpful to see exact error JSON

resp.raise_for_status()
ACCESS_TOKEN = resp.json()["access_token"]
print("Access token obtained.")
print(resp.headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import time
import pandas as pd

BASE_URL = "https://api.hubapi.com"
CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts"

headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
params = {
    "limit": 100,
    "properties": ["email", "firstname", "lastname", "phone", "company"]
}

all_rows = []
after = None

while True:
    page_params = dict(params)
    if after:
        page_params["after"] = after

    resp = requests.get(CONTACTS_URL, headers=headers, params=page_params, timeout=60)

    # Simple rate-limit handling: if 429 Too Many Requests, honor Retry-After
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(CONTACTS_URL, headers=headers, params=page_params, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(f"HubSpot API error [{resp.status_code}]: {resp.text}")

    j = resp.json()
    batch = j.get("results", [])
    all_rows.extend(batch)

    nxt = (j.get("paging") or {}).get("next")
    after = nxt.get("after") if nxt else None
    if not after:
        break

print(f"Total contacts fetched: {len(all_rows)}")

df_all = pd.json_normalize(all_rows)
df_all.head(10)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL = "https://api.hubapi.com"
HEADERS  = {"Authorization": f"Bearer {ACCESS_TOKEN}"} 


prop_resp = requests.get(f"{BASE_URL}/crm/v3/properties/contacts", headers=HEADERS, timeout=60)
if prop_resp.status_code != 200:
    raise RuntimeError(f"Properties API failed [{prop_resp.status_code}]: {prop_resp.text}")

prop_data = prop_resp.json()
ALL_PROPERTIES = [p["name"] for p in prop_data.get("results", [])]
print("Discovered property count:", len(ALL_PROPERTIES))
print("Sample:", ALL_PROPERTIES)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests, time
import pandas as pd
from itertools import islice

BASE_URL    = "https://api.hubapi.com"
HEADERS     = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
CONTACTS_URL= f"{BASE_URL}/crm/v3/objects/contacts"
PAGE_SIZE   = 100
CHUNK_SIZE  = 80   # keep URL reasonable; adjust if needed

# Discover all properties first (same as Fix A)
PROP_URL = f"{BASE_URL}/crm/v3/properties/contacts"
prop_resp = requests.get(PROP_URL, headers=HEADERS, timeout=60)
prop_resp.raise_for_status()
ALL_PROPERTIES = [p["name"] for p in prop_resp.json().get("results", [])]
print("Property count:", len(ALL_PROPERTIES))

def chunks(lst, n):
    it = iter(lst)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk

def fetch_chunk(chunk_props):
    all_rows = []
    after = None
    props_param = ",".join(chunk_props)
    while True:
        params = {"limit": PAGE_SIZE, "properties": props_param}
        if after:
            params["after"] = after

        resp = requests.get(CONTACTS_URL, headers=HEADERS, params=params, timeout=60)
        if resp.status_code == 429:
            wait_s = int(resp.headers.get("Retry-After", "2"))
            time.sleep(wait_s)
            resp = requests.get(CONTACTS_URL, headers=HEADERS, params=params, timeout=60)

        if resp.status_code != 200:
            raise RuntimeError(f"GET contacts error [{resp.status_code}]: {resp.text}")

        j = resp.json()
        batch = j.get("results", [])
        all_rows.extend(batch)

        # nxt = (j.get("paging") or {}).get("next")
        # after = nxt.get("after") if nxt else None
        # if not after:
        break
    return pd.json_normalize(all_rows)

# Pull first chunk to get base set and meta columns
dfs = []
for chunk in chunks(ALL_PROPERTIES, CHUNK_SIZE):
    df_chunk = fetch_chunk(chunk)
    # ensure meta columns present
    for c in ["id","createdAt","updatedAt","archived"]:
        if c not in df_chunk.columns:
            df_chunk[c] = None
    dfs.append(df_chunk)

# # Outer-merge all chunks on 'id' to build the wide table
# df_merged = dfs[0]
# for df_next in dfs[1:]:
#     df_merged = df_merged.merge(df_next, on=["id","createdAt","updatedAt","archived"], how="outer")

# # # Optionally strip 'properties.' prefix
# for col in list(df_merged.columns):
#     if col.startswith("properties."):
#         df_merged.rename(columns={col: col.replace("properties.","")}, inplace=True)

spark_df = spark.createDataFrame(df_chunk)
table_name = "br_hs_contacts"
table_path = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"
spark_df.write.format("delta").mode("overwrite").save(table_path)
# print(f"Saved {spark_df.count()} rows to bronze_hubspot_contacts")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests
import time
import pandas as pd

# =========================
# Config
# =========================
BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE      = 100         # only one page
PROP_CHUNK     = 80          # tune if needed
TIMEOUT        = 60

table_name     = "br_hs_contacts"
table_path     = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"


def hubspot_get(url, params=None):
    """GET with basic 429 retry."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def hubspot_post(url, json_body):
    """POST with basic 429 retry."""
    resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")
    return resp.json()

def chunks(lst, n):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def strip_properties_prefix_spark(df):
    """Remove 'properties.' prefix from any columns in a Spark DF."""
    for col_name in df.columns:
        if col_name.startswith("properties."):
            df = df.withColumnRenamed(col_name, col_name.replace("properties.", ""))
    return df

props_json = hubspot_get(PROPS_URL)
ALL_PROPERTIES = [p["name"] for p in props_json.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))  # ~864


list_json = hubspot_get(CONTACTS_URL, params={"limit": PAGE_SIZE})
first_page = list_json.get("results", [])
if not first_page:
    raise RuntimeError("No contacts returned in the first page.")

CONTACT_IDS = [c["id"] for c in first_page]
print("Row count (first page):", len(CONTACT_IDS))  # expect 100 (or fewer if account has fewer contacts)

# Build Spark meta DF from the first page (id + meta only)
meta_pdf = pd.json_normalize(first_page)
# Ensure meta columns present even if empty
for c in ["id", "createdAt", "updatedAt", "archived"]:
    if c not in meta_pdf.columns:
        meta_pdf[c] = None
meta_pdf = meta_pdf[["id", "createdAt", "updatedAt", "archived"]]
spark_wide_df = spark.createDataFrame(meta_pdf)  # our base

# Track what properties have been added to avoid duplicates
added_props = set()  # property names already present in spark_wide_df

# =========================
# 3) For the same 100 IDs, batch-read properties in chunks and Spark-join
# =========================
chunk_idx = 0
for prop_chunk in chunks(ALL_PROPERTIES, PROP_CHUNK):
    # Only fetch properties we haven't added yet
    props_to_fetch = [p for p in prop_chunk if p not in added_props]
    if not props_to_fetch:
        continue  # nothing new in this chunk

    chunk_idx += 1
    body = {
        "properties": props_to_fetch,
        "inputs": [{"id": cid} for cid in CONTACT_IDS],
    }
    batch_json = hubspot_post(BATCH_READ_URL, json_body=body)
    results = batch_json.get("results", [])

    # Normalize to pandas then to Spark
    chunk_pdf = pd.json_normalize(results)
    # Ensure id exists even if the response is sparse
    if "id" not in chunk_pdf.columns:
        chunk_pdf["id"] = CONTACT_IDS  # fallback; typically 'id' is present
    spark_chunk_df = spark.createDataFrame(chunk_pdf)

    # Strip 'properties.' prefixes to get clean column names
    spark_chunk_df = strip_properties_prefix_spark(spark_chunk_df)

    # Build the exact list of property column names we expect to add (post-strip)
    # Some HubSpot properties like 'hs_object_id', 'createdate', 'lastmodifieddate' come back under properties
    desired_prop_cols = [p for p in props_to_fetch if p in spark_chunk_df.columns]

    # Select only 'id' + desired new property columns (NO meta from the right side)
    spark_chunk_df = spark_chunk_df.select(["id"] + desired_prop_cols)

    # LEFT OUTER JOIN on 'id' to enrich columns without duplicating left columns
    spark_wide_df = (
        spark_wide_df.alias("left")
        .join(spark_chunk_df.alias("right"), on="id", how="left")
    )

    # Mark these properties as added so we don't add them again from future chunks
    added_props.update(desired_prop_cols)

    print(f"Joined chunk {chunk_idx} with {len(desired_prop_cols)} properties. "
          f"Total columns now: {len(spark_wide_df.columns)}")

# =========================
# 4) Safety: verify there are no duplicate column names
# =========================
cols = spark_wide_df.columns
dupes = [c for c in cols if cols.count(c) > 1]
if dupes:
    # As a last resort, drop duplicated right-side columns if somehow present
    print("Found duplicate columns, dropping duplicates:", set(dupes))
    # Keep only the first occurrence; Spark can't select duplicate names, so rebuild a unique selection
    unique_cols = []
    seen = set()
    for c in cols:
        if c not in seen:
            unique_cols.append(c)
            seen.add(c)
    spark_wide_df = spark_wide_df.select(*unique_cols)

# =========================
# 5) Write to Delta
# =========================
spark_wide_df.write.format("delta").mode("overwrite").save(table_path)
# print(f"Saved {spark_wide_df.count()} rows to {table_name} at {table_path}")
# Optional: register table
# spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {ACCESS_TOKEN}"} 
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE      = 100        
PROP_CHUNK     = 80          
TIMEOUT        = 60

table_name     = "br_hs_contacts3"
table_path     = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"

def hubspot_get(url, params=None):
    """GET with simple 429 handling."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp

def hubspot_post(url, json_body):
    """POST with simple 429 handling."""
    resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")
    return resp

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def strip_properties_prefix_spark(df):
    """Remove 'properties.' prefix from Spark DF column names."""
    for col_name in df.columns:
        if col_name.startswith("properties."):
            df = df.withColumnRenamed(col_name, col_name.replace("properties.", ""))
    return df

props_resp = hubspot_get(PROPS_URL)
props_json = props_resp.json()
ALL_PROPERTIES = [p["name"] for p in props_json.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))


params = {
    "limit": PAGE_SIZE,
    "properties": ",".join(ALL_PROPERTIES)   # all columns requested
}


query_len_estimate = len(params["properties"]) + 50
use_fallback = query_len_estimate > 6000  

spark_final_df = None

if not use_fallback:
    try:
        list_resp = hubspot_get(CONTACTS_URL, params=params)
        list_json = list_resp.json()
        results = list_json.get("results", [])
        if not results:
            raise RuntimeError("No contacts returned from list endpoint.")

        # Normalize to pandas → Spark
        pdf = pd.json_normalize(results)

        # Ensure meta columns exist
        for c in ["id", "createdAt", "updatedAt", "archived"]:
            if c not in pdf.columns:
                pdf[c] = None

        # Strip properties prefix
        pdf.columns = [c.replace("properties.", "") for c in pdf.columns]

        spark_final_df = spark.createDataFrame(pdf)
        print(f"[GET path] Final columns: {len(spark_final_df.columns)}; rows: {spark_final_df.count()}")

    except Exception as e:
        print(f"[GET path] Failed or unsuitable ({e}). Falling back to batch-read POST...")
        use_fallback = True

if use_fallback:
    # First lock the first page IDs using GET list (limit=100)
    list_resp = hubspot_get(CONTACTS_URL, params={"limit": PAGE_SIZE})
    list_json = list_resp.json()
    first_page = list_json.get("results", [])
    if not first_page:
        raise RuntimeError("No contacts returned in the first page.")

    CONTACT_IDS = [c["id"] for c in first_page]
    print("Row count (first page):", len(CONTACT_IDS))

    # Build meta DF in Spark (id + meta columns)
    meta_pdf = pd.json_normalize(first_page)
    for c in ["id", "createdAt", "updatedAt", "archived"]:
        if c not in meta_pdf.columns:
            meta_pdf[c] = None
    meta_pdf = meta_pdf[["id", "createdAt", "updatedAt", "archived"]]
    spark_wide_df = spark.createDataFrame(meta_pdf)

    added_props = set()
    chunk_idx = 0

    for prop_chunk in chunks(ALL_PROPERTIES, PROP_CHUNK):
        props_to_fetch = [p for p in prop_chunk if p not in added_props]
        if not props_to_fetch:
            continue

        chunk_idx += 1
        body = {
            "properties": props_to_fetch,
            "inputs": [{"id": cid} for cid in CONTACT_IDS], 
        }
        batch_resp = hubspot_post(BATCH_READ_URL, json_body=body)
        batch_json = batch_resp.json()
        results = batch_json.get("results", [])

        # Normalize chunk
        chunk_pdf = pd.json_normalize(results)
        if "id" not in chunk_pdf.columns:
            chunk_pdf["id"] = CONTACT_IDS 

        spark_chunk_df = spark.createDataFrame(chunk_pdf)
        spark_chunk_df = strip_properties_prefix_spark(spark_chunk_df)

        current_cols = set(spark_chunk_df.columns)
        for p in props_to_fetch:
            if p not in current_cols:
                spark_chunk_df = spark_chunk_df.withColumn(p, F.lit(None).cast(StringType()))

        spark_chunk_df = spark_chunk_df.select(["id"] + props_to_fetch)


        spark_wide_df = spark_wide_df.join(spark_chunk_df, on="id", how="left")

        added_props.update(props_to_fetch)
        print(f"Joined chunk {chunk_idx} with {len(props_to_fetch)} properties. "
              f"Total columns now: {len(spark_wide_df.columns)}")

    spark_final_df = spark_wide_df


spark_final_df.write.format("delta").mode("overwrite").save(table_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Final DF columns (count):", len(spark_final_df.columns))

df_back = spark.read.format("delta").load(table_path)
print("Read-back columns:", len(df_back.columns))

missing = [p for p in ALL_PROPERTIES if p not in df_back.columns]
print("Missing properties:", len(missing), missing[:20])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# required columns

# CELL ********************

"firstname,lastname,email,motorbike_owner,do_you_have_at_least_a_m2_licence,cbt_or_motorcycle_licence_prior_to_purchase,licence_status,hs_analytics_source,where_the_customer_first_heard_of_us,hs_analytics_first_visit_timestamp,klaviyo_first_active,first_conversion_event_name,first_conversion_date,engagements_last_meeting_booked,date_of_birth,country,country_or_region,zip,city,createdate,test_ride___date_of_booking_creation,test_ride___booked_date,data___test_ride___attended,test_ride___location_name,test_ride___model_selected,test_ride___booked_type,test_ride___cancelled_date,do_you_want_one_,customer___purchase_intent___timeframe, hs_additional_emails,date_of_birth__dd_mm_yyyy_,year_of_birth,lifecyclestage"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL     = "https://api.hubapi.com"
CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts"

headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"} 
PAGE_SIZE = 100


props_str = "firstname,lastname,email,motorbike_owner,do_you_have_at_least_a_m2_licence,cbt_or_motorcycle_licence_prior_to_purchase,licence_status,hs_analytics_source,where_the_customer_first_heard_of_us,hs_analytics_first_visit_timestamp,klaviyo_first_active,first_conversion_event_name,first_conversion_date,engagements_last_meeting_booked,date_of_birth,country,country_or_region,zip,city,createdate,test_ride___date_of_booking_creation,test_ride___booked_date,data___test_ride___attended,test_ride___location_name,test_ride___model_selected,test_ride___booked_type,test_ride___cancelled_date,do_you_want_one_,customer___purchase_intent___timeframe,hs_additional_emails,date_of_birth__dd_mm_yyyy_,year_of_birth,lifecyclestage"
LIMITED_PROPERTIES = [p.strip() for p in props_str.split(",") if p.strip()]

params = {
    "limit": PAGE_SIZE,
    "properties": ",".join(LIMITED_PROPERTIES)
}


resp = requests.get(CONTACTS_URL, headers=headers, params=params, timeout=60)
if resp.status_code != 200:
    raise RuntimeError(f"Contacts request failed [{resp.status_code}]: {resp.text}")

payload = resp.json()
results = payload.get("results", [])
if not results:
    raise RuntimeError("No contacts returned from HubSpot list endpoint.")

pdf = pd.json_normalize(results)

for c in ["id", "createdAt", "updatedAt", "archived"]:
    if c not in pdf.columns:
        pdf[c] = None

pdf.columns = [c.replace("properties.", "") for c in pdf.columns]

spark_df = spark.createDataFrame(pdf)


for p in LIMITED_PROPERTIES:
    if p not in spark_df.columns:
        spark_df = spark_df.withColumn(p, F.lit(None).cast(StringType()))

meta_cols = ["id", "createdAt", "updatedAt", "archived"]
ordered_cols = meta_cols + LIMITED_PROPERTIES
spark_df = spark_df.select(*ordered_cols)


table_name = "br_hs_contacts_limited"
table_path  = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# infer schema 

# CELL ********************

BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {ACCESS_TOKEN}"}  
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE      = 100         
PROP_CHUNK     = 80          
PARALLELISM    = 6           
TIMEOUT        = 60

table_name     = "br_hs_contacts_all_parallel"
table_path     = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"


def chunkify(lst, n):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def http_get(url, params=None):
    """GET with basic 429 handling."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def http_post(url, json_body):
    """POST with basic 429 handling."""
    resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")
    return resp.json()

def strip_properties_prefix(cols):
    """Return map for renaming columns 'properties.*' -> name."""
    renames = {}
    for c in cols:
        if c.startswith("properties."):
            renames[c] = c.replace("properties.", "")
    return renames

props_j = http_get(PROPS_URL)
ALL_PROPERTIES = [p["name"] for p in props_j.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))


list_j = http_get(CONTACTS_URL, params={"limit": PAGE_SIZE})
first_page = list_j.get("results", [])
if not first_page:
    raise RuntimeError("No contacts returned in the first page.")
CONTACT_IDS = [r.get("id") for r in first_page]
print("Row count (first page):", len(CONTACT_IDS))

meta_rows = []
for r in first_page:
    meta_rows.append(Row(
        id=r.get("id"),
        createdAt=r.get("createdAt"),
        updatedAt=r.get("updatedAt"),
        archived=r.get("archived")
    ))
spark_wide_df = spark.createDataFrame(meta_rows)


added_props = set()

prop_chunks = list(chunkify(ALL_PROPERTIES, PROP_CHUNK))

num_slices = PARALLELISM if len(prop_chunks) >= PARALLELISM else len(prop_chunks)

def fetch_chunk(props_to_fetch):
    """Executor-side: call batch/read for given properties, flatten rows."""
    body = {
        "properties": props_to_fetch,
        "inputs": [{"id": cid} for cid in CONTACT_IDS],
    }
    j = http_post(BATCH_READ_URL, json_body=body)
    results = j.get("results", [])


    out_rows = []
    for r in results:
        base = {
            "id": r.get("id"),
            "createdAt": r.get("createdAt"),
            "updatedAt": r.get("updatedAt"),
            "archived": r.get("archived"),
        }
        props = r.get("properties", {}) or {}
        base.update(props)  
        out_rows.append(base)

    return {"data": out_rows, "requested": list(props_to_fetch)}

chunk_payloads = sc.parallelize(prop_chunks, numSlices=num_slices) \
                   .map(fetch_chunk) \
                   .collect()

for idx, payload in enumerate(chunk_payloads, start=1):
    rows_dicts = payload["data"]
    requested_props = payload["requested"]

    spark_chunk_df = spark.createDataFrame(
        [Row(**rd) for rd in rows_dicts]
    )

    rename_map = strip_properties_prefix(spark_chunk_df.columns)
    for src, dst in rename_map.items():
        spark_chunk_df = spark_chunk_df.withColumnRenamed(src, dst)


    current_cols = set(spark_chunk_df.columns)
    for p in requested_props:
        if p not in current_cols:
            spark_chunk_df = spark_chunk_df.withColumn(p, F.lit(None).cast(StringType()))

    new_cols = [p for p in requested_props if p not in added_props]
    if not new_cols:
        print(f"Chunk {idx}: no new columns to add, skipping.")
        continue

    spark_chunk_df = spark_chunk_df.select(["id"] + new_cols)

    # LEFT JOIN on id to enrich the wide DF with this chunk's properties
    spark_wide_df = spark_wide_df.join(spark_chunk_df, on="id", how="left")

    # Mark properties as added
    added_props.update(new_cols)

    print(f"Joined chunk {idx} with {len(new_cols)} properties. "
          f"Total columns now: {len(spark_wide_df.columns)}")

spark_wide_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests
import time
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {ACCESS_TOKEN}"}  # <-- ensure ACCESS_TOKEN is defined
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE      = 100          # one page only
PROP_CHUNK     = 80           # properties per chunk (tune)
PARALLELISM    = 6            # number of concurrent chunks (tune vs. rate limits)
TIMEOUT        = 60

table_name     = "br_hs_contacts_all_parallel"
table_path     = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def http_post(url, json_body):
    """POST with basic 429 handling."""
    resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")
    return resp.json()

def http_get(url, params=None):
    """GET with basic 429 handling."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

props_j = http_get(PROPS_URL)
ALL_PROPERTIES = [p["name"] for p in props_j.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))


list_j = http_get(CONTACTS_URL, params={"limit": PAGE_SIZE})
first_page = list_j.get("results", [])
if not first_page:
    raise RuntimeError("No contacts returned in the first page.")
CONTACT_IDS = [r.get("id") for r in first_page]
print("Row count (first page):", len(CONTACT_IDS))

meta_schema = StructType([
    StructField("id",        StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("archived",  StringType(), True),
])
meta_rows = [
    (r.get("id"), r.get("createdAt"), r.get("updatedAt"), r.get("archived"))
    for r in first_page
]
spark_wide_df = spark.createDataFrame(meta_rows, schema=meta_schema)

added_props = set()

prop_chunks = list(chunkify(ALL_PROPERTIES, PROP_CHUNK))

import builtins
num_slices = builtins.min(len(prop_chunks), PARALLELISM)

def fetch_chunk(props_to_fetch):
    """Executor-side: call batch/read for given properties; return per-id dict."""
    body = {
        "properties": props_to_fetch,
        "inputs": [{"id": cid} for cid in CONTACT_IDS],
    }
    j = http_post(BATCH_READ_URL, json_body=body)
    results = j.get("results", [])

    per_id = {}
    for r in results:
        cid = r.get("id")
        props = (r.get("properties") or {})
        per_id[cid] = {k: (str(v) if v is not None else None) for k, v in props.items()}
    return {"requested": list(props_to_fetch), "per_id": per_id}

chunk_payloads = sc.parallelize(prop_chunks, numSlices=num_slices) \
                   .map(fetch_chunk) \
                   .collect()

for idx, payload in enumerate(chunk_payloads, start=1):
    requested_props = payload["requested"]
    per_id_map      = payload["per_id"]

    new_cols = [p for p in requested_props if p not in added_props]
    if not new_cols:
        print(f"Chunk {idx}: no new columns to add, skipping.")
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

    spark_wide_df = spark_wide_df.join(spark_chunk_df, on="id", how="left")

    added_props.update(new_cols)
    print(f"Joined chunk {idx} with {len(new_cols)} properties. "
          f"Total columns now: {len(spark_wide_df.columns)}")

spark_wide_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests
import time
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {ACCESS_TOKEN}"}  # <-- ensure ACCESS_TOKEN is defined
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"

PAGE_SIZE      = 100          # contacts per page
PAGES          = 4            # number of pages to fetch (3–4 as requested)
ID_CHUNK       = 100          # IDs per batch/read call (tune if needed)
PROP_CHUNK     = 80           # properties per chunk (tune)
PARALLELISM    = 6            # concurrent property-chunk tasks (tune vs. rate limits)
TIMEOUT        = 60

table_name     = "br_hs_contacts_all_parallel2"
table_path     = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"



def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def http_post(url, json_body):
    """POST with basic 429 handling."""
    resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.post(url, headers=HEADERS, json=json_body, timeout=TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST error [{resp.status_code}]: {resp.text}")
    return resp.json()

def http_get(url, params=None):
    """GET with basic 429 handling."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait_s = int(resp.headers.get("Retry-After", "2"))
        time.sleep(wait_s)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


props_j = http_get(PROPS_URL)
ALL_PROPERTIES = [p["name"] for p in props_j.get("results", [])]
print("Property count:", len(ALL_PROPERTIES))


CONTACT_IDS = []
meta_map = {}   # id -> (id, createdAt, updatedAt, archived)

after = None
for page_idx in range(PAGES):
    params = {"limit": PAGE_SIZE}
    if after:
        params["after"] = after

    list_j = http_get(CONTACTS_URL, params=params)
    results = list_j.get("results", [])
    if not results:
        break

    for r in results:
        cid = r.get("id")
        if cid not in meta_map:  # avoid dup if any
            meta_map[cid] = (
                cid,
                r.get("createdAt"),
                r.get("updatedAt"),
                r.get("archived"),
            )
            CONTACT_IDS.append(cid)

    nxt = (list_j.get("paging") or {}).get("next")
    after = nxt.get("after") if nxt else None
    if not after:
        break

print(f"Total contacts collected: {len(CONTACT_IDS)} (pages requested: {PAGES})")

# Base DF with meta (StringType schema)
meta_schema = StructType([
    StructField("id",        StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("archived",  StringType(), True),
])

meta_rows = list(meta_map.values())
spark_wide_df = spark.createDataFrame(meta_rows, schema=meta_schema)

added_props = set()


prop_chunks = list(chunkify(ALL_PROPERTIES, PROP_CHUNK))

# Use Python's built-in min (avoid collision with pyspark.sql.functions.min)
import builtins
num_slices = builtins.min(len(prop_chunks), PARALLELISM)

def fetch_chunk(props_to_fetch):
    """Executor-side: call batch/read for given properties over ID chunks; return per-id dict."""
    per_id = {}

    # Iterate ID chunks inside the task to respect HubSpot body limits
    for id_chunk in chunkify(CONTACT_IDS, ID_CHUNK):
        body = {
            "properties": props_to_fetch,
            "inputs": [{"id": cid} for cid in id_chunk],
        }
        j = http_post(BATCH_READ_URL, json_body=body)
        results = j.get("results", [])
        for r in results:
            cid = r.get("id")
            props = (r.get("properties") or {})
            # string-ify values for explicit StringType
            per_id[cid] = per_id.get(cid, {})
            for k, v in props.items():
                per_id[cid][k] = (str(v) if v is not None else None)

    return {"requested": list(props_to_fetch), "per_id": per_id}

chunk_payloads = sc.parallelize(prop_chunks, numSlices=num_slices) \
                   .map(fetch_chunk) \
                   .collect()


for idx, payload in enumerate(chunk_payloads, start=1):
    requested_props = payload["requested"]
    per_id_map      = payload["per_id"]

    # Only add columns that aren't already present
    new_cols = [p for p in requested_props if p not in added_props]
    if not new_cols:
        print(f"Chunk {idx}: no new columns to add, skipping.")
        continue

    # Build explicit schema: id + new property columns (StringType)
    schema = StructType([StructField("id", StringType(), True)] +
                        [StructField(p, StringType(), True) for p in new_cols])

    # Build rows aligned to schema: (id, col1, col2, ...)
    data_rows = []
    for cid in CONTACT_IDS:
        row_vals = [cid]
        props_for_id = per_id_map.get(cid, {})
        for p in new_cols:
            row_vals.append(props_for_id.get(p))  # None if missing
        data_rows.append(tuple(row_vals))

    spark_chunk_df = spark.createDataFrame(data_rows, schema=schema)

    # LEFT JOIN on id to enrich the wide DF
    spark_wide_df = spark_wide_df.join(spark_chunk_df, on="id", how="left")

    added_props.update(new_cols)
    print(f"Joined chunk {idx} with {len(new_cols)} properties. "
          f"Total columns now: {len(spark_wide_df.columns)}")


spark_wide_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(table_path)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests
import pandas as pd


BASE_URL      = "https://api.hubapi.com"
ACCESS_TOKEN  = ACCESS_TOKEN  # define your token
HEADERS       = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

CONTACTS_URL  = f"{BASE_URL}/crm/v3/objects/contacts"
PROP_URL      = f"{BASE_URL}/crm/v3/properties/contacts"

LIMIT_ROWS    = 1000


prop_resp = requests.get(PROP_URL, headers=HEADERS, timeout=60)
prop_resp.raise_for_status()
ALL_PROPERTIES = [p["name"] for p in prop_resp.json().get("results", [])]
print(f"Total properties available: {len(ALL_PROPERTIES)}")

# ===== Fetch one page with ALL properties =====
params = {
    "limit": LIMIT_ROWS,
    "properties": ",".join(ALL_PROPERTIES)  # all properties in one call
}

resp = requests.get(CONTACTS_URL, headers=HEADERS, params=params, timeout=60)
resp.raise_for_status()

data = resp.json().get("results", [])
df = pd.json_normalize(data)

# Strip 'properties.' prefix for clarity
df.rename(columns={c: c.replace("properties.", "") for c in df.columns if c.startswith("properties.")}, inplace=True)

print(f"Fetched {len(df)} rows and {len(df.columns)} columns")
# print(df.head(3))  # preview first 3 rows

# ===== Convert to Spark and write to Lakehouse =====
spark_df = spark.createDataFrame(df)
table_name = "br_hs_contacts"
table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
spark_df.write.format("delta").mode("overwrite").save(table_name)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 

# CELL ********************

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",  # you already refreshed and printed it
    "Content-Type": "application/json"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# --- Fetch ALL contact properties: /crm/v3/properties/0-1 ---
BASE_URL = "https://api.hubapi.com"
PROPS_URL = f"{BASE_URL}/crm/v3/properties/contacts"   # 0-1 = contacts objectTypeId

prop_resp = requests.get(PROP_URL, headers=HEADERS, timeout=60)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prop_resp.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


result = prop_resp.json() 
target = "customer___purchase_intent___timeframe"


results = result.get("results", [])

found = any(item.get("name") == target for item in results)

print(found)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = prop_resp.json()
count = len(data.get("results", []))
print(count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests

BASE_URL     = "https://api.hubapi.com"
CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts"
headers      = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

params = {"limit": 100}

resp = requests.get(CONTACTS_URL, headers=headers, params=params, timeout=60)

print("Status code:", resp.status_code)


print("All headers:", dict(resp.headers))         
print("Content-Type:", resp.headers.get("Content-Type"))
print("Date:", resp.headers.get("Date"))


if resp.status_code == 429:
    print("Retry-After:", resp.headers.get("Retry-After"))

# resp.raise_for_status()
# payload = resp.json()
# results = payload.get("results", [])
# paging  = (payload.get("paging") or {}).get("next")
# after   = paging.get("after") if paging else None

print(f"Fetched {len(results)} contacts on this page.")
# print("Next 'after' token (from JSON body):", after)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retry_after = headers.get("Retry-After")
print(retry_after)

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
