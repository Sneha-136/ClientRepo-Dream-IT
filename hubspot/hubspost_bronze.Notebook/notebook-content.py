# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

CLIENT_ID = "ecc9ac61-ccbc-4974-be41-b587129ff708"
CLIENT_SECRET = "ee6e5bc8-35f5-45cb-8474-567874e9d7d6"
REFRESH_TOKEN = "eu1-6105-5e5c-4bb3-bdf1-d802309feda0"
BASE_URL = "https://api.hubapi.com"

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

# CELL ********************

BASE_URL       = "https://api.hubapi.com"
PROPS_URL      = f"{BASE_URL}/crm/v3/properties/contacts"
CONTACTS_URL   = f"{BASE_URL}/crm/v3/objects/contacts"
BATCH_READ_URL = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"


TIMEOUT   = 60

# PAGE_SIZE   = 100     # contacts per page
# # PAGES       =''      # number of pages to fetch (set 3-4 as you prefer)
# ID_CHUNK    = 100     # IDs per batch/read POST call
# PROP_CHUNK  = 80      # properties per chunk (tune between 60-100)
# PARALLELISM = 6       # number of concurrent property-chunk tasks

# table_name = "br_hs_contacts_all_parallelly"
# table_path = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# MARKDOWN ********************

# chunking

# CELL ********************

def get_all_contact_properties():
    url = f"{BASE_URL}/crm/v3/properties/contacts"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    return [p["name"] for p in data.get("results", [])]

def try_fetch(props):
    url = f"{BASE_URL}/crm/v3/objects/contacts"
    params = {"properties": ",".join(props), "limit": 1}
    r = requests.get(url, params=params, headers=HEADERS)
    if r.status_code >= 400:
        raise Exception(str(r.status_code))
    return r.json()

def chunk_properties(props):
    chunks = []
    current = []
    for prop in props:
        candidate = current + [prop]
        try:
            try_fetch(candidate)
            current = candidate
        except:
            print("Started chunk with", len(current), "properties")
            chunks.append(current)
            try_fetch([prop])
            current = [prop]
    if current:
        print("Started chunk with", len(current), "properties")
        chunks.append(current)
    print("Total properties:", len(props))
    return chunks

props = get_all_contact_properties()
chunks = chunk_properties(props)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# pagination

# CELL ********************


def fetch_all_pages_contacts(contacts_url, headers, limit=100, properties=None, timeout=60):
    all_records = []
    after = None
    page_no = 1

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
        print(f"Page {page_no}: fetched {len(batch)} records")

        if not batch:
            break

        all_records.extend(batch)

        next_obj = (result.get("paging") or {}).get("next")
        after = next_obj.get("after") if next_obj else None

        if not after:
            break

        page_no += 1

    print("Total records fetched:", len(all_records))
    return all_records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# merge_results

# CELL ********************


def merge_chunk_results(property_chunks, contacts_url, headers):
    merged = {}

    for idx, props in enumerate(property_chunks, start=1):
        print(f"\nFetching for chunk {idx} with {len(props)} properties")
        records = fetch_all_pages_contacts(
            contacts_url=contacts_url,
            headers=headers,
            properties=props
        )

        for r in records:
            cid = r.get("id")
            if not cid:
                continue
            if cid not in merged:
                merged[cid] = {"id": cid}

            for p, v in (r.get("properties") or {}).items():
                merged[cid][p] = v

    print("\nTotal merged unique records:", len(merged))
    return list(merged.values())

    spark_wide_df = spark_wide_df.select(*ordered_cols)

    table_path  = f"{BRONZE_LAKEHOUSE_PATH}/Tables/{table_name}"
    print(f"[all] Writing Delta to: {table_path}")
    (spark_wide_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


final_data = merge_chunk_results(chunks, CONTACTS_URL, HEADERS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

    # meta_schema = StructType([
    #     StructField("id",        StringType(), True),
    #     StructField("createdAt", StringType(), True),
    #     StructField("updatedAt", StringType(), True),
    # #     StructField("archived",  StringType(), True),
    #       StructField("archivedAt",  StringType(), True)
    # ])
    # spark_wide_df = spark.createDataFrame(meta_rows, schema=meta_schema).persist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
