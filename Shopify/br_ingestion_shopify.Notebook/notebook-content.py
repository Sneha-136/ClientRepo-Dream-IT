# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
import time
import os
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Row
from cryptography.fernet import Fernet
import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_LAKEHOUSE_NAME = 'Bronze_Lakehouse'
try:
    bronze_lakehouse_details = notebookutils.lakehouse.get(BRONZE_LAKEHOUSE_NAME)
except:
    Exception(f"A LAKEHOUSE WITH NAME '{BRONZE_LAKEHOUSE_NAME}' DOES NOT EXIST IN THE WORKSPACE")

LAKEHOUSE_PATH = bronze_lakehouse_details.get('properties').get('abfsPath')
CONFIG_PATH = f"{LAKEHOUSE_PATH}/Tables/br_shopify_config"

PRIVATE_KEY = spark.read.text(f"{LAKEHOUSE_PATH}/Files/fernet_key.txt").collect()[0][0].encode("utf-8")
FERNET = Fernet(PRIVATE_KEY)

WORKSPACE_ID = bronze_lakehouse_details.get('workspaceId')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_config_date(store, table, new_last_sync):
    delta_table = DeltaTable.forName(spark, f"delta.`{CONFIG_PATH}`")
    delta_table.update(
        condition=f"store = '{store}' AND table = '{table}'",
        set={"last_sync": F.lit(new_last_sync)} 
    )
    print(f"Config table updated for {store} {table} with date {new_last_sync}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_date_chunks(object, min_date, max_date, shopify_store, access_token):

    end_date = max_date
    start_date = min_date
    modified_start_date = min_date

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }

    yearly_chunks = []
    current_year = start_date.year
    year_group = []
    
    while start_date < end_date:
        chunk_start = modified_start_date
        chunk_end = start_date + relativedelta(months=1) - timedelta(seconds=1)
        
        chunk = (
            shopify_store,
            headers,
            object,
            chunk_start.strftime("%Y-%m-%dT%H:%M:%S"),
            chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        )
        
        if start_date.year != current_year:
            yearly_chunks.append(year_group)
            year_group = []
            current_year = start_date.year
        
        year_group.append(chunk)
        modified_start_date = start_date + relativedelta(months=1) - timedelta(days=2)
        start_date = start_date + relativedelta(months=1)
    
    if year_group:
        yearly_chunks.append(year_group)
    
    return yearly_chunks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_shopify_records(chunk):
    store, headers, object, min_timestamp, max_timestamp = chunk
    base_url = f"https://{store}/admin/api/2025-10/{object}.json"
    all_records = []
    since_id = None
    attempt = 0

    print(f"Starting fetch for store: {store}, object: {object}, range: {min_timestamp} to {max_timestamp},base_url: {base_url}")

    params = {
        "limit": 250,
        "order": "id asc",
        "updated_at_min": min_timestamp,
        "updated_at_max": max_timestamp,
    }
    if since_id:
        params["since_id"] = since_id

    print(f"[INFO] Initial params: {params}")

    # Determine status (fallback to 'any' if available)
    try:
        temp = params.copy()
        temp['status'] = 'any'
        response = requests.get(base_url, headers=headers, params=temp, timeout=30)
        response.raise_for_status()
        data = response.json().get(object, [])
        if data:
            print("[INFO] 'status=any' appears supported; enabling it.")
            params['status'] = 'any'
        else:
            print("[INFO] Probe returned empty data; proceeding without explicit status.")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed during status probe for store {store}: {e}")
        return []

    while True:
        attempt += 1

        if since_id:
            params['since_id'] = since_id
        elif 'since_id' in params:
            del params['since_id']

        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Request failed on attempt {attempt} for store {store}: {e}")
            break

        try:
            payload = response.json()
            batch = payload.get(object, [])
        except ValueError as e:
            print(f"[ERROR] Failed to parse JSON response for store {store}: {e}")
            break

        if not batch:
            print(f"[INFO] No more records found for store {store} in this chunk.")
            break

        if isinstance(batch, list):
            all_records.extend(batch)
            # Update since_id from last record if present
            if batch and isinstance(batch[-1], dict) and "id" in batch[-1]:
                since_id = batch[-1]["id"]
            else:
                since_id = None
        else:
            # Rare case: API returns an object instead of list
            all_records.append(batch)
            if isinstance(batch, dict) and "since_id" in batch:
                since_id = batch.get('since_id')
            else:
                since_id = None

        time.sleep(0.25)

    return all_records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config = spark.read.format("delta").load(CONFIG_PATH)

def decrypt_token(token):
    if token:
        return FERNET.decrypt(token.encode()).decode()
    return None
decrypt_udf = F.udf(decrypt_token, StringType())

# df_config = df_config.filter(
#     (F.col('source').isin(['products']))
# )
df_config = df_config.filter(F.col("isActive") == F.lit(True))
df_config = df_config.withColumn("access_token", decrypt_udf(df_config["access_token"]))

display(df_config.orderBy("store"))

for row in df_config.collect():
    store = row['store']
    access_token = row['access_token']
    destination_table = row['table']
    source_object = row['source']
    prefix = row['prefix']
    last_sync = row['last_sync']
    # last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

    print(f"\nStarting ingestion for {store} {source_object}")

    table_path = f"{LAKEHOUSE_PATH}/Tables/{destination_table}"
    print(f"Target Delta table path: {table_path}")

    new_last_sync = datetime.now()
    chunks = get_date_chunks(source_object, last_sync, new_last_sync, store, access_token)
    print(f"Generated date chunks for source: {source_object}")
    for year_chunk in reversed(chunks):
        rdd = sc.parallelize(year_chunk)
        print(f"Fetching records in parallel for {source_object} from {year_chunk[0][-2]} to {year_chunk[-1][-1]}")

        record_chunks = rdd.map(fetch_shopify_records).collect()
        print(f"Completed fetch")

        # # DEBUG
        # record_chunks = []
        # for month_chunk in year_chunk:
        #     month_data = fetch_shopify_records(month_chunk)
        #     record_chunks.append(month_data)
        # # DEBUG

        record_chunks_data_existence_map = []
        for i in range(len(record_chunks)):
            if record_chunks[i]:
                record_chunks_data_existence_map.append(1)
            else: record_chunks_data_existence_map.append(0)

        record_existence_sum = 0
        for val in record_chunks_data_existence_map: record_existence_sum += val

        if record_existence_sum:
            record_chunks = [item for item, flag in zip(record_chunks, record_chunks_data_existence_map) if flag]
            sample_record = record_chunks[0][0]
            string_schema = StructType([
                StructField(key, StringType(), True) for key in sample_record.keys()
            ])
            print(f"Inferred schema with {len(sample_record.keys())} fields from sample record.")

            for i, records in enumerate(record_chunks):
                print(f"writing chunk {i+1}/{len(record_chunks)} with {len(records)} records...")
                df = spark.createDataFrame(records, schema=string_schema)
                df = df.withColumn("record_timestamp", F.current_timestamp())

                if DeltaTable.isDeltaTable(spark, table_path):
                    delta_table = DeltaTable.forPath(spark, table_path)
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        f"target.id = source.id"
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                else:
                    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

            print(f"Finished writing all chunks to {table_path}")
        else: print("No data was fetched")

    update_config_date(store, destination_table, new_last_sync)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
