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

import json
import time
import os
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

# PARAMETERS CELL ********************

input_config = '{}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Remove non-breaking spaces (\xa0) and replace with regular spaces
input_config_fixed = input_config.replace('\xa0', ' ').strip()

try:
    config = json.loads(input_config_fixed)
    print("Configuration loaded successfully")
    print(json.dumps(config, indent=2))         
except json.JSONDecodeError as e:
    print(f"✗ Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_lakehouses(names):

    for name in names:
        try:
            notebookutils.lakehouse.create(name=name)
            print(f"Created: {name}")
        except Exception as e:
            error_str = str(e).lower()
            if ("itemdisplaynamealreadyinuse" in error_str or 
                "already in use" in error_str or 
                "already exists" in "errorcode" in error_str):
                print(f"Already exists: {name}")
            else:
                print(f"Unexpected error for {name}: {e}")
                raise

lakehouse_names = ["Bronze_Lakehouse", "Staging_Lakehouse", "Silver_Lakehouse", "Gold_Lakehouse"]
create_lakehouses(lakehouse_names)

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

key_path = f"{BRONZE_LAKEHOUSE_PATH}/Files/fernet_key.txt"

try:
    key_df = spark.read.text(key_path)
    print("Loading existing key...")
    private_key = key_df.first()[0].encode("utf-8")
    print(" Existing key loaded")
    
except:
    print("Generating new key...")
    private_key = Fernet.generate_key()
    
    # Save key
    key_df = spark.createDataFrame([private_key.decode("utf-8")], "string")
    key_df.write.mode("overwrite").text(key_path)
    print(" New key generated and saved")

# Create Fernet instance
fernet = Fernet(private_key)
print(f"Fernet key ready: {private_key.decode('utf-8')[:20]}...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


schema = StructType([
    StructField("store", StringType(), True),
    StructField("access_token", StringType(), True),
    StructField("table", StringType(), True),
    StructField("source", StringType(), True),
    StructField("prefix", StringType(), True),
    StructField("last_sync", TimestampType(), True),
    StructField("isActive", BooleanType(), True)
])

last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

data = []
for store_name, store_config in config["stores"].items():
    access_token = store_config["access_token"]
    prefix = store_config["prefix"]
    sources = store_config["sources"]
    for source_name, source_config in sources.items():
        active_flag = source_config["active_flag"]
        table_name = f"br_shopify_{source_name}"
        full_table_name = table_name + prefix
        encrypted_token = fernet.encrypt(access_token.encode()).decode()
        
        data.append((
            store_name,
            encrypted_token,
            full_table_name,
            source_name,
            prefix,
            last_sync,
            active_flag
        ))

new_df = spark.createDataFrame(data, schema)

table_path = f"{BRONZE_LAKEHOUSE_PATH}/Tables/br_shopify_config"

try:
    target_df = spark.read.format("delta").load(table_path)
except:
    print("Table doesn't exist yet → creating...")
    (new_df.write
     .format("delta")
     .mode("overwrite")
     .save(table_path))
    target_df = spark.read.format("delta").load(table_path)

delta_table = DeltaTable.forPath(spark, table_path)

(delta_table.alias("target")
    .merge(
        new_df.alias("source"),
        "target.store = source.store AND target.table = source.table AND target.source = source.source"
    )
    .whenMatchedUpdate(set = {
        "isActive": "source.isActive"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_CONFIG_PATH = f"{BRONZE_LAKEHOUSE_PATH}/Tables/br_shopify_config"
bronze_config = spark.read.format("delta").load(BRONZE_CONFIG_PATH)

default_sync_date = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

new_processing_rows = bronze_config.select(
    col("table").alias("source"),
    col("isActive").alias("isActive"),
    col("source").alias("bronze_source")
).distinct()
new_processing_rows = new_processing_rows.withColumn("table",concat(lit("sil.shopify."), col("bronze_source"))).withColumn(
    "last_sync",
    lit(default_sync_date)
).withColumn("key", lit("").cast(StringType()))
# Select and filter
new_processing_rows = new_processing_rows.select("table", "last_sync", "source","isActive","key")
new_processing_rows = new_processing_rows.filter(col("table").isNotNull())
display(new_processing_rows)

PROCESSING_CONFIG_PATH = f"{STAGING_LAKEHOUSE_PATH}/Tables/Staging_config"

try:
    existing_df = spark.read.format("delta").load(PROCESSING_CONFIG_PATH)
    print("Existing Staging_config found will preserve last_sync and upsert isActive")
except:
    existing_df = None
    print("No existing Staging_config creating from scratch")

if existing_df is not None:
    delta_table = DeltaTable.forPath(spark, PROCESSING_CONFIG_PATH)

    (delta_table.alias("target")
     .merge(
         new_processing_rows.alias("source"),
         "target.table = source.table AND target.source = source.source" 
     )
     .whenMatchedUpdate(set={
         "isActive": "source.isActive"
     })
     .whenNotMatchedInsertAll() 
     .execute())

    print("MERGE completed: isActive updated, new sources added, last_sync preserved")

else:
    (new_processing_rows.write
     .format("delta")
     .mode("overwrite").option("overwriteSchema", "true")
     .save(PROCESSING_CONFIG_PATH))
    print("Staging_config created for the first time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema1 = StructType([
    StructField("table", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("last_sync", TimestampType(), True)
])
data1 = []
df = spark.createDataFrame(data1, schema1)

sil_lakehouse_path = f"{SILVER_LAKEHOUSE_PATH}/Tables/Sil_config"
try:
    spark.read.format("delta").load(sil_lakehouse_path)
    pass
except:
    df.write.format("delta").mode("overwrite").save(sil_lakehouse_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
