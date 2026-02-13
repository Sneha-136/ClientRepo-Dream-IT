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
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from delta.tables import DeltaTable
from pyspark.sql import *
from pyspark.sql.functions import *
from notebookutils import mssparkutils, lakehouse
from pyspark.sql.types import *
import re

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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    config = json.loads(input_config)
    print(" Configuration loaded successfully")
    print(json.dumps(config, indent=2))
except json.JSONDecodeError as e:
    print(f" Error parsing input_config: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# {
#   "sources": {
#     "contacts_all": {
#       "active_flag": true,
#       "fields_all": true,
#       "destination": "br_hs_contacts_all"
#     },
#     "contacts_limited": {
#       "active_flag": true,
#       "fields_all": false,
#       "fields": "firstname,lastname,email,motorbike_owner,do_you_have_at_least_a_m2_licence,cbt_or_motorcycle_licence_prior_to_purchase,licence_status,hs_analytics_source,where_the_customer_first_heard_of_us,hs_analytics_first_visit_timestamp,klaviyo_first_active,first_conversion_event_name,first_conversion_date,engagements_last_meeting_booked,date_of_birth,country,country_or_region,zip,city,createdate,test_ride___date_of_booking_creation,test_ride___booked_date,data___test_ride___attended,test_ride___location_name,test_ride___model_selected,test_ride___booked_type,test_ride___cancelled_date,do_you_want_one_,customer___purchase_intent___timeframe, hs_additional_emails,date_of_birth__dd_mm_yyyy_,year_of_birth,lifecyclestage",
#       "destination": "br_hs_contacts_limited"
#     }
#   }
# }

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
            # The two possible error messages Fabric returns
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


# CONFIG_TABLE_PATH = f"{BRONZE_LAKEHOUSE_PATH}/br_hs_config"

# DEFAULT_LAST_SYNC_STR = "1900-01-01 00:00:00.00000"
# DEFAULT_LAST_SYNC = datetime.strptime(DEFAULT_LAST_SYNC_STR, "%Y-%m-%d %H:%M:%S.%f")

# schema = StructType([
#     StructField("source", StringType(), True),        
#     StructField("destination", StringType(), True),     
#     StructField("last_sync", TimestampType(), True),    
#     StructField("isActive", BooleanType(), True),      
#     StructField("URL", StringType(), True),           
#     StructField("fields", StringType(), True)        
# ])

# BASE_URL = "https://api.hubspot.com"

# def normalize_source_key(s: str) -> str:
#     return str(s).strip().lower()

# def object_type_from_logical_source(logical_source: str) -> str:
#     key = normalize_source_key(logical_source)
#     return key.split("_", 1)[0] if "_" in key else key

# def get_objects_url(object_type: str) -> str:
#     obj = object_type.strip().lower()
#     return f"{BASE_URL}/crm/v3/objects/{obj}"

# def to_timestamp_or_default(s: str, default_dt: datetime) -> datetime:
#     if not s:
#         return default_dt
#     try:
#         return datetime.fromisoformat(s.replace("Z", "+00:00"))
#     except Exception:
#         return default_dt

# def normalize_fields(fields_all: bool, fields_raw) -> str:
#     if fields_all:
#         return "*"
#     if isinstance(fields_raw, list):
#         return ",".join([str(f).strip() for f in fields_raw if str(f).strip()])
#     if fields_raw is None:
#         return ""
#     return ",".join([s.strip() for s in str(fields_raw).split(",") if s.strip()])

# def build_rows_from_sources(sources: dict):
#     rows = []
#     for logical_source, cfg in sources.items():
#         object_type = object_type_from_logical_source(logical_source)
#         objects_url = get_objects_url(object_type)

#         is_active   = bool(cfg.get("active_flag", True))
#         destination = cfg.get("destination", f"br_hs_{normalize_source_key(logical_source)}")
#         last_sync   = to_timestamp_or_default(cfg.get("last_sync", DEFAULT_LAST_SYNC_STR), DEFAULT_LAST_SYNC)
#         fields      = normalize_fields(bool(cfg.get("fields_all", False)), cfg.get("fields", ""))

#         rows.append((
#             normalize_source_key(logical_source),
#             destination,
#             last_sync,
#             is_active,
#             objects_url,
#             fields
#         ))
#     return rows

# def upsert_config_table(base_param_json: str):
#     payload = json.loads(base_param_json)
#     sources = payload.get("sources", {})
#     rows = build_rows_from_sources(sources)

#     new_df = spark.createDataFrame(rows, schema)

#     try:
#         _ = spark.read.format("delta").load(CONFIG_TABLE_PATH)
#     except Exception:
#         print("Config table doesn't exist yet. Creating it…")
#         (new_df.write
#             .format("delta")
#             .mode("overwrite")
#             .save(CONFIG_TABLE_PATH))

#     delta_table = DeltaTable.forPath(spark, CONFIG_TABLE_PATH)
#     (delta_table.alias("target")
#         .merge(
#             new_df.alias("source"),
#             "target.source = source.source AND target.destination = source.destination"
#         )
#         .whenMatchedUpdate(set={
#             "last_sync": "source.last_sync",
#             "isActive":  "source.isActive",
#             "URL":       "source.URL",
#             "fields":    "source.fields"
#         })
#         .whenNotMatchedInsertAll()
#         .execute()
#     )
#     print(f"HubSpot config upsert complete -> {CONFIG_TABLE_PATH}")

# upsert_config_table(base_param_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

schema = StructType([
    StructField("table", StringType(), True),         
    StructField("source", StringType(), True),        
    StructField("last_sync", TimestampType(), True),  
    StructField("isActive", BooleanType(), True),    
    StructField("URL", StringType(), True),           
    StructField("fields", StringType(), True)          
])


last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
BASE_URL = "https://api.hubspot.com"
CONFIG_TABLE_PATH = f"{BRONZE_LAKEHOUSE_PATH}/br_hs_config"


data = []
payload = json.loads(input_config)
sources = payload.get("sources", {})

for source_name, source_config in sources.items():
    object_type = source_name.split("_", 1)[0].lower()
    url = f"{BASE_URL}/crm/v3/objects/{object_type}"
    table_name = source_config.get("destination", f"br_hs_{source_name.lower()}")
    fields = "*" if source_config.get("fields_all", False) else source_config.get("fields", "")
    active_flag = source_config.get("active_flag", True)

    data.append((
        table_name,
        source_name,
        last_sync,
        active_flag,
        url,
        fields
    ))

print("Rows to upsert:", data)

if not data:
    raise RuntimeError("No sources found in input_config; nothing to upsert.")


new_df = spark.createDataFrame(data, schema)

try:
    target_df = spark.read.format("delta").load(CONFIG_TABLE_PATH)
except:
    print("Config table doesn't exist yet. Creating it")
    (new_df.write
        .format("delta")
        .mode("overwrite")
        .save(CONFIG_TABLE_PATH))
    target_df = spark.read.format("delta").load(CONFIG_TABLE_PATH)

delta_table = DeltaTable.forPath(spark, CONFIG_TABLE_PATH)

(delta_table.alias("target")
    .merge(
        new_df.alias("source"),
        "target.table = source.table AND target.source = source.source"
    )
    .whenMatchedUpdate(set={
        "isActive": "source.isActive",
        "fields": "source.fields",
        "URL": "source.URL"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"HubSpot config upsert complete -> {CONFIG_TABLE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
