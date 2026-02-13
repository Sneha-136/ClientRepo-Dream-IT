# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c5952be-dd39-4325-a1bc-95b687d505a9",
# META       "default_lakehouse_name": "Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "473d7198-1f98-4a1e-bbcd-715b24c4259a",
# META       "known_lakehouses": [
# META         {
# META           "id": "9c5952be-dd39-4325-a1bc-95b687d505a9"
# META         },
# META         {
# META           "id": "3b0831ee-2110-45ee-b5e0-98d0c02f5aee"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import notebookutils
from collections import deque
import re
from datetime import datetime
import os
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
tables = spark.sql("SHOW TABLES").filter("tableName LIKE '%shopify%'")
# tables.select("tableName").show(truncate=False)
# for row in tables.collect():
#     spark.sql(f"DROP TABLE IF EXISTS {row.tableName}")

for row in tables.collect():
    table_name = row.tableName
    full_table_path = f"abfss://473d7198-1f98-4a1e-bbcd-715b24c4259a@onelake.dfs.fabric.microsoft.com/1141d732-1626-4983-bacc-93bd4aef2950/Tables/{table_name}"
    notebookutils.fs.rm(full_table_path, recurse=True)
    print(f"Deleted {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM Silver_Lakehouse.Sil_config
# MAGIC 
# MAGIC WHERE table LIKE '%shopify%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM Staging_Lakehouse.Staging_config
# MAGIC WHERE table LIKE '%shopify%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Staging_Lakehouse.Staging_config LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver_Lakehouse.Sil_config LIMIT 1000")
display(df)

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
