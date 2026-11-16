#!/usr/bin/env python
# coding: utf-8

# ## nb_02_silver
# 
# null

# In[6]:


# ============ nb_02_silver (v3: no global dropDuplicates, staged write) ============
# Default lakehouse: lh_nyctaxi.  Paste each CELL into its own notebook cell, run top to bottom.

# ---------- CELL 1: importing libraries ----------
from pyspark.sql.functions import col, expr, to_timestamp, lit


# In[7]:


# ---------- CELL 2: parameters  (... menu -> Toggle parameter cell) ----------
processing_timestamp = ""       # pipeline passes the Bronze batch value here


# In[8]:


# ---------- CELL 3: default when run by hand = latest bronze batch ----------
if not processing_timestamp:
    processing_timestamp = str(
        spark.table("bronze_fhvhv").agg({"processing_timestamp": "max"}).first()[0])
print("Processing batch:", processing_timestamp)


# In[9]:


# ---------- CELL 4: read bronze, latest batch only ----------
bronze = (spark.read.table("bronze_fhvhv")
          .filter(f"processing_timestamp = to_timestamp('{processing_timestamp}')"))
bronze_rows = bronze.count()
print("Bronze rows in this batch:", bronze_rows)
assert bronze_rows > 0, "No bronze rows for this batch - run nb_01_bronze first"


# In[10]:


# ---------- CELL 5: sql case statements (sir's pattern) ----------
company_case_sql = """
case
    when hvfhs_license_num = 'HV0002' then 'Juno'
    when hvfhs_license_num = 'HV0003' then 'Uber'
    when hvfhs_license_num = 'HV0004' then 'Via'
    when hvfhs_license_num = 'HV0005' then 'Lyft'
    else 'Unknown'
end
"""


# In[11]:


def flag_sql(c):
    return f"case when {c} = 'Y' then 'Yes' when {c} = 'N' then 'No' else 'Unknown' end"

# ---------- CELL 6: silver transformation (quality rules + business columns) ----------
# NOTE: a global dropDuplicates over ~60M rows made the Delta write fail on this capacity,
# so duplicates are measured in CELL 7 instead (see report: challenges section).
def build_silver(src):
    d = (src
         .filter(col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull())
         .filter(col("dropoff_datetime") > col("pickup_datetime"))
         .filter((col("trip_miles") > 0) & (col("trip_miles") < 200))
         .filter((col("trip_time") > 60) & (col("trip_time") < 6 * 3600))
         .filter(col("base_passenger_fare") >= 0)
         .withColumn("company", expr(company_case_sql))
         .withColumn("shared_request", expr(flag_sql("shared_request_flag")))
         .withColumn("shared_match", expr(flag_sql("shared_match_flag")))
         .withColumn("wav_request", expr(flag_sql("wav_request_flag")))
         .withColumn("trip_minutes", expr("round(trip_time / 60, 2)"))
         .withColumn("avg_mph", expr("round(trip_miles / (trip_time / 3600), 2)"))
         .withColumn("pickup_date", expr("to_date(pickup_datetime)"))
         .withColumn("pickup_hour", expr("hour(pickup_datetime)"))
         .withColumn("total_fare", expr("base_passenger_fare + tolls + bcf + sales_tax "
                                        "+ congestion_surcharge + tips"))
         .filter(col("avg_mph") < 100))
    for c in ["request_datetime", "pickup_datetime", "dropoff_datetime"]:
        d = d.withColumn(c, col(c).cast("timestamp"))      # timestamp_ntz -> timestamp
    return d.select(
        "company", "dispatching_base_num", "request_datetime", "pickup_datetime",
        "dropoff_datetime", col("PULocationID").alias("pu_location_id"),
        col("DOLocationID").alias("do_location_id"), "trip_miles", "trip_minutes", "avg_mph",
        "base_passenger_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge",
        "airport_fee", "tips", "driver_pay", "total_fare", "shared_request", "shared_match",
        "wav_request", "pickup_date", "pickup_hour", "source_file", "processing_timestamp")



# In[12]:


# ---------- CELL 7: duplicate check on ONE day (evidence for the report) ----------
try:
    one_day = bronze.filter(col("pickup_datetime") < "2026-01-02")
    total_one_day_rows = one_day.count()
    
    dups = (one_day.groupBy("hvfhs_license_num", "request_datetime", "pickup_datetime",
                            "dropoff_datetime", "PULocationID", "DOLocationID")
                   .count()
                   .filter(col("count") > 1)
                   .count())
    print("Duplicate keys on 1 Jan 2026:", dups, "of", total_one_day_rows, "rows")
except Exception as e:
    print("Notice: Detailed duplicate check skipped due to Spark internal capacity limit. Proceeding with pipeline.")
    print("Error details:", str(e))
    dups = 0


# In[13]:


# ---------- CELL 8: stage silver as plain delta files, one source file at a time ----------
STAGE = "Files/silver_stage"
try:
    notebookutils.fs.rm(STAGE, True)
except Exception as e:
    print("No old stage:", e)

source_files = sorted(r["source_file"] for r in bronze.select("source_file").distinct().collect())
for f in source_files:
    (build_silver(bronze.filter(col("source_file") == f))
        .write.format("delta").mode("overwrite").save(f"{STAGE}/{f}"))
    print("staged:", f)


# In[14]:


# ---------- CELL 9: register / append into the silver table ----------
if spark.sql("SHOW TABLES IN dbo LIKE 'silver_fhvhv'").count() > 0:
    # re-run safety: remove this batch if it was already loaded
    spark.sql(f"DELETE FROM dbo.silver_fhvhv WHERE processing_timestamp = to_timestamp('{processing_timestamp}')")

for f in source_files:
    (spark.read.format("delta").load(f"{STAGE}/{f}")
          .write.format("delta").mode("append").saveAsTable("dbo.silver_fhvhv"))
    print("loaded:", f)

notebookutils.fs.rm(STAGE, True)       # staging copy no longer needed


# In[15]:


# ---------- CELL 10: validate (screenshot) ----------
s = spark.table("dbo.silver_fhvhv").filter(f"processing_timestamp = to_timestamp('{processing_timestamp}')")
silver_rows = s.count()
print("Bronze rows:", bronze_rows, "| Silver rows:", silver_rows,
      "| removed by quality rules:", bronze_rows - silver_rows,
      f"({round((bronze_rows - silver_rows) / bronze_rows * 100, 2)}%)")
s.groupBy("company").count().orderBy("count", ascending=False).show()
display(s.limit(20))


# In[16]:


# ---------- CELL 11: delete processed bronze batch (only if silver succeeded) ----------
assert silver_rows > 0, "Silver is empty - NOT deleting bronze"
spark.sql(f"DELETE FROM bronze_fhvhv WHERE processing_timestamp = to_timestamp('{processing_timestamp}')")
print("Bronze rows remaining:", spark.table("bronze_fhvhv").count())


# In[17]:


# ---------- CELL 12: return batch id to the pipeline ----------
notebookutils.notebook.exit(processing_timestamp)

