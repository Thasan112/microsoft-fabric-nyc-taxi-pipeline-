#!/usr/bin/env python
# coding: utf-8

# ## nb_03_gold
# 
# null

# In[1]:


# ============ nb_03_gold (sir's structure, adapted to HVFHV / dbo.silver_fhvhv) ============
# Default lakehouse: lh_nyctaxi.  Paste each CELL into its own notebook cell.
 
# ---------- CELL 1: importing libraries ----------
from pyspark.sql.functions import to_timestamp, col, current_timestamp, expr, date_format
from pyspark.sql import functions as F
 


# In[2]:


# ---------- CELL 2: parameters  (... menu -> Toggle parameter cell) ----------
processing_timestamp = ""       # pipeline passes @activity('nb_02_silver').output.result.exitValue


# In[3]:


# ---------- CELL 3: read silver (latest processed batch) ----------
if not processing_timestamp:    # manual run = latest silver batch
    processing_timestamp = str(
        spark.table("dbo.silver_fhvhv").agg({"processing_timestamp": "max"}).first()[0])
print("Processing batch:", processing_timestamp)

df = (spark.read.table("dbo.silver_fhvhv")
      .filter(f"processing_timestamp = to_timestamp('{processing_timestamp}')"))
print("Silver rows in this batch:", df.count())
 


# In[4]:


# ---------- CELL 4: select gold metrics (no lookups) ----------
df_gold = df.select(
    col("company"),
    col("pickup_date"),
    date_format("dropoff_datetime", "yyyy-MM-dd").alias("dropoff_date"),
    col("pu_location_id"),
    col("do_location_id"),
    col("shared_request"),
    col("shared_match"),
    col("wav_request"),
    col("trip_miles"),
    col("trip_minutes"),
    col("avg_mph"),
    col("base_passenger_fare"),
    col("tips"),
    col("total_fare"),
    col("processing_timestamp"))
 
# re-run safety: remove this batch if it was already loaded
if spark.sql("SHOW TABLES IN dbo LIKE 'gold_fhvhv'").count() > 0:
    spark.sql(f"DELETE FROM dbo.gold_fhvhv WHERE processing_timestamp = to_timestamp('{processing_timestamp}')")
 
df_gold.write.format("delta").mode("append").saveAsTable("dbo.gold_fhvhv")
print("Gold layer written to dbo.gold_fhvhv")
 


# In[5]:


# ---------- CELL 5: validate (screenshot) ----------
g = spark.table("dbo.gold_fhvhv").filter(f"processing_timestamp = to_timestamp('{processing_timestamp}')")
print("Gold rows for this batch:", g.count())
display(g.limit(20))


# In[7]:


# ---------- CELL 6: small summary tables for Power BI and the Reflex ----------
# (aggregated from gold_fhvhv; each is a few hundred rows so Power BI stays fast)
def save_summary(d, name):
    spark.sql(f"DROP TABLE IF EXISTS dbo.{name}")
    d.write.format("delta").mode("append").saveAsTable(f"dbo.{name}")
    print(name, "rows:", spark.table(f"dbo.{name}").count())
 
all_gold = spark.table("dbo.gold_fhvhv").filter(f"processing_timestamp = to_timestamp('{processing_timestamp}')")
 
save_summary(
    all_gold.groupBy("pickup_date", "company").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_fare"), 2).alias("revenue"),
        F.round(F.sum("tips"), 2).alias("tips"),
        F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_minutes")),
    "gold_daily_company")
 
save_summary(
    all_gold.groupBy("pu_location_id", "pickup_date").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("total_fare"), 2).alias("avg_fare")),
    "gold_zone_daily")
 
save_summary(
    all_gold.groupBy("pickup_date").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("avg_mph"), 2).alias("avg_mph"),
        F.round(F.avg("tips"), 2).alias("avg_tip"))
    .withColumn("refreshed_ts", current_timestamp()),
    "gold_daily_health")


# In[ ]:


for t in ["dbo.silver_fhvhv", "dbo.gold_fhvhv"]:
       latest = spark.sql(f"SELECT max(processing_timestamp) FROM {t}").first()[0]
       spark.sql(f"DELETE FROM {t} WHERE processing_timestamp < to_timestamp('{latest}')")
       spark.sql(f"SELECT processing_timestamp, COUNT(*) AS row_count FROM {t} GROUP BY processing_timestamp").show(truncate=False)

