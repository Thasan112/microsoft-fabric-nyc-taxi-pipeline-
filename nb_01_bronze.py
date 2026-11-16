#!/usr/bin/env python
# coding: utf-8

# ## nb_01_bronze
# 
# null

# In[23]:


# ---------- CELL 1: importing libraries ----------
from pyspark.sql.functions import to_timestamp, col, lit, current_timestamp


# In[24]:


# ---------- CELL 2: parameters  (... menu -> Toggle parameter cell) ----------
processing_timestamp = ""       # the pipeline passes @utcnow() here


# In[25]:


# ---------- CELL 3: default when run by hand ----------
if not processing_timestamp:
    processing_timestamp = str(spark.sql("select current_timestamp()").first()[0])
print("Batch:", processing_timestamp)


# In[26]:


# ---------- CELL 4: prepare landing copy (raw shortcut stays untouched) ----------
RAW     = "Files/nyctaxi-raw"
LANDING = "Files/landing"
try:
    notebookutils.fs.rm(LANDING, True)       # clear previous run's copy
except Exception as e:
    print("Nothing to clear:", e)
notebookutils.fs.mkdirs(LANDING)
notebookutils.fs.cp(RAW, LANDING, True)
files = [f.path for f in notebookutils.fs.ls(LANDING) if f.name.endswith(".parquet")]
print(files)


# In[29]:


# ---------- CELL 4: prepare landing copy (raw shortcut stays untouched) ----------
RAW = "Files/nyctaxi-raw"
LANDING = "Files/landing"

try:
    notebookutils.fs.rm(LANDING, True)  # clear previous run's copy
except Exception as e:
    print("Nothing to clear:", e)

notebookutils.fs.mkdirs(LANDING)
notebookutils.fs.cp(RAW, LANDING, True)

# Recursively find all parquet files in the landing zone (handles nested folders if any)
files = []
def list_recursive(path):
    for f in notebookutils.fs.ls(path):
        if f.isDir:
            list_recursive(f.path)
        elif f.name.endswith(".parquet"):
            files.append(f.path)

list_recursive(LANDING)

print(f"Total parquet files found: {len(files)}")
print(files[:5]) # Print first few files to verify paths


# In[30]:


# ---------- CELL 5: read parquet, add processing timestamp, append to BRONZE ----------
df = None
for path in files:
    d = (spark.read.format("parquet").load(path)
           .withColumn("processing_timestamp", to_timestamp(lit(processing_timestamp)))
           .withColumn("source_file", lit(path.split("/")[-1])))
    for c in ["airport_fee", "cbd_congestion_fee"]:          # types drift between months
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("double"))
    df = d if df is None else df.unionByName(d, allowMissingColumns=True)

df.write.mode("append").option("mergeSchema", "true").saveAsTable("bronze_fhvhv")
# schema-enabled lakehouse alternative: df.write.mode("append").save("Tables/bronze/nyc_fhvhv")


# In[31]:


# CELL 6: validate this batch (screenshot) ----------
b = spark.table("bronze_fhvhv").filter(f"processing_timestamp = '{processing_timestamp}'")
bronze_rows = b.count()
raw_rows = sum(spark.read.parquet(p).count() for p in files)
print("Raw rows:", raw_rows, "| Bronze rows this batch:", bronze_rows, "| match:", raw_rows == bronze_rows)
b.groupBy("source_file").count().orderBy("source_file").show(truncate=False)
display(b.limit(20))


# In[32]:


print(f"Raw rows calculated: {raw_rows}")
print(f"Bronze rows found for this batch: {bronze_rows}")


# In[33]:


# ---------- CELL 8: return batch id to the pipeline ----------
notebookutils.notebook.exit(processing_timestamp)

