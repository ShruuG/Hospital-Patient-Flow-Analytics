
	# Gold layer Notebook  for Transformation
 =======================================
 
# ----------------------------------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)


# Paths
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_department = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

# Read silver data (assume append-only)
silver_df = spark.read.format("delta").load(silver_path)

# Define window for latest admission per patient
w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

silver_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(w))  # Rank by latest admission_time
    .filter(F.col("row_num") == 1)                  # Keep only latest row
    .drop("row_num")
)

# ==================================================================
# Build / Upsert patient_dim (SCD Type 2)
# Natural key : patient_id
# Attribute to track : gender , age
# ==================================================================

#   Patient Dimension Table Creation
# =======================================

#   Prepare incoming dimension records (deduplicated per patient, latest record)
# ==================================================================
incoming_patient = (silver_df
                    .select("patient_id", "gender", "age")
                    .withColumn("effective_from", current_timestamp())
                   )

#   Create target if not exists
# =========================
if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    # initialize table with schema and empty data
    incoming_patient.withColumn("surrogate_key", F.monotonically_increasing_id()) \
                    .withColumn("effective_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .write.format("delta").mode("overwrite").save(gold_dim_patient)

# 		Load target as DeltaTable
# =========================
target_patient = DeltaTable.forPath(spark, gold_dim_patient)

# Create an expression to detect attribute changes (hash or explicit comparisons)
# We'll use a simple concat hash to detect changes
# ==================================================================

incoming_patient = incoming_patient.withColumn(
    "_hash",
    F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
)

#   Bring target current hash
# =======================
target_patient_df = spark.read.format("delta").load(gold_dim_patient).withColumn(
    "_target_hash",
    F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
).select("surrogate_key", "patient_id", "gender", "age", "is_current", "_target_hash", "effective_from", "effective_to")

#   Create temp views for merge
# ===========================
incoming_patient.createOrReplaceTempView("incoming_patient_tmp")
target_patient_df.createOrReplaceTempView("target_patient_tmp")

# We'll implement in two steps using Delta MERGE (safe & explicit)

# 1) Mark old current rows as not current where changed
# ==================================================================

changes_df = spark.sql("""
SELECT t.surrogate_key, t.patient_id
FROM target_patient_tmp t
JOIN incoming_patient_tmp i
  ON t.patient_id = i.patient_id
WHERE t.is_current = true AND t._target_hash <> i._hash
""")

changed_keys = [row['surrogate_key'] for row in changes_df.collect()]

if changed_keys:
    # Update existing current records: set is_current=false and effective_to=current_timestamp()
    # ==================================================================

	target_patient.update(
        condition = expr("is_current = true AND surrogate_key IN ({})".format(",".join([str(k) for k in changed_keys]))),
        set = {
            "is_current": expr("false"),
            "effective_to": expr("current_timestamp()")
        }
    )

# 2) Insert new rows for changed & new records
# Build insert DF: join incoming with target to figure new inserts where either not exists or changed
# ==================================================================

inserts_df = spark.sql("""
SELECT i.patient_id, i.gender, i.age, i.effective_from, i._hash
FROM incoming_patient_tmp i
LEFT JOIN target_patient_tmp t
  ON i.patient_id = t.patient_id AND t.is_current = true
WHERE t.patient_id IS NULL OR t._target_hash <> i._hash
""").withColumn("surrogate_key", F.monotonically_increasing_id()) \
  .withColumn("effective_to", lit(None).cast("timestamp")) \
  .withColumn("is_current", lit(True)) \
  .select("surrogate_key", "patient_id", "gender", "age", "effective_from", "effective_to", "is_current")

#     Append new rows
# =====================
if inserts_df.count() > 0:
    inserts_df.write.format("delta").mode("append").save(gold_dim_patient)

# ======================================================================
#   Build / Upsert department_dim (SCD Type 2)
#   Natural key : department + hospotal_id
#   Attribute to track : capacity ( if you have ), staff_count (example)
# =======================================================================

#   Department Dimension Table Creation
# =======================================

#   prepare incoming (latest per patient feed snapshot)
# ==============================================

incoming_dept = (silver_df
                 .select("department", "hospital_id")
                )

#   add hash and dedupe incoming (one row per natural key)
# ===================================================

incoming_dept = incoming_dept.dropDuplicates(["department", "hospital_id"]) \
    .withColumn("surrogate_key", monotonically_increasing_id())

# initialize table if missing
# =======================
incoming_dept.select("surrogate_key", "department", "hospital_id") \
    .write.format("delta").mode("overwrite").save(gold_dim_department)

# ======================================================================
#   Build fact_patient_flow
#   Join silver events with latest dimension surrogate keys to build star schema fact records
# =======================================================================

#   Create Fact table
# =====================

#    Read current dims (filter is_current=true)
# ========================================
dim_patient_df = (spark.read.format("delta").load(gold_dim_patient)
                  .filter(col("is_current") == True)
                  .select(col("surrogate_key").alias("surrogate_key_patient"), "patient_id", "gender", "age"))

dim_dept_df = (spark.read.format("delta").load(gold_dim_department)
               .select(col("surrogate_key").alias("surrogate_key_dept"), "department", "hospital_id"))

#     Build base fact from silver events
# =================================
fact_base = (silver_df
             .select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id")
             .withColumn("admission_date", F.to_date("admission_time"))
            )

#    Join to get surrogate keys
# ========================
fact_enriched = (fact_base
                 .join(dim_patient_df, on="patient_id", how="left")
                 .join(dim_dept_df, on=["department", "hospital_id"], how="left")
                )

#   Compute metrics
# ==================
fact_enriched = fact_enriched.withColumn("length_of_stay_hours",
                                         (F.unix_timestamp(col("discharge_time")) - F.unix_timestamp(col("admission_time"))) / 3600.0) \
                             .withColumn("is_currently_admitted", F.when(col("discharge_time") > current_timestamp(), lit(True)).otherwise(lit(False))) \
                             .withColumn("event_ingestion_time", current_timestamp())

#   NOTE : fix department surrogate key column name depending on join result
# =======================================================================

#    Let's make column names explicit instead:
# =====================================
fact_final = fact_enriched.select(
    F.monotonically_increasing_id().alias("fact_id"),
    col("surrogate_key_patient").alias("patient_sk"),
    col("surrogate_key_dept").alias("department_sk"),
    "admission_time",
    "discharge_time",
    "admission_date",
    "length_of_stay_hours",
    "is_currently_admitted",
    "bed_id",
    "event_ingestion_time"
)

#   Persist fact table partitioned by admission_date (helps Synapse / queries)
# ===============================================================

fact_final.write.format("delta").mode("overwrite").save(gold_fact)


#    Quick sanity checks
# ====================
print("Patient dim count:", spark.read.format("delta").load(gold_dim_patient).count())
print("Department dim count:", spark.read.format("delta").load(gold_dim_department).count())
print("Fact rows:", spark.read.format("delta").load(gold_fact).count())

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

O/P :

changes_df:pyspark.sql.connect.dataframe.DataFrame = [surrogate_key: long, patient_id: string]
dim_dept_df:pyspark.sql.connect.dataframe.DataFrame = [surrogate_key_dept: long, department: string ... 1 more field]
dim_patient_df:pyspark.sql.connect.dataframe.DataFrame = [surrogate_key_patient: long, patient_id: string ... 2 more fields]
fact_base:pyspark.sql.connect.dataframe.DataFrame = [patient_id: string, department: string ... 5 more fields]
fact_enriched:pyspark.sql.connect.dataframe.DataFrame = [department: string, hospital_id: integer ... 12 more fields]
fact_final:pyspark.sql.connect.dataframe.DataFrame = [fact_id: long, patient_sk: long ... 8 more fields]
incoming_dept:pyspark.sql.connect.dataframe.DataFrame = [department: string, hospital_id: integer ... 1 more field]
incoming_patient:pyspark.sql.connect.dataframe.DataFrame = [patient_id: string, gender: string ... 3 more fields]
inserts_df:pyspark.sql.connect.dataframe.DataFrame = [surrogate_key: long, patient_id: string ... 5 more fields]
silver_df:pyspark.sql.connect.dataframe.DataFrame = [patient_id: string, gender: string ... 6 more fields]
target_patient_df:pyspark.sql.connect.dataframe.DataFrame
surrogate_key:long
patient_id:string
gender:string
age:integer
is_current:boolean
_target_hash:string
effective_from:timestamp
effective_to:timestamp

Patient dim count: 3833
Department dim count: 49
Fact rows: 3832

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

display(spark.read.format("delta").load(gold_dim_patient))
O/P :
patient_id gender age effective_from surrogate_key effective_to is_current
000c484e-d36f-433f-8f8d-d830453e9df1	Male	74	2026-06-18T08:49:01.390+00:00	0	-	true
001f4b05-b290-4d41-b199-9e07cb46b48e	Female	79	2026-06-18T08:49:01.390+00:00	1	-	true
002b203a-1ff2-4c00-bde1-3024aa4c7a0d	Male	33	2026-06-18T08:49:01.390+00:00	2	-	true

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/dim_patient"
display(spark.read.format("delta").load(gold_dim_patient))

display(spark.read.format("delta").load(gold_dim_department))

display(spark.read.format("delta").load(gold_fact))

#     Filtering the manual intered record :
#  ----------------------------------------------------

display(spark.read.format("delta").load(gold_dim_patient).filter("patient_id = '3ef42adc-0019-4e27-ad71-72dcdd3b3aee'"))
O/P :
patient_id	gender	age	effective_from	surrogate_key	effective_to	is_current
3ef42adc-0019-4e27-ad71-72dcdd3b3aee	Male	6	2026-06-18T08:49:01.390+00:00	771	2026-06-18T10:49:59.372+00:00	false
3ef42adc-0019-4e27-ad71-72dcdd3b3aee	Male	80	2026-06-18T10:50:04.470+00:00	0	-	true

#    To know the cluster ID of databricks.
#  --------------------------------------------------
spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

spark.read.format("delta").load("abfss://gold@<<Storageaccount_name>>.core.windows.net/fact_patient_flow").count()

spark.read.format("delta").load("abfss://gold@<<Storageaccount_name>>.core.windows.net/dim_patient") \
    .write.mode("overwrite") \
    .parquet("abfss://gold@<<Storageaccount_name>>.core.windows.net/dim_patient_parquet")

display(dbutils.fs.ls("abfss://gold@<<Storageaccount_name>>.core.windows.net/dim_patient_parquet"))

spark.read.parquet(
    "abfss://gold@<<Storageaccount_name>>.core.windows.net/dim_patient_parquet"
).printSchema()
O/P :
root
 |-- patient_id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- effective_from: timestamp (nullable = true)
 |-- surrogate_key: long (nullable = true)
 |-- effective_to: timestamp (nullable = true)
 |-- is_current: boolean (nullable = true)

spark.read.parquet(
    "abfss://gold@<<Storageaccount_name>>.core.windows.net/dim_patient_parquet"
).printSchema()
O/P :
root
 |-- patient_id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- effective_from: timestamp (nullable = true)
 |-- surrogate_key: long (nullable = true)
 |-- effective_to: timestamp (nullable = true)
 |-- is_current: boolean (nullable = true)



spark.read.parquet(
    "abfss://gold@<<Storageaccount_name>>.dfs.core.windows.net/dim_patient_parquet"
).show(5, truncate=False)
O/P :
+------------------------------------+------+---+-------------------------+-------------+------------+----------+
|patient_id                          |gender|age|effective_from           |surrogate_key|effective_to|is_current|
+------------------------------------+------+---+-------------------------+-------------+------------+----------+
|000c484e-d36f-433f-8f8d-d830453e9df1|Male  |74 |2026-06-18 08:49:01.39087|0            |NULL        |true      |
|001f4b05-b290-4d41-b199-9e07cb46b48e|Female|79 |2026-06-18 08:49:01.39087|1            |NULL        |true      |
|002b203a-1ff2-4c00-bde1-3024aa4c7a0d|Male  |33 |2026-06-18 08:49:01.39087|2            |NULL        |true      |
|002c03bc-5c99-40af-ae68-b2edbe46eb7f|Female|18 |2026-06-18 08:49:01.39087|3            |NULL        |true      |
|003951b7-c4f2-43ca-86a9-24c198a86bf8|Female|76 |2026-06-18 08:49:01.39087|4            |NULL        |true      |
+------------------------------------+------+---+-------------------------+-------------+------------+----------+
only showing top 5 rows


df = spark.read.parquet(
    "abfss://gold@<<Storageaccount_name>>.dfs.core.windows.net/dim_patient_parquet"
)

df.printSchema()
display(df.limit(5))
O/P :
root
 |-- patient_id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- effective_from: timestamp (nullable = true)
 |-- surrogate_key: long (nullable = true)
 |-- effective_to: timestamp (nullable = true)
 |-- is_current: boolean (nullable = true)

patient_id	gender	age	effective_from	surrogate_key	effective_to	is_current
000c484e-d36f-433f-8f8d-d830453e9df1	Male	74	2026-06-18T08:49:01.390+00:00	0	-	true
001f4b05-b290-4d41-b199-9e07cb46b48e	Female	79	2026-06-18T08:49:01.390+00:00	1	-	true
002b203a-1ff2-4c00-bde1-3024aa4c7a0d	Male	33	2026-06-18T08:49:01.390+00:00	2	-	true
002c03bc-5c99-40af-ae68-b2edbe46eb7f	Female	18	2026-06-18T08:49:01.390+00:00	3	-	true
003951b7-c4f2-43ca-86a9-24c198a86bf8	Female	76	2026-06-18T08:49:01.390+00:00	4	-	true


# 1. Check Schemas in Databricks 

spark.read.format("delta").load(gold_dim_department).printSchema()

spark.read.format("delta").load(gold_fact).printSchema()

O/P :
root
 |-- surrogate_key: long (nullable = true)
 |-- department: string (nullable = true)
 |-- hospital_id: integer (nullable = true)

root
 |-- fact_id: long (nullable = true)
 |-- patient_sk: long (nullable = true)
 |-- department_sk: long (nullable = true)
 |-- admission_time: timestamp (nullable = true)
 |-- discharge_time: timestamp (nullable = true)
 |-- admission_date: date (nullable = true)
 |-- length_of_stay_hours: double (nullable = true)
 |-- is_currently_admitted: boolean (nullable = true)
 |-- bed_id: integer (nullable = true)
 |-- event_ingestion_time: timestamp (nullable = true)



# 1. Export Department Dimension to Parquet
# Export Department Dimension as Synapse-Friendly Parquet
# -----------------------------------------------------------------------------------------------------------------------

spark.read.format("delta").load(gold_dim_department) \
    .write.mode("overwrite") \
    .parquet(
        "abfss://gold@<<Storageaccount_name>>.dfs.core.windows.net/dim_department_parquet"
    )
	
#   2. Export Fact Table to Synapse-Friendly Parquet
# -----------------------------------------------------------------------
from pyspark.sql.functions import col

fact_synapse = (
    spark.read.format("delta").load(gold_fact)
    .withColumn(
        "is_currently_admitted",
        col("is_currently_admitted").cast("int")
    )
)

fact_synapse.write \
    .mode("overwrite") \
    .parquet(
        "abfss://gold@<<Storageaccount_name>>.dfs.core.windows.net/fact_patient_flow_parquet"
    )
O/P :
	
fact_id:long
patient_sk:long
department_sk:long
admission_time:timestamp
discharge_time:timestamp
admission_date:date
length_of_stay_hours:double
is_currently_admitted:integer
bed_id:integer
event_ingestion_time:timestamp

# ====================================================================== || ============================================================
