
  # Silver Clean Notebook 
  --------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql.types import *
from pyspark.sql.functions import *


#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#Defin Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

#Parse it to dataframe
parsed_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

#convert type to Timestamp
clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))

#invalid admission_times
clean_df = clean_df.withColumn("admission_time",
                               when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                   current_timestamp())
                               .otherwise(col("admission_time")))

#Handle Invalid Age
clean_df = clean_df.withColumn("age",
                               when(col("age")>100,floor(rand()*90+1).cast("int"))
                               .otherwise(col("age"))
                               )

#schema evolution
expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

#Write to silver table
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema","true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)

------------------------------------------------------------------------------------------------------------------------------------------------

clean_df.printSchema()
O/P :
root
 |-- patient_id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- department: string (nullable = true)
 |-- admission_time: timestamp (nullable = true)
 |-- discharge_time: timestamp (nullable = true)
 |-- bed_id: integer (nullable = true)
 |-- hospital_id: integer (nullable = true)


display(spark.read.format("delta").load(silver_path))
O/P :
patient_id gender  age department admission_time discharge_time bed_id hospital_id
3ef42adc-0019-4e27-ad71-72dcdd3b3aee	Male	6	Cardiology	2026-06-16T07:24:07.057+00:00	2026-06-18T13:24:07.057+00:00	445	2
a9750a91-01a1-4e23-8fd1-f22339527762	Female	52	Emergency	2026-06-16T20:24:08.062+00:00	2026-06-19T04:24:08.062+00:00	428	5
a9f98a5e-1274-4c88-bc09-072c2e9c5c65	Female	76	Maternity	2026-06-16T02:24:09.064+00:00	2026-06-18T16:24:09.064+00:00	372	4

spark.read.format("delta").load(silver_path).count()
O/P : 3835


# -------------------------------------- || END || ---------------------------------------------
# Do not run below code : This was try and error code. Above code is perfect.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import *


#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# Define Schema 
schema = StructType([
        StructField("PatientID", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("AdmissionDate", TimestampType(), True),
        StructField("DischargeDate", TimestampType(), True),
        StructField("Admission_Time", StringType(), True),
        StructField("Discharge_Time", StringType(), True),
        StructField("Bed_ID", IntegerType(), True),
        StructField("Hospital_ID", IntegerType(), True)
])



# Parse it to Dataframe

parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

clean_df = parsed_df.select(
    col("PatientID").alias("patient_id"),
    col("Age").alias("age"),
    col("Gender").alias("gender"),
    col("Department").alias("department"),
    col("AdmissionDate"),
    col("DischargeDate"),
    col("Admission_Time").alias("admission_time"),
    col("Discharge_Time").alias("discharge_time"),
    col("Bed_ID").alias("bed_id"),
    col("Hospital_ID").alias("hospital_id")
)

# Convert type to Date
from pyspark.sql.functions import col, to_timestamp

clean_df = clean_df.withColumn(
    "AdmissionDate",
    to_timestamp(col("AdmissionDate"), "MM-dd-yyyy")
).withColumn(
    "DischargeDate",
    to_timestamp(col("DischargeDate"), "MM-dd-yyyy")
)


clean_df = clean_df.withColumn(
    "admission_time",
    to_timestamp("admission_time")
)

clean_df = clean_df.withColumn(
    "discharge_time",
    to_timestamp("discharge_time")
)

# Invalid Admission_time
clean_df = clean_df.withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() |
        (col("admission_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("admission_time"))
)

clean_df = clean_df.withColumn(
    "discharge_time",
    when(
        col("discharge_time").isNull() |
        (col("discharge_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("discharge_time"))
)

#Handle Invalid Age
clean_df = clean_df.withColumn("age",
                               when(col("age")>100,floor(rand()*90+1).cast("int"))
                               .otherwise(col("age"))
                               )

#schema evolution
expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]


if "patient_id" not in clean_df.columns:
    clean_df = clean_df.withColumn(
        "patient_id",
        lit(None).cast("string")
    )
