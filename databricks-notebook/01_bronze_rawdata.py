


# Here I am going to take the data out from the event streama and converted from binary into JSON stream and loaded in bronze table.
# Any data that comes from Kafka it is in binary form so we have to convert it.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "NS-hospital-analytics-namespace.servicebus.windows.net"
event_hub_name="eh-hospital-analytics"  
event_hub_conn_str = dbutils.secrets.get(scope = "hospitalanalyticssecretscope", key = "Eventhub-Connection")

# hospitalanalyticssecretscope

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

# To write the data into Bronze layer in ADLS Gen2

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

# Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bronze/_checkpoints/patient_flow")
    .start(bronze_path)
)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

display( spark.read.format("delta").load(bronze_path))
O/P :
{"patient_id": "3ef42adc-0019-4e27-ad71-72dcdd3b3aee", "gender": "Male", "age": 6, "department": "Cardiology", "admission_time": "2026-06-16T07:24:07.057924", "discharge_time": "2026-06-18T13:24:07.057924", "bed_id": 445, "hospital_id": 2}
{"patient_id": "a9750a91-01a1-4e23-8fd1-f22339527762", "gender": "Female", "age": 52, "department": "Emergency", "admission_time": "2026-06-16T20:24:08.062721", "discharge_time": "2026-06-19T04:24:08.062721", "bed_id": 428, "hospital_id": 5}
{"patient_id": "a9f98a5e-1274-4c88-bc09-072c2e9c5c65", "gender": "Female", "age": 76, "department": "Maternity", "admission_time": "2026-06-16T02:24:09.064390", "discharge_time": "2026-06-18T16:24:09.064390", "bed_id": 372, "hospital_id": 4}

spark.read.format("delta").load(bronze_path).printSchema()
O/P :
root
 |-- raw_json: string (nullable = true)


dbutils.fs.ls("abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/")
O/P:
[FileInfo(path='abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/_checkpoints/', name='_checkpoints/', size=0, modificationTime=1781681045000),
 FileInfo(path='abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/patient_flow/', name='patient_flow/', size=0, modificationTime=1781681683000)]

dbutils.fs.ls("abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/_checkpoints/patient_flow")

spark.read.format("delta").load(bronze_path).count()
O/P : 3835

for s in spark.streams.active:
    print(s.status)
O/P : {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}


# Manual entry for testing   : I have try this testing after the gold layer ceration is done. 

import json

manual_json = {
    "patient_id": "3ef42adc-0019-4e27-ad71-72dcdd3b3aee",
    "gender": "Male",
    "age": 80,
    "department": "Cardiology",
    "admission_time": "2026-06-19T09:36:07.057924",
    "discharge_time": "2026-06-28T13:24:07.057924",
    "bed_id": 445,
    "hospital_id": 2
}

manual_df = spark.createDataFrame(
    [(json.dumps(manual_json),)],
    ["raw_json"]
)

# Copy this patient_id => "3ef42adc-0019-4e27-ad71-72dcdd3b3aee"
# Run the Silver notebook to get the incremented record .

manual_df.printSchema()

O/P : manual_df:pyspark.sql.connect.dataframe.DataFrame = [raw_json: string]
root
 |-- raw_json: string (nullable = true)


manual_df.write.format("delta").mode("append").save(bronze_path)


display(spark.read.format("delta").load(bronze_path))

# To Stop the Stream :

for stream in spark.streams.active:
stream.stop()

===================================================== || ========================================================