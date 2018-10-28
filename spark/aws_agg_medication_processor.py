import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, sum, last, first
from pyspark.sql.types import StringType, DateType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ds = glueContext.create_dynamic_frame.from_catalog(database = "arine", table_name = "medicationdispense", transformation_ctx = "ds0")

ds = ApplyMapping.apply(frame = ds, mappings = [
    ("ndc9", "string", "ndc9", "string"),
    ("filldate", "string", "filldate", "date"),
    ("quantity", "double", "quantity", "double"),
    ("dayssupply", "long", "dayssupply", "long"),
    ("brandname", "string", "brandname", "string"),
    ("genericname", "string", "genericname", "string"),
    ("dosagestrength", "string", "dosagestrength", "string"),
    ("dosageunit", "long", "dosageunit", "long"),
    ("doseform", "string", "doseform", "string"),
    ("route", "string", "route", "string"),
    ("unitsperday", "float", "unitsperday", "float"),
    ("patientid", "string", "patientid", "string")],
    transformation_ctx = "am1")
    
df = ds.toDF()

# Aggregate data over patientId and genericName
agg_df = df.orderBy("fillDate").rollup("patientId", "genericName").agg(
    last("ndc9", True).alias("ndc9"),
    last("quantity", True).alias("quantity"),
    last("dayssupply", True).alias("daysSupply"),
    last("unitsperday", True).alias("unitsPerDay"),
    last("brandName", True).alias("brandName"),
    last("dosagestrength", True).alias("dosageStrength"),
    last("dosageunit", True).alias("dosageUnit"),
    last("doseform", True).alias("doseForm"),
    last("route").alias("route"),
    first("fillDate", True).alias("firstFillDateTime"),
    last("fillDate", True).alias("lastFillDateTime"),
    sum("dayssupply").alias("cumDaysSupply"))
# Convert the dates back into a string
date_to_str_udf = udf(lambda date: str(date), StringType())
agg_df = agg_df.withColumn("firstFillDate", date_to_str_udf(agg_df["firstFillDateTime"])).withColumn("lastFillDate", date_to_str_udf(agg_df["lastFillDateTime"]))
# Drop columns firstFillDateTime and lastFillDateTime
agg_df = agg_df.drop('firstFillDateTime').drop('lastFillDateTime')

glue_df = DynamicFrame.fromDF(agg_df, glueContext, "glue_df")


glueContext.write_dynamic_frame.from_options(
    frame = glue_df,
    connection_type = "s3",
    connection_options = {"path": "s3://arine-raw-pharmacy-1/etl/MedicationEra", "partitionKeys": ["patientId"]},
    format = "parquet",
    transformation_ctx = "datasink4")
job.commit()