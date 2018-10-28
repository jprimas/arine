import sys
import uuid
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, sum, last
from pyspark.sql.types import StringType, FloatType, DateType
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

ds = glueContext.create_dynamic_frame.from_catalog(database = "arine", table_name = "raw_pharmacy_1_csv", transformation_ctx = "ds1")

ds = ApplyMapping.apply(frame = ds, mappings = [
    ("mbr01_member_number", "string", "memberNumber", "string"),
    ("clm09_date_of_service", "string", "fillDate", "string"),
    ("clm10_product_service_identification", "long", "product_service_identification", "string"),
    ("clm11_product_service_name", "string", "brandName", "string"),
    ("clm12_product_service_generic_name", "string", "genericName", "string"),
    ("clm14_decimal_quantity_dispensed", "double", "quantity", "double"),
    ("clm15_days_supply", "long", "daysSupply", "long"),
    ("clm27_unit_dose_indicator", "long", "dosageUnit", "long"),
    ("drg13_route_description", "string", "route", "string"),
    ("drg14_strength", "string", "dosageStrength", "string"),
    ("drg25_dosage_form", "string", "doseForm", "string")
    ], transformation_ctx = "ds2")
    
# Convert to spark DF
df = ds.toDF()

# Create MemberNumber -> patientId Map
temp_dict = {}
for row in df.select("memberNumber").distinct().collect():
    temp_dict[str(row.memberNumber)] = str(uuid.uuid4())
# Share this dictionary with all workers
patient_id_dict = sc.broadcast(temp_dict)
def get_patient_id(member_number):
    return patient_id_dict.value[str(member_number)]

# Add new column patientId based on memberNumber
get_patient_id_udf = udf(get_patient_id, StringType())
df = df.withColumn("patientId", get_patient_id_udf(df["memberNumber"]))
# Convert date string to a datetime
convert_date_udf = udf(lambda date_str: str(datetime.strptime(date_str, "%Y-%m-%d")), StringType())
df = df.withColumn("fillDate", convert_date_udf(df["fillDate"]))
# Add new column ndc9 based on substring of product_service_identification
create_ndc9_udf = udf(lambda id: id[:9] if id else id, StringType())
df = df.withColumn("ndc9", create_ndc9_udf(df["product_service_identification"]))
# drop columns product_service_identification and memberNumber
df = df.drop('product_service_identification').drop('memberNumber')
# Clean up NaN values for daysSupply, dosageUnit, quantity
df = df.fillna({'daysSupply': 0, 'dosageUnit': 0, 'quantity': 0})
# Aggregate over patientId, ndc9, and fillDate
agg_df = df.rollup("patientId", "ndc9", "fillDate").agg(
    sum("quantity").alias("quantity"),
    sum("daysSupply").alias("daysSupply"),
    last("brandName").alias("brandName"),
    last("genericName").alias("genericName"),
    last("dosageStrength").alias("dosageStrength"),
    last("dosageUnit").alias("dosageUnit"),
    last("doseForm").alias("doseForm"),
    last("route").alias("route"))
# Add new column unitsPerDay
divide_udf = udf(lambda x,y: x/y if y else 0, FloatType())
agg_df = agg_df.withColumn("unitsPerDay", divide_udf(agg_df["quantity"],agg_df["daysSupply"]))
#Write data to S3
glue_df = DynamicFrame.fromDF(agg_df, glueContext, "glue_df")
glueContext.write_dynamic_frame.from_options(
    frame = glue_df,
    connection_type = "s3",
    connection_options = {"path": "s3://arine-raw-pharmacy-1/etl/MedicationDispense", "partitionKeys": ["patientId"]},
    format = "parquet",
    transformation_ctx = "datasink4")

job.commit()