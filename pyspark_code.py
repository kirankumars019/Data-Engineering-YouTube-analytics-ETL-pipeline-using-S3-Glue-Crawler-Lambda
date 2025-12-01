import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when
from awsglue.dynamicframe import DynamicFrame

# Initialize job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------------------------
# Load Catalog Tables - Source
# --------------------------------------------------------------------

ref_df = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned5",
    table_name="cleaned_statistics_reference_data5",
    transformation_ctx="ref_data"
)

raw_df = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned5",
    table_name="raw_statistics",
    transformation_ctx="raw_data"
)

# --------------------------------------------------------------------
# Convert Schema + Clean Join Columns
# --------------------------------------------------------------------

print("Casting and cleaning join columns...")

raw_spark_df = raw_df.toDF().withColumn(
    "category_id",
    when(col("category_id").isNull(), "-1")
    .when(col("category_id") == "", "-1")
    .otherwise(col("category_id").cast("string"))
)

ref_spark_df = ref_df.toDF().withColumn(
    "id",
    col("id").cast("string")
)

# Convert back to DynamicFrames
raw_cleaned = DynamicFrame.fromDF(raw_spark_df, glueContext, "raw_cleaned")
ref_cleaned = DynamicFrame.fromDF(ref_spark_df, glueContext, "ref_cleaned")

# --------------------------------------------------------------------
# Join 2 Table
# --------------------------------------------------------------------

Join_node = Join.apply(
    frame1=raw_cleaned,
    frame2=ref_cleaned,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node"
)

print("Schema after join:")
Join_node.printSchema()

# --------------------------------------------------------------------
# Write to S3 + Glue Catalog -Target
# --------------------------------------------------------------------

sink = glueContext.getSink(
    path="s3://de-on-youtube-analytic-useast1-dev5",
    connection_type="s3",
    enableUpdateCatalog=True,         # <-- allows Glue to create the table
    partitionKeys=["region", "category_id"],
    transformation_ctx="S3_sink"
)

sink.setCatalogInfo(
    catalogDatabase="db_youtube_analytics5",
    catalogTableName="final_analytics5"
)

sink.setFormat("glueparquet", compression="snappy")

print("Writing data to S3 + Glue Catalog: line 80")
sink.writeFrame(Join_node)

job.commit()
print("Job completed successfully!")
