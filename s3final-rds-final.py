import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------
# Glue job setup
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# Read CSV from S3
# -----------------------------
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    },
    connection_options={
        "paths": ["s3://glue-gcp-1111111111/transformation/custom_users.csv"]
    },
    transformation_ctx="source_df"
)

# -----------------------------
# Map schema to match your table
# -----------------------------
mapped_df = ApplyMapping.apply(
    frame=source_df,
    mappings=[
        ("id", "string", "id", "string"),
        ("user_name", "string", "user_name", "string"),
        ("Email", "string", "Email", "string"),
        ("age", "string", "age", "string"),
        ("city", "string", "city", "string"),
        ("country", "string", "country", "string"),
        ("status", "string", "status", "string"),
        ("phone", "string", "phone", "string")
    ],
    transformation_ctx="mapped_df"
)

# -----------------------------
# Write to MySQL RDS
# -----------------------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=mapped_df,
    catalog_connection="mysql-rds",  # Replace with your Glue connection
    connection_options={
        "database": "test",
        "dbtable": "users"  # Your target table
    },
    transformation_ctx="write_mysql"
)

job.commit()
