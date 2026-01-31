import sys
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
# JDBC read using Spark (users table)
# -----------------------------
jdbc_url = "jdbc:mysql://34.59.188.85:3306/test"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "users") \
    .option("user", "root") \
    .option("password", "Cloud@123") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# DEBUG: check data
print("Schema of users table:")
df.printSchema()
print("Row count:", df.count())
df.show(5)

# -----------------------------
# Write to S3 as single CSV
# -----------------------------
output_path = "s3://glue-gcp-1111111111/users/"

df.coalesce(1) \
  .write.mode("overwrite") \
  .option("header", True) \
  .csv(output_path)

print(f"Cleaned CSV written to: {output_path}")

job.commit()
