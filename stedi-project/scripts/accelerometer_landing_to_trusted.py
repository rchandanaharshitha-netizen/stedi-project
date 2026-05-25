import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read accelerometer landing from S3
accelerometer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
).toDF()

# Read customer trusted from S3
customer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="customer_trusted"
).toDF()

# Print schemas to CloudWatch for debugging
print("Accelerometer columns:", accelerometer_df.columns)
print("Accelerometer count  :", accelerometer_df.count())
print("Customer columns     :", customer_df.columns)
print("Customer count       :", customer_df.count())

# Inner join — keep only accelerometer columns
joined_df = accelerometer_df.join(
    customer_df,
    accelerometer_df["user"] == customer_df["email"],
    "inner"
).select(
    accelerometer_df["timestamp"],
    accelerometer_df["user"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)

print("Joined count:", joined_df.count())

# Write to accelerometer trusted S3 location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        joined_df, glueContext, "accelerometer_trusted"
    ),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-chandana/accelerometer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="write_accelerometer_trusted"
)

job.commit()