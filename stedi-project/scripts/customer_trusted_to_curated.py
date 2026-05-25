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

# Read accelerometer trusted from S3
accelerometer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/accelerometer/trusted/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_trusted"
).toDF()

print("Customer trusted count     :", customer_df.count())
print("Accelerometer trusted count:", accelerometer_df.count())

# Keep only customers who have accelerometer data
# Join on email = user, keep only customer columns
joined_df = customer_df.join(
    accelerometer_df.select("user").distinct(),
    customer_df["email"] == accelerometer_df["user"],
    "inner"
).select(
    customer_df["serialnumber"],
    customer_df["sharewithpublicasofdate"],
    customer_df["birthday"],
    customer_df["registrationdate"],
    customer_df["sharewithresearchasofdate"],
    customer_df["customername"],
    customer_df["email"],
    customer_df["lastupdatedate"],
    customer_df["phone"],
    customer_df["sharewithfriendsasofdate"]
)

print("Customer curated count:", joined_df.count())

# Write to customer curated S3 location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        joined_df, glueContext, "customer_curated"
    ),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-chandana/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="write_customer_curated"
)

job.commit()