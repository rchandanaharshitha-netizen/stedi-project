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

# Read step trainer landing from S3
step_trainer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/step_trainer/landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
).toDF()

# Read customer curated from S3
customer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/customer/curated/"],
        "recurse": True
    },
    transformation_ctx="customer_curated"
).toDF()

print("Step trainer landing count:", step_trainer_df.count())
print("Customer curated count    :", customer_df.count())

# Join on serial number — keep only step trainer columns
joined_df = step_trainer_df.join(
    customer_df.select("serialnumber").distinct(),
    step_trainer_df["serialnumber"] == customer_df["serialnumber"],
    "inner"
).select(
    step_trainer_df["sensorreadingtime"],
    step_trainer_df["serialnumber"],
    step_trainer_df["distancefromobject"]
)

print("Step trainer trusted count:", joined_df.count())

# Write to step trainer trusted S3 location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        joined_df, glueContext, "step_trainer_trusted"
    ),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-chandana/step_trainer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="write_step_trainer_trusted"
)

job.commit()