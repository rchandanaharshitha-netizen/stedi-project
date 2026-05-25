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

# Read step trainer trusted from S3
step_trainer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/step_trainer/trusted/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_trusted"
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

print("Step trainer trusted count :", step_trainer_df.count())
print("Accelerometer trusted count:", accelerometer_df.count())

# Join on timestamp = sensorreadingtime
joined_df = step_trainer_df.join(
    accelerometer_df,
    step_trainer_df["sensorreadingtime"] == accelerometer_df["timestamp"],
    "inner"
)

print("Machine learning curated count:", joined_df.count())

# Write to machine learning curated S3 location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        joined_df, glueContext, "machine_learning_curated"
    ),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-chandana/machine_learning/curated/",
        "partitionKeys": []
    },
    transformation_ctx="write_ml_curated"
)

job.commit()