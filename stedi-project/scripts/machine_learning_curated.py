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

joined_df = step_trainer_df.join(
    accelerometer_df,
    step_trainer_df["sensorreadingtime"] == accelerometer_df["timestamp"],
    "inner"
)

ml_curated = DynamicFrame.fromDF(
    joined_df, glueContext, "machine_learning_curated"
)

sink = glueContext.getSink(
    path="s3://stedi-project-chandana/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="write_ml_curated"
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated"
)
sink.setFormat("json")
sink.writeFrame(ml_curated)

job.commit()
