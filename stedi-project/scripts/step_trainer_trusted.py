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
        "paths": ["s3://stedi-project-chandana/step_trainer/landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
).toDF()

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

joined_df = step_trainer_df.join(
    customer_df.select("serialnumber").distinct(),
    step_trainer_df["serialnumber"] == customer_df["serialnumber"],
    "inner"
).select(
    step_trainer_df["sensorreadingtime"],
    step_trainer_df["serialnumber"],
    step_trainer_df["distancefromobject"]
)

step_trainer_trusted = DynamicFrame.fromDF(
    joined_df, glueContext, "step_trainer_trusted"
)

sink = glueContext.getSink(
    path="s3://stedi-project-chandana/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="write_step_trainer_trusted"
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="step_trainer_trusted"
)
sink.setFormat("json")
sink.writeFrame(step_trainer_trusted)

job.commit()
