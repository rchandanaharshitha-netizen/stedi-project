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

accelerometer_trusted = DynamicFrame.fromDF(
    joined_df, glueContext, "accelerometer_trusted"
)

sink = glueContext.getSink(
    path="s3://stedi-project-chandana/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="write_accelerometer_trusted"
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="accelerometer_trusted"
)
sink.setFormat("json")
sink.writeFrame(accelerometer_trusted)

job.commit()
