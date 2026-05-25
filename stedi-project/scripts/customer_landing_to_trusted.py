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

customer_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-chandana/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_landing"
).toDF()

print("Landing columns  :", customer_df.columns)
print("Landing row count:", customer_df.count())

customer_trusted_df = customer_df.filter(
    (customer_df["sharewithresearchasofdate"].isNotNull()) &
    (customer_df["sharewithresearchasofdate"] > 0)
)

print("Trusted row count:", customer_trusted_df.count())

glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        customer_trusted_df, glueContext, "customer_trusted"
    ),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-chandana/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="write_customer_trusted"
)

job.commit()