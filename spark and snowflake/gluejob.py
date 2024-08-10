import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1723275334108 = glueContext.create_dynamic_frame.from_catalog(database="community-finance-database", table_name="dbo_account_csv", transformation_ctx="AWSGlueDataCatalog_node1723275334108")

# Script generated for node Snowflake
Snowflake_node1723275340721 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1723275334108, connection_type="snowflake", connection_options={"autopushdown": "on", "dbtable": "account", "connectionName": "Snowflake connection", "preactions": "CREATE TABLE IF NOT EXISTS public.account (accountid string, name string, description string, accountheadid string, isactive string, createdby string, createddate string, updatedby string, updateddate string);", "sfDatabase": "community_finance", "sfSchema": "public"}, transformation_ctx="Snowflake_node1723275340721")

job.commit()


print("GLUE JOB DON E")


