


import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def glue_type_to_snowflake_type(glue_type):
    type_mapping = {
        # Type mappings as previously defined
    }
    
    if glue_type.startswith('decimal'):
        return 'DECIMAL'
    elif glue_type.startswith('char') or glue_type.startswith('varchar'):
        return glue_type.upper()
    
    return type_mapping.get(glue_type.lower(), 'VARCHAR')

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_database = 'community-finance-database'
glue_client = boto3.client('glue')

paginator = glue_client.get_paginator('get_tables')
tables = []
for page in paginator.paginate(DatabaseName=glue_database):
    tables.extend(page['TableList'])

for table in tables:
    table_name = table['Name']
    print(f"Processing table: {table_name}")

    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=glue_database,
            table_name=table_name,
            transformation_ctx=f"dynamic_frame_{table_name}"
        )
        print(f"Successfully created dynamic frame for table: {table_name}")
    except Exception as e:
        print(f"Error creating dynamic frame for table {table_name}: {str(e)}")
        continue

    schema = dynamic_frame.schema()
    create_table_sql = f'CREATE TABLE IF NOT EXISTS public."{table_name}" ('
    create_table_sql += ", ".join([f'"{field.name}" {glue_type_to_snowflake_type(field.dataType.typeName())}' for field in schema.fields])
    create_table_sql += ");"
    
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="snowflake",
            connection_options={
                "autopushdown": "on",
                "dbtable": f'"{table_name}"',
                "connectionName": "Snowflake connection",
                "preactions": create_table_sql,
                "sfDatabase": "community_finance",
                "sfSchema": "public"
            },
            transformation_ctx=f"snowflake_{table_name}"
        )
        print(f"Successfully wrote table {table_name} to Snowflake")
    except Exception as e:
        print(f"Error writing table {table_name}: {str(e)}")
        print(f"CREATE TABLE SQL: {create_table_sql}")

job.commit()
