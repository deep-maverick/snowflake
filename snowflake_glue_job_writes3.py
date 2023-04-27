#snowflake_glue_Job write into s3
----------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#write read data from snowflake and write data into s3

def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "SFDB"
    snowflake_schema = "SPARK_SCHEMA"
    snowflake_warehouse="SMALLWH"
    source_table_name = "BANKTAB"
    snowflake_options={
    "sfURL": "fbqwibi-mq90187.snowflakecomputing.com",
    "sfUser": "ph470",
    "sfPassword": "Snowflake19",
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": snowflake_warehouse
    }
    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("query","select marital ,count(*) as people_count from banktab group by marital order by people_count" )\
        .option("autopushdown", "off").load()

    df.coalesce(1).write.option("mode","overwrite").option("header","true").csv("s3://ph470/output/snow.csv")

main()
