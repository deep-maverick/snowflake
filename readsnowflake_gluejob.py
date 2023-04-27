#read table from snowflake and write into snowflake:
-----------------------------------------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



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
        .option("dbtable",snowflake_database+"."+snowflake_schema+"."+source_table_name )\
        .option("autopushdown", "off").load()

    df1=df.groupBy(col("marital")).agg(count("*"))
    df1.write.mode("overwrite").format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "bank_count") \
        .option("header", "true").save()

main()



