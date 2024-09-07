import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1725728959639 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sgonzalg2lab1/trabajo1/RAW/dataset-trabajo1-raw.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1725728959639")

# Script generated for node SQL Query
SqlQuery5303 = '''
select
'Domain Code' as Domain_Code,
Domain,
'Area Code (FAO)' as area_code,
area,
'Element Code' as element_code,
element,
'Months Code' as month_code,
Months,
'Year Code' as year_code,
Year,
unit,
value,
flag,
'Flag Description' as Flag_Description
from myDataSource;
'''
SQLQuery_node1725729167250 = sparkSqlQuery(glueContext, query = SqlQuery5303, mapping = {"myDataSource":AmazonS3_node1725728959639}, transformation_ctx = "SQLQuery_node1725729167250")

# Script generated for node Amazon S3
AmazonS3_node1725729707913 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1725729167250, connection_type="s3", format="csv", connection_options={"path": "s3://sgonzalg2lab1/trabajo1/Trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1725729707913")

job.commit()