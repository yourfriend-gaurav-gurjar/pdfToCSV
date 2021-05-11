import sys
from itertools import cycle
import pdfplumber as pp
import boto3
import pandas as pd
import s3fs
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from datetime import datetime, timedelta
import awswranger as wr

# Initialize job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Assign bucket names.
source_bucket = 'cannaeyeglass-datalake-raw'
dest_bucket = 'cannaeyeglass-datalake-processed'
#error_bucket = 'cannaeyeglass-datalake-errors'


# method To convert to csv
def pdf_to_csv(source_bucket, source_key, destination_bucket, destination_key):
    s3 = s3fs.S3FileSystem(anon=False)
    pdfFileObj = s3.open(f'{source_bucket}/{source_key}')
    pdf = pp.open(pdfFileObj)
    pages = pdf.pages
    counter = 0
    for i in pages:
        table = i.extract_table()
        df = pd.DataFrame(table[0:])
        counter=counter+1
        with s3.open(f'{destination_bucket}/{destination_key}_'+str(counter)+'.csv', 'w') as f:
            df.to_csv(f)
    pdfFileObj.close()

# Calling the function with latest available files
date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
day=(datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[2]
filenames = ['growers','processor','transporter','dispensaries','laboratory','waste_disposal']
for file in filenames:
    source_filename = 'US/OK/CannabisLifecycle/' + file+'_' + date + '.pdf'
    dest_filename = 'US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day + '/' + file +'_'+ date
    pdf_to_csv(source_bucket, source_filename, dest_bucket, dest_filename)


#counter = 0
raw_s3_bucket = "cannaeyeglass-datalake-processed"
dest_bucket = 'cannaeyeglass-datalake-processed'
raw_path_dir = "US/OK/CannabisLifecycle"
raw_path = f"s3://{raw_s3_bucket}/{raw_path_dir}"


def csvToCombinedcsv(raw_s3_bucket, source_key, destination_bucket, destination_key):
   # counter = counter + 1
   s3 = s3fs.S3FileSystem(anon=False)
   csvFileObj = s3.open(f'{raw_s3_bucket}/{source_key}')
   with s3.open(f'{destination_bucket}/{destination_key}', 'w') as f:
        raw_df = wr.s3.read_csv(path=f, path_suffix = ['.csv'], dataset=True)
        # wr.s3.delete_objects(f"s3://{raw_s3_bucket}/")
        raw_df.to_csv(f)


# Calling the function with latest available files
date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
day=(datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[2]
filenames = ['growers','processor','transporter','dispensaries','laboratory','waste_disposal']
for file in filenames:
#    source_filename = 'US/OK/CannabisLifecycle/' + file+'_' + date + '.csv'
    source_filename = 'cannaeyeglass-datalake-processed/US/OK/CannabisLifecycle/' + file + year + '/' + month + '/' + day + '/' + file +'_'+ date + '*.csv'
    dest_filename = 'US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day + '/' + file
    csvToCombinedcsv(raw_s3_bucket, source_filename, dest_bucket, dest_filename)


job.commit()