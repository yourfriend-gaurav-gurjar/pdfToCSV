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
import awswrangler as wr

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

def csvToCombinedcsv(dest_bucket, destination_file, source_bucket, file_path):
   # counter = counter + 1
   s3 = s3fs.S3FileSystem(anon=False)
   all_filenames = s3.ls(dest_bucket)
   with s3.open(f'{dest_bucket}/{file_path},'w') as f:
        raw_df = pd.concat([pd.read_csv(f) for f in all_filenames], join="outer")
        data=raw_df['0'].str.split(r'Trade Name:', expand=True)
        raw_df['Trade'] = data[1]
        raw_df['0'] = data[0]
        raw_df.to_csv(f, index=False, encoding='utf-8-sig')
   #        raw_df = wr.s3.read_csv(path=f, path_suffix = ['.csv'], dataset=True)
        # wr.s3.delete_objects(f"s3://{dest_bucket}/{destination_file}")
   #     raw_df.to_csv(f,index=False, encoding='utf-8-sig')

   # Calling the function with latest available files
date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
day=(datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[2]
filenames = ['growers','processor','transporter','dispensaries','laboratory','waste_disposal']
for file in filenames:
	raw_path_dir = 'US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day + '/'
	source_filename = 'US/OK/CannabisLifecycle/' + file+'_' + date + '.pdf'
    dest_filename = 'US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day + '/' + file +'_'+ date
	combained='US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day + '/' + file +'_final_'+ date+'.csv'
    pdf_to_csv(source_bucket, source_filename, dest_bucket, dest_filename)
	csvToCombinedcsv(dest_bucket, combained, dest_bucket, raw_path_dir)
    for j in all_filenames:
        if dest_filename in j:
   #         print(j)
            s3.rm(j)

   job.commit()