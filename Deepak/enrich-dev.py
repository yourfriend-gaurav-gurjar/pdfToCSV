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
source_bucket = 'cannaeyeglass-datalake-processed-dev'
dest_bucket = 'cannaeyeglass-datalake-enriched-dev'
#error_bucket = 'cannaeyeglass-datalake-errors'

def remove_newline(x):
    try:
        x = x.str.replace('\n','').replace(' \n','').replace('\n ','')
    except:
        pass
    return x

def csvToCombinedcsv(dest_bucket, destination_file, source_bucket, file_path,license_type):
    s3 = s3fs.S3FileSystem(anon=False)
    raw_df = wr.s3.read_csv(path=f's3://{source_bucket}/{file_path}/', path_suffix = ['.csv'], dataset=True)
    raw_df =raw_df.drop(['Unnamed: 0'],axis=1)
    raw_df=raw_df[raw_df['0'].notnull()]
    enrich_df= raw_df.iloc[:,:7]
    enrich_df.columns=['Name','Lincence_number','Email','Phone','City','Zip','County']
    enrich_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True )
    #enrich_df.apply(remove_newline)
    enrich_df['license_type']=license_type
    with s3.open(f's3://{dest_bucket}/{destination_file}/','w') as f:
        enrich_df.to_csv(f)


processed_bucket = 'cannaeyeglass-datalake-enriched-dev'
enriched_bucket = 'cannaeyeglass-datalake-enriched-dev'
def enrichment(enriched_bucket, destination_file, processed_bucket, file_path, license_type):
    s3 = s3fs.S3FileSystem(anon=False)
    processed_df = wr.s3.read_csv(path=f's3://{enriched_bucket}/{file_path}', path_suffix=['.csv'], dataset=True)
    processed_df = processed_df.drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
    processed_df = processed_df[processed_df['0'].notnull()]
    if license_type == "dispensaries" or license_type== "growers" or license_type =="transporter" or license_type=="processor":
        df = processed_df
        df_7thColIndex = df['7'].notnull()
        missing_data = df[df_7thColIndex]
        missing_data.drop('1', inplace=True, axis=1)
        missing_data.drop('2', inplace=True, axis=1)
        missing_data.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
        missing_data = missing_data.replace('\n', '', regex=True)
        missing_data['zip'] = missing_data['zip'].astype("int64")
        data = missing_data['business'].str.split(r'Trade Name:', expand=True)
        missing_data['trade_name'] = data[1]
        missing_data['business'] = data[0]
        index_names_df = df[df['7'].notnull()].index
        df.drop(index_names_df, inplace=True)
        df.drop('7', inplace=True, axis=1)
        df.drop('8', inplace=True, axis=1)
        df.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', license_type]
        df = df.replace('\n', '', regex=True)
        df['zip'] = df['zip'].astype("int64")
        data = df['business'].str.split(r'Trade Name:', expand=True)
        df['trade_name'] = data[1]
        df['business'] = data[0]
        frame = [df, missing_data]
        df = pd.concat(frame)

    if license_type == "laboratory":
        df2 = processed_df
        df2.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', license_type]
        df2 = df2.replace('\n', '', regex=True)
        df2['zip'] = df2['zip'].astype("int64")
        data = df2['business'].str.split(r'Trade Name:', expand=True)
        df2['trade_name'] = data[1]
        df2['business'] = data[0]

    if license_type == "waste_disposal":
        df3 = processed_df
        df3 = df3[df3['0'].notnull()]
        df3.drop('1', inplace=True, axis=1)
        df3.drop('2', inplace=True, axis=1)
        df3.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', license_type]
        df3 = df3.replace('\n', '', regex=True)
        df3['zip'] = df3['zip'].astype("int64")
        df3['phone'] = df3['phone'].astype("int64")
        data = df3['business'].str.split(r'Trade Name:', expand=True)
        # print(data)
        df3['trade_name'] = data[1]
        df3['business'] = data[0]

    master_frame = [df,df2,df3]
    enrich_df = pd.concat(master_frame)
    with s3.open(f's3://{processed_bucket}/{destination_file}/','w') as f:
        enrich_df.to_csv(f)

# Calling the function with latest available files
date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
day=(datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[2]
filenames = ['growers','processor','transporter','dispensaries','laboratory','waste_disposal']
#filenames = ['waste_disposal']

for file in filenames:
    raw_path_dir = 'US/OK/CannabisLifecycle/' + file + '/'  + year + '/' + month + '/' + day
    dest_filename = 'US/OK/LicenceInfo/' + year + '/' + month + '/' + day + '/' + file +'_'+ date
    combained='US/OK/LicenseInfo/' + year + '/' + month + '/' + day + '/' + file + date+'.csv'
    csvToCombinedcsv(dest_bucket, combained, source_bucket, raw_path_dir,file)
    enrich_path_dir = 'US/OK/LicenseInfo' + year + '/' + month + '/' + day +'/'+ file +'_'+ date +'.csv'
    dest_combinedfilename = 'US/OK/LicenceInfo/' + year + '/' + month + '/' + day + '/' + date
    combined = 'US/OK/LicenseInfo/' + year + '/' + month + '/' + day + '/' + date + '.csv'
    enrichment(dest_bucket, combined, enriched_bucket, enrich_path_dir, file)

job.commit()