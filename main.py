import sys
from itertools import cycle
import PyPDF2
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
error_bucket = 'cannaeyeglass-datalake-errors'


# Method to read pdf file and convert it to csv.
def pdf_to_csv(source_bucket, source_key, destination_bucket, destination_key):
    s3 = s3fs.S3FileSystem(anon=False)
    pdfFileObj = s3.open(f'{source_bucket}/{source_key}')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pages = pdfReader.numPages

    df1 = pd.DataFrame(pd.np.empty((0, 2)))
    for i in range(pages):
        pageObj = pdfReader.getPage(i)
        text = pageObj.extractText().replace("\n LLC", 'LLC').replace("\nLLC", 'LLC').replace('\n-\n', '-').replace(
            '\n \n', '\c').replace('\n', '').split('\c')

        df = pd.DataFrame(text)
        df.columns = ["data"]
        row_ignore = df[df['data'] == 'COUNTY'].index[0]
        df = df.iloc[row_ignore + 1:]
        colName = cycle(['Name', 'trade name', 'license', 'email', 'phone', 'city', 'zip', 'county'])
        df['colname'] = [next(colName) for col in range(len(df))]
        df1 = df1.append(df)

    df1 = df1[(df1['data'] != '')]

    df2 = pd.DataFrame(df1)
    df2['row_num'] = df2.groupby(['colname']).cumcount() + 1
    df3 = df2.pivot(index='row_num', columns='colname', values='data')
    format_data = pd.DataFrame(df3)

    with s3.open(f'{destination_bucket}/{destination_key}', 'w') as f:
        format_data.to_csv(f)

    pdfFileObj.close()


#date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')

#year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
#month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
date='20210427'
year='2021'
month='04'

filenames = ['Growers_']

for file in filenames:
    source_filename = 'US/OK/CannabisLifecycle/' + file + date + '.pdf'
    dest_filename = 'US/OK/CannabisLifecycle/' + year + '/' + month + '/' + file + date + '.csv'
    pdf_to_csv(source_bucket, source_filename, dest_bucket, dest_filename)

job.commit()
