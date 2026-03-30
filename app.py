import boto3
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os

# Environment variables
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY")

RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DB = os.getenv("RDS_DB")
RDS_TABLE = os.getenv("RDS_TABLE")

GLUE_DB = os.getenv("GLUE_DB")
GLUE_TABLE = os.getenv("GLUE_TABLE")
S3_OUTPUT = f"s3://{S3_BUCKET}/"

def read_from_s3():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj['Body'])
    print("✅ Data read from S3")
    return df

def write_to_rds(df):
    try:
        engine = create_engine(
            f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}/{RDS_DB}"
        )
        df.to_sql(RDS_TABLE, con=engine, if_exists='replace', index=False)
        print("✅ Data written to RDS")
    except Exception as e:
        print("❌ RDS Failed:", e)
        raise

def fallback_to_glue():
    glue = boto3.client('glue')

    try:
        glue.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                'Name': GLUE_TABLE,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'int'},
                        {'Name': 'name', 'Type': 'string'},
                        {'Name': 'age', 'Type': 'int'}
                    ],
                    'Location': S3_OUTPUT,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print("✅ Glue table created")
    except Exception as e:
        print("❌ Glue Failed:", e)

def main():
    df = read_from_s3()
    try:
        write_to_rds(df)
    except:
        fallback_to_glue()

if __name__ == "__main__":
    main()
