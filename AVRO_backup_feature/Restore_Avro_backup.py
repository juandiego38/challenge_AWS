import boto3
import psycopg2
import avro.schema
import json
import io
from avro.datafile import DataFileReader
from avro.io import DatumReader
from datetime import datetime
from io import BytesIO

def extract_parameters_from_response(parameters):
    
    for parameter in parameters:
        if parameter['Name'].split('/')[-1]=='databasename':
            databasename=parameter['Value']
        elif parameter['Name'].split('/')[-1]=='user':
            user=parameter['Value']
        elif parameter['Name'].split('/')[-1]=='password':
            password=parameter['Value']
        elif parameter['Name'].split('/')[-1]=='port':
            port=parameter['Value']
        elif parameter['Name'].split('/')[-1]=='database_identifier':
            database_identifier=parameter['Value']
        elif parameter['Name'].split('/')[-1]=='bucketname':
            bucketname=parameter['Value']
    
    parameters_dict={'databasename':databasename,'user':user,'password':password,'port':port,'database_identifier':database_identifier,'bucketname':bucketname}
    return parameters_dict

def get_last_created_folder(bucket_name, parent_folder_prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=parent_folder_prefix, Delimiter='/')

    folders = response.get('CommonPrefixes', [])

    if folders:
        # Sort the folders in descending order based on the folder names
        sorted_folders = sorted(folders, key=lambda x: x['Prefix'], reverse=True)
        last_folder = sorted_folders[0]['Prefix']
        # Remove the parent folder prefix from the last folder
        last_folder = last_folder.replace(parent_folder_prefix, '', 1)
        return last_folder.rstrip('/')
    else:
        return None

def read_avro_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    avro_data = response['Body'].read()
    return avro_data

def deserialize_avro_data(avro_data, avro_schema):
    schema = avro.schema.Parse(avro_schema)
    bytes_reader = BytesIO(avro_data)
    datum_reader = DatumReader(schema)
    data_file_reader = DataFileReader(bytes_reader, datum_reader)
    records = [record for record in data_file_reader]
    data_file_reader.close()
    return records


def lambda_handler(event, context):
    
    # Create an SSM client
    ssm_client = boto3.client('ssm')
    # Specify the SSM parameter names to retrieve
    path = '/RDS/test-migration-db/'
    
    # Create an RDS client
    rds_client = boto3.client('rds')
    
    # Retrieve parameters by path from SSM Parameter Store
    response = ssm_client.get_parameters_by_path(Path=path,Recursive=True, WithDecryption=True)
    
    # Extract the parameters values from the response
    parameters_to_extract = response['Parameters']
    parameters = extract_parameters_from_response(parameters_to_extract)
    
    #Set all the required parameters for the RDS database
    #Describe the RDS instance to extract the endpoint using the database_identifier parameter stored in SSM 
    response_describe_db = rds_client.describe_db_instances(DBInstanceIdentifier=parameters['database_identifier'])
    endpoint = response_describe_db['DBInstances'][0]['Endpoint']['Address']
    port=parameters['port']
    master_username = parameters['user']
    master_password = parameters['password']
    database_name = parameters['databasename']

    # S3 bucket configuration
    s3_bucket = parameters['bucketname']

    # Connect to RDS database
    conn = psycopg2.connect(
        host=endpoint,
        database=database_name,
        user=master_username,
        password=master_password
    )
    
    # Let's create the cursor
    cursor = conn.cursor()
    
    # Get the last created folder
    parent_folder_prefix='backups_tables/'
    last_folder = get_last_created_folder(s3_bucket, parent_folder_prefix)
    
    if last_folder:
        avro_table1 = f'backups_tables/{last_folder}/hired_employees.avro'
        avro_table2 = f'backups_tables/{last_folder}/departments.avro'
        avro_table3 = f'backups_tables/{last_folder}/jobs.avro'

        # AVRO schema for tables
        hired_employees_schema = '''{
                "type": "record",
                "name": "hired_employees",
                "fields": [
                    { "name": "id", "type": "int" },
                    { "name": "name", "type": "string" },
                    { "name": "datetime", "type": "string" },
                    { "name": "department_id", "type": "int" },
                    { "name": "job_id", "type": "int" }
                ]
            }'''
            
        departments_schema = '''{
                "type": "record",
                "name": "departments",
                "fields": [
                    { "name": "id", "type": "int" },
                    { "name": "department", "type": "string" }
                ]
            }'''
            
        jobs_schema = '''{
                "type": "record",
                "name": "jobs",
                "fields": [
                    { "name": "id", "type": "int" },
                    { "name": "job", "type": "string" }
                ]
            }'''

        # Read AVRO data from S3
        avro_data1 = read_avro_from_s3(s3_bucket, avro_table1)
        avro_data2 = read_avro_from_s3(s3_bucket, avro_table2)
        avro_data3 = read_avro_from_s3(s3_bucket, avro_table3)

        # Deserialize AVRO data
        hired_employees_records = deserialize_avro_data(avro_data1, hired_employees_schema)
        departments_records = deserialize_avro_data(avro_data2, departments_schema)
        jobs_records = deserialize_avro_data(avro_data3, jobs_schema)
        
        # First we need to clean the database - to roll back into the last backup
        delete_query='DELETE FROM migration.hired_employees;\
                      DELETE FROM migration.departments;\
                      DELETE FROM migration.jobs;'
        cursor.execute(delete_query)
        
        sql_statements = []
        for row in departments_records:
            column1 = row['id']
            column2 = row['department']
            sql = f"""INSERT INTO migration.departments (id, department) VALUES ({column1}, $${column2}$$);"""
            sql_statements.append(sql)

        for sql in  sql_statements:
            cursor.execute(sql)
        
        sql_statements = []
        for row in jobs_records:
            column1 = row['id']
            column2 = row['job']
            sql = f"""INSERT INTO migration.jobs (id, job) VALUES ({column1}, $${column2}$$);"""
            sql_statements.append(sql)
            
        for sql in  sql_statements:
            cursor.execute(sql)
            
        sql_statements = []
        for row in hired_employees_records:
            column1 = row['id']
            column2 = row['name']
            column3 = row['datetime']
            column4 = row['department_id']
            column5 = row['job_id']
            sql = f"""INSERT INTO migration.hired_employees (id, name, datetime, department_id, job_id) VALUES ({column1}, $${column2}$$, $${column3}$$, {column4}, {column5});"""
            sql_statements.append(sql)
            
        for sql in  sql_statements:
            cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()
        return {
        'statusCode': 200,
        'body': 'AVRO tables backup successfully restored into RDS database'
        }
        
    else:
        return 'Error: There is not a backup created.'
