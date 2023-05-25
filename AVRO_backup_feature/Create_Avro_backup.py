import boto3
import psycopg2
import avro.schema
import json
import io
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
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


def serialize_avro_data(schema, data):
    avro_schema = avro.schema.Parse(schema)
    bytes_writer = BytesIO()
    datum_writer = DatumWriter(avro_schema)

    data_file_writer = DataFileWriter(bytes_writer, datum_writer, avro_schema)
    for record in data:
        data_file_writer.append(record)

    data_file_writer.flush()

    serialized_data = bytes_writer.getvalue()

    return serialized_data
    
def lambda_handler(event, context):
    timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Create S3 Client
    s3_client = boto3.client('s3')
    key=f'backups_tables/avro_tables_backup_{timestamp}/'
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

    # Serialize data to AVRO format
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
        
    # Fetch data from table 1
    cursor = conn.cursor()
    cursor.execute("SELECT he.id,he.name,he.datetime,he.department_id,he.job_id FROM migration.hired_employees he")
    hired_employees_data = cursor.fetchall()
    hired_employees_data = [dict(zip([col[0] for col in cursor.description], row)) for row in hired_employees_data]


    # Fetch data from table 2
    cursor.execute("SELECT d.id,d.department FROM migration.departments d")
    departments_data = cursor.fetchall()
    departments_data = [dict(zip([col[0] for col in cursor.description], row)) for row in departments_data]


    # Fetch data from table 3
    cursor.execute("SELECT j.id,j.job FROM migration.jobs j")
    jobs_data = cursor.fetchall()
    jobs_data = [dict(zip([col[0] for col in cursor.description], row)) for row in jobs_data]


    # Close the database connection
    conn.close()

    # Serialize data into AVRO format
    serialized_table2 = serialize_avro_data(departments_schema, departments_data)
    serialized_table1 = serialize_avro_data(hired_employees_schema, hired_employees_data)
    serialized_table3 = serialize_avro_data(jobs_schema, jobs_data)

    #Write AVRO files to S3
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=key + 'hired_employees.avro',
        Body=serialized_table1
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=key + 'departments.avro',
        Body=serialized_table2
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=key + 'jobs.avro',
        Body=serialized_table3
    )

    return {
        'statusCode': 200,
        'body': 'AVRO tables backup successfully written to S3'
    }