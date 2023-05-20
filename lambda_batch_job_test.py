#The lambda will be triggered once the files are uploaded
import csv
import json
import boto3
import tempfile
from datetime import datetime


#This function help us to read the csv file from s3
def read_csv_from_s3(s3_bucket_name,key):
    s3 = boto3.client('s3')
    # Read the CSV file from S3
    s3_object = s3.get_object(Bucket=s3_bucket_name, Key=key)
    rows = s3_object['Body'].read().decode('utf-8-sig').replace('\r','').split('\n')
    csv_content=[row.split(',') for row in rows]
    return csv_content

def transform_list_to_csv(list_of_lists):
    # Create a temporary file to store the CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    
    # Write the CSV data to the temporary file
    with open(temp_file.name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list_of_lists)
    return temp_file.name
    
def write_csv_to_s3(bucket_name, file_key, file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_key)
    
def lambda_handler(event, context):
    # Extract the file name from the event
    s3_bucket_name=event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    # Write the bucket name
    #s3_bucket_name = 'jd-practice-bucket'
    #key='data/hired_employees.csv'

    apigateway_client = boto3.client('apigateway')
    # API Gateway information
    rest_api_id = 'dv6rqvmho7'
    resource_id = 'hfbug9'
    
    #Read the data
    rows=read_csv_from_s3(s3_bucket_name,key)
    
    #Let's create the batches
    batch_size = 50
    batches = [[s3_bucket_name,key,rows[i:i+batch_size]] for i in range(0, len(rows), batch_size)]
    
    total_responses=[]
    # Write batches to API Gateway
    for batch_id,batch in enumerate(batches):
        batch_id+=1
        batch.append(batch_id)
        # Invoke the REST API Gateway
        response = apigateway_client.test_invoke_method(
            restApiId=rest_api_id,
            resourceId=resource_id,
            httpMethod='POST',
            body=json.dumps(batch)
        )
        total_responses.append(['Batch_Id:'+str(batch_id)+',Batch_Size:'+str(batch_size)+',status:'+str(response['status'])+',body:'+response['body']])
       
    
    #write migration log into s3  
    csv_file_path = transform_list_to_csv(total_responses)
    timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    #Extract table name
    start_char = "/"
    end_char = "."
    table_name = key[key.index(start_char) + 1 : key.index(end_char)]
    file_key=f"migration_log/log_migration_history_table_{table_name}_{timestamp}.csv"
    write_csv_to_s3(s3_bucket_name, file_key, csv_file_path)
    
    return total_responses
