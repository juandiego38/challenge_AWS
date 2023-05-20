#The lambda will be triggered once the files are uploaded
import csv
import json
import tempfile
from datetime import datetime
import boto3


#This function help us to read the csv file from s3
def read_csv_from_s3(s3_bucket_name,key):
    s3 = boto3.client('s3')
    # Read the CSV file from S3
    s3_object = s3.get_object(Bucket=s3_bucket_name, Key=key)
    csv_content = s3_object['Body'].read().decode('utf-8-sig').replace('\r','').split('\n')
    return csv_content

#This function help us to check if the payload has the minimum requirements.
def validation_hired_employees(rows,batch_id):
    pass_=1
    logs=[['Batch_Id','Row','Error']]
    for row in rows:
        row=row.split(',')
    
        if len(row) == 5:
            log=[batch_id,str(row),[]]
            first_column = row[0]
            second_column = row[1]
            third_column = row[2]
            fourth_column = row[3]
            fifth_column = row[4]
            # Validate the first column as an integer
            try:
                first_column = int(first_column)
            except ValueError:
                log[2].append("Validation Error: First value should be an integer")
                pass_=0

            # Validate the second column as a string
            if not isinstance(second_column, str):
                log[2].append("Validation Error: Second value should be a string")
                pass_=0
                
            # Validate the third column as a datetime in ISO format
            try:
                datetime.strptime(third_column, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                log[2].append("Validation Error: Third value should be a datetime in ISO format")
                pass_=0
            try:
                fourth_column = int(fourth_column)
            except ValueError:
                log[2].append("Validation Error: Fourth value should be an integer")
                pass_=0
            
            try:
                fifth_column = int(fifth_column)
            except ValueError:
                log[2].append("Validation Error: Fifth value should be an integer")
                pass_=0

        else:
            log[2].append("Validation Error: Data malformed, more or less than the fields expected")
            pass_=0
        
        #We append the log for each row
        logs.append(log)
        
        
    if pass_==1:
        return pass_
    else:
        return logs
    
def validation_departments_or_jobs(rows,batch_id):
    pass_=1
    for row in rows:
        row=row.split(',')
        if len(row) == 2:
            first_column = row[0]
            second_column = row[1]

            # Validate the first column as an integer
            try:
                first_column = int(first_column)
            except ValueError:
                print("Validation Error: First value should be an integer")
                pass_=0

            # Validate the second column as a string
            if not isinstance(second_column, str):
                print("Validation Error: Second value should be a string")
                pass_=0
        else:
            print("Validation Error: Data malformed, more or less than the fields expected")
            pass_=0
            
    return pass_

def write_csv_to_s3(bucket_name, file_key, file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_key)

def transform_list_to_csv(list_of_lists):
    # Create a temporary file to store the CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    
    # Write the CSV data to the temporary file
    with open(temp_file.name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list_of_lists)

    return temp_file.name
    

def lambda_handler(event, context):
    # Extract the file name from the event
    #s3_bucket_name=event['Records'][0]['s3']['bucket']['name']
    #key = event['Records'][0]['s3']['object']['key']
    # Write the bucket name
    s3_bucket_name = 'jd-practice-bucket'
    key='data/hired_employees.csv'
    
    apigateway = boto3.client('apigateway')
    rows= read_csv_from_s3(s3_bucket_name,key)
    
    if key=='data/hired_employees.csv':
        validation=1
    else:
        validation=2
        
    # Split data into batches of 1000 rows
    batch_size = 50
    batches = [rows[i:i+batch_size] for i in range(0, len(rows), batch_size)]
    
    for batch_id, batch in enumerate(batches):
        batch_id+=1
        if validation==1:
            result=validation_hired_employees(batch,batch_id)
            if result==1:
                print(batch_id,'PASS_PAYLOAD')
            else:
                # Transform list of lists to CSV
                csv_file_path = transform_list_to_csv(result)
                # Write CSV file to S3
                timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                batch_id_str=str(batch_id)
                file_key = f"logs/errors_hired_employees_batch-{batch_id_str}_{timestamp}.csv"
                write_csv_to_s3(s3_bucket_name, file_key, csv_file_path)

                
        
    
    
 
    