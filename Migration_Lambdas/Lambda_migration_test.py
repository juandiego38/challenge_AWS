import csv
import json
import tempfile
from datetime import datetime
import boto3
import psycopg2


#This function help us to read the csv file from s3
def read_csv_from_s3(s3_bucket_name,key):
    # Create an S3 client
    s3 = boto3.client('s3')
    
    # Read the CSV file from S3
    s3_object = s3.get_object(Bucket=s3_bucket_name, Key=key)
    csv_content = s3_object['Body'].read().decode('utf-8-sig').replace('\r','').split('\n')
    return csv_content

#This function help us to check if the payload has the minimum requirements.
def validation_hired_employees(rows,batch_id):
    pass_=1
    logs=[['Batch_Id','Row','Errors']]
    for row in rows:
        
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
            if not isinstance(second_column, str) or second_column=='':
                log[2].append("Validation Error: Second value should be a string and must not be empty")
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
    logs=[['Batch_Id','Row','Errors']]
    for row in rows:

        if len(row) == 2:
            first_column = row[0]
            second_column = row[1]
            log=[batch_id,str(row),[]]
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
        else:
            log[2].append("Validation Error: Data malformed, more or less than the fields expected")
            pass_=0
            
        #We append the log for each row
        logs.append(log)
            
    if pass_==1:
        return pass_
    else:
        return logs
        
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
    
    parameters_dict={'databasename':databasename,'user':user,'password':password,'port':port,'database_identifier':database_identifier}
    return parameters_dict
    
    
def lambda_handler(event, context):
    
    # Create an RDS client
    rds_client = boto3.client('rds')
    # Create an SSM client
    ssm_client = boto3.client('ssm')
    # Specify the SSM parameter names to retrieve
    path = '/RDS/test-migration-db/'
    
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

    
    # Connect to the RDS instance
    conn = psycopg2.connect(
        host=endpoint,
        port=port,
        database=database_name,
        user=master_username,
        password=master_password
    )
    # Let's create the cursor
    cursor = conn.cursor()

    
    # Extract the headers from the event
    s3_bucket_name=event[0]
    key=event[1]
    batch = event[2]
    batch_id=event[3]
    start_char = "/"
    end_char = "."
    table_name = key[key.index(start_char) + 1 : key.index(end_char)]
    
    
    if table_name=='hired_employees':
        validation=1
    elif table_name=='departments':
        validation=2
        column='department'
    elif table_name=='jobs':
        validation=3
        column='job'
    else:
        validation=0
        

    if validation != 0:
    
        
        if validation==1:
            result=validation_hired_employees(batch,batch_id)
            if result==1:
                
                # Prepare the INSERT statement with ON CONFLICT DO NOTHING
                # This will allow me to don't have any error when migration happens
                insert_query =  """
                                    INSERT INTO migration.hired_employees (id, name, datetime, department_id, job_id)
                                    SELECT cast(q.id as INT) as id, q.name, q.datetime, cast(q.department_id as INT) as department_id, cast(q.job_id as INT) as job_id FROM (
                                      VALUES %s
                                    ) AS q (id, name, datetime, department_id, job_id)
                                    LEFT JOIN migration.departments d ON d.id = cast(q.department_id as INT)
                                    LEFT JOIN migration.jobs j ON j.id = cast(q.job_id as INT)
                                    WHERE d.id IS NOT NULL and j.id IS NOT NULL
                                    ON CONFLICT (id) DO NOTHING;
                                """
                
                # SINGLE INSERT
                # Convert list of lists to values inside the query
                values = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", row).decode() for row in batch)
                # Format the insert statement with the values
                formatted_statement = insert_query % values
                cursor.execute(formatted_statement)
                # Get the number of rows affected
                status_message = cursor.statusmessage
                affected_rows = int(status_message.split(" ")[-1])
                
                # MULTIPLE INSERTS
                #cursor.executemany(formatted_statement, batch)
                
                # Execute the single insert statement
                # As we know that we have a maximum batch size of 1000, we can execute each query in a single query
                # If the batch size increases, we could use executemany function, creating batches inside each batch to perform smaller queries
                cursor.close()
                conn.commit()
                conn.close()
                return [batch_id,'Pass_Payload',f"Rows affected: {affected_rows}"]
  
            else:
                # Transform list of lists to CSV
                csv_file_path = transform_list_to_csv(result)
                # Write CSV file to S3
                timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                batch_id_str=str(batch_id)
                file_key = f"logs/errors_table_{table_name}_batch-{batch_id_str}_{timestamp}.csv"
                write_csv_to_s3(s3_bucket_name, file_key, csv_file_path)
                return [batch_id,'Failed_Payload']
        else:
            result=validation_departments_or_jobs(batch,batch_id)
            if result==1:
                
                insert_query =  """
                                    INSERT INTO migration.{} (id, {})
                                    SELECT cast(q.id as INT) as id, q.name FROM (
                                      VALUES %s
                                    ) AS q (id,name)
                                    ON CONFLICT (id) DO NOTHING;
                                """.format(table_name,column)
                                
                # Convert list of lists to values inside the query
                values = ','.join(cursor.mogrify("(%s,%s)", row).decode() for row in batch)
                # Format the insert statement with the values
                formatted_statement = insert_query % values
                cursor.execute(formatted_statement)
                # Get the number of rows affected
                status_message = cursor.statusmessage
                affected_rows = int(status_message.split(" ")[-1])
                
                cursor.close()
                conn.commit()
                conn.close()
                return [batch_id,'Pass_Payload',f"Rows affected: {affected_rows} on table {table_name}"]
            else:
                # Transform list of lists to CSV
                csv_file_path = transform_list_to_csv(result)
                # Write CSV file to S3
                timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                batch_id_str=str(batch_id)
                file_key = f"logs/errors_table_{table_name}_batch-{batch_id_str}_{timestamp}.csv"
                write_csv_to_s3(s3_bucket_name, file_key, csv_file_path)
                return [batch_id,'Failed_Payload']
    else:
        return [batch_id,'Failed_Payload - File name not matching']
