import boto3

def lambda_handler(event, context):
    # Create an RDS client
    rds_client = boto3.client('rds')

    # Define the parameters for the new DB instance
    db_instance_identifier = 'test-migration-db'
    db_instance_class = 'db.t3.micro' # This is the instance for free tier
    allocated_storage = 10
    engine = 'postgres'  # postgreSQL
    master_username = '<HERE_IS_YOUR_USER>'
    master_password = '<HERE_IS_YOUR_PASSWORD>'
    database_name = '<HERE_IS_YOUR_DATABASE_NAME>'
    

    # Create the DB instance
    response = rds_client.create_db_instance(
        DBInstanceIdentifier=db_instance_identifier,
        AllocatedStorage=allocated_storage,
        Engine=engine,
        DBInstanceClass=db_instance_class,
        MasterUsername=master_username,
        MasterUserPassword=master_password,
        DBName=database_name,
        BackupRetentionPeriod=0, # This disable the automatic backups
        StorageType='gp2',  # Set the storage type (e.g., gp2 for General Purpose SSD)
        MultiAZ=False # Not multi AZ 
    )
    
    # Return the response
    return {
        'statusCode': 200,
        'body': 'DB instance created using the specified snapshot.'
    }
    

        