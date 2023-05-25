import boto3

def lambda_handler(event, context):
    # Create an RDS client
    rds_client = boto3.client('rds')

    # Define the parameters for the new DB instance
    db_instance_identifier = '<HERE_IS_YOUR_DB_INSTANCE_IDENTIFIER>'
    db_snapshot_identifier = '<HERE_IS_YOUR_DB_SNAPSHOT_IDENTIFIER>'
    db_instance_class = 'db.t3.micro' # This is the instance for free tier
    engine = 'postgres'  # postgreSQL
    

    # Create the DB instance
    response = rds_client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=db_instance_identifier,
        DBSnapshotIdentifier = db_snapshot_identifier,
        Engine=engine,
        DBInstanceClass=db_instance_class
     )
    
    # Return the response
    return {
        'statusCode': 200,
        'body': 'DB instance created using the specified snapshot.'
    }