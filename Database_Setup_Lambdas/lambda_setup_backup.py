import psycopg2
import json
import boto3

def lambda_handler(event, context):
    
    host='test-migration-db.cyyv6lswmayt.us-east-1.rds.amazonaws.com'
    port=5432
    master_username = 'migrationuser'
    master_password = 'mytestpassword'
    database_name = 'migrationdb'


    # Connect to the RDS instance
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database_name,
        user=master_username,
        password=master_password
    )

    cursor = conn.cursor()
    
    #CREATE TABLES 
    query_create_schema = "   CREATE SCHEMA migration;\
                                \
                              CREATE TABLE migration.jobs (id integer PRIMARY KEY,\
                                 job varchar);\
                                \
                              CREATE TABLE migration.departments (id integer PRIMARY KEY, \
                                                        department varchar);\
                                \
                              CREATE TABLE migration.hired_employees (id integer PRIMARY KEY, \
                                                            name varchar, \
                                                            datetime varchar, \
                                                            department_id integer, \
                                                            job_id integer,\
                                                            FOREIGN KEY (department_id) REFERENCES departments(id),\
                                                            FOREIGN KEY (job_id) REFERENCES jobs(id));   "
    

    try:
        cursor.execute(query_create_schema)
        cursor.close()
        conn.commit()
        return (f'The database and the schema were created')
    except:
        print("Error: The schema and tables could not be created")