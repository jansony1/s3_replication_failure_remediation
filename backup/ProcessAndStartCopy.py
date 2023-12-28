import boto3
import csv
import os
import json
import time
from io import StringIO


# Initialize boto3 clients for DynamoDB and S3
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
# For batch replication
s3_control_client = boto3.client('s3control')

# Change below parameters, when invoke lambda via env overide
table_name = os.environ['table_name']
csv_bucket = os.environ['bucket_name']
src_bucket = os.environ['src_bucket']
account_id = os.environ['account_id']
replication_role =  os.environ['replication_role']
replication_rule = os.environ['replication_rule']





def one_time_batch(target_file_lists, etag):
    
    response = s3_control_client.create_job(
        AccountId=account_id,
        Operation={
            'S3ReplicateObject': {}
        },
        Report={
            'Bucket': 'arn:aws:s3:::' + csv_bucket,
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'Prefix': 'report/',
            'ReportScope': 'AllTasks'
        },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': ['Bucket', 'Key', 'VersionId']
            },
            'Location': {
                'ObjectArn': 'arn:aws:s3:::'+ csv_bucket+'/'+ target_file_lists,
                'ETag': etag
            }
        },
        Priority=10,
        ConfirmationRequired=False,
        RoleArn=replication_role
    )
    
    # The response contains the job ID
    print("Job created. ID:", response['JobId'])
    

def lambda_handler(event, context):
    # Reference to your DynamoDB table
    table = dynamodb.Table(table_name)

    # 初始化存储所有查询结果的列表
    all_items = []
    

    # Query the table
    kwargs = {
        'KeyConditionExpression': 'ReplicationRuleId = :id',
        'ExpressionAttributeValues': {':id': replication_rule},
        'ProjectionExpression': 'ReplicationRuleId, ObjectKeyVersionId'
    }
    
    while True:
        #paginate automatically up to 1MB
        try:
            response = table.query(**kwargs)
        except Exception as e:
            logger.error(f"DynamoDB query: {e}")
            raise e
        
        items = response.get('Items', [])
        print("items are: " + json.dumps(items))
        if not items:
            break
        
        all_items.extend(items)
            
        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
    
        kwargs['ExclusiveStartKey'] = last_key
            
    
    # Create a CSV file in memory
    csv_file = StringIO()
    writer = csv.writer(csv_file)
    
    
    # Write the data rows
    for item in all_items:
        object_key, version_id = item.get('ObjectKeyVersionId', '').split('#', 1)
        writer.writerow([src_bucket,object_key, version_id])
    
    # Set for duplicate data manipulation
    unique_rows = set()

    # Data preparation
    for item in all_items:
        object_key, version_id = item.get('ObjectKeyVersionId', '').split('#', 1)
        unique_rows.add((src_bucket, object_key, version_id))
    
    # Create a CSV file in memory
    csv_file = StringIO()
    writer = csv.writer(csv_file)
    
    # Ingest data to CSV
    for row in unique_rows:
        writer.writerow(row)
    
    # Reset the file pointer to the beginning
    csv_file.seek(0)  
    
    # Define the S3 bucket and the file name where the CSV will be stored
    s3_file_key = replication_rule + ".csv"
    
    # Upload the CSV file to S3
    s3_response = s3.put_object(Bucket=csv_bucket, Key=s3_file_key, Body=csv_file.getvalue())
    etag = s3_response.get('ETag')
    print(json.dumps(f'CSV file created and uploaded to S3 bucket {csv_bucket} with key {s3_file_key}'))

     # Close the StringIO object
    csv_file.close()
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'finished ontime batch generation for replication id {replication_rule}')
   