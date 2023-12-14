import os
import re
import time
import json
import random
import boto3


dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
iam_client = boto3.client('iam')
osis_client = boto3.client('osis')
aoss_client = boto3.client('opensearchserverless')
ec2_client = boto3.client('ec2')
s3_client = boto3.client('s3')

with open('dynamodb-data/table-data.json', 'r') as f:
    table_data = json.load(f)

with open('dynamodb-data/table-attributes.json', 'r') as f:
    table_attributes = json.load(f)
    

def on_event(event, context):
    table_name = os.environ.get('TABLE_NAME')
    pipeline_name = os.environ.get('PIPELINE_NAME')
    pipeline_role_name = os.environ.get('PIPELINE_ROLE_NAME')
    region = os.environ.get('REGION')
    account_id = os.environ.get('ACCOUNT_ID')
    network_policy_name = os.environ.get('NETWORK_POLICY_NAME')
    bucket_name = os.environ.get('BUCKET_NAME')
    bucket_arn = os.environ.get('BUCKET_ARN')
    collection_arn = os.environ.get('COLLECTION_ARN')
    collection_name = os.environ.get('COLLECTION_NAME')
    collection_endpoint = os.environ.get('COLLECTION_ENDPOINT')
    log_group_name = os.environ.get('LOG_GROUP_NAME')
    vpc_id = os.environ.get('VPC_ID')
    vpc_endpoint_name = os.environ.get('VPC_ENDPOINT_NAME')
    security_group_ids = [os.environ.get('SECURITY_GROUP_IDS')]
    subnet_ids_pipeline = json.loads(os.environ.get('SUBNET_IDS_ISOLATED'))

    print(json.dumps(event))
    request_type = event['RequestType']
    if request_type == 'Create':
        return on_create(event=event,
                         table_name=table_name,
                         pipeline_name=pipeline_name,
                         pipeline_role_name=pipeline_role_name,
                         region=region,
                         bucket_arn=bucket_arn,
                         collection_name=collection_name,
                         network_policy_name=network_policy_name,
                         bucket_name=bucket_name,
                         collection_arn=collection_arn,
                         collection_endpoint=collection_endpoint,
                         log_group_name=log_group_name,
                         vpc_id=vpc_id,
                         vpc_endpoint_name=vpc_endpoint_name,
                         security_group_ids=security_group_ids,
                         subnet_ids_pipeline=subnet_ids_pipeline)
    if request_type == 'Update':
        return on_update(event=event)
    if request_type == 'Delete':
        return on_delete(table_name=table_name,
                         bucket_name=bucket_name,
                         account_id=account_id,
                         pipeline_name=pipeline_name,
                         pipeline_role_name=pipeline_role_name,
                         vpc_endpoint_name=vpc_endpoint_name)
    raise Exception("Invalid request type: %s" % request_type)


def on_create(event, table_name, pipeline_role_name, region,
              network_policy_name, bucket_name, collection_arn,
              collection_name, bucket_arn, collection_endpoint,
              log_group_name, pipeline_name, vpc_id, vpc_endpoint_name,
              security_group_ids, subnet_ids_pipeline):
    props = event["ResourceProperties"]
    print("create new resource with props %s" % props)

    table_arn = create_and_populate_table(table_name=table_name)

    vpc_endpoint_id = create_vpc_endpoint(vpc_id=vpc_id,
                                          vpc_endpoint_name=vpc_endpoint_name,
                                          security_group_ids=security_group_ids,
                                          subnet_ids_pipeline=subnet_ids_pipeline)

    update_network_policy(network_policy_name=network_policy_name,
                          vpc_endpoint_id=vpc_endpoint_id,
                          collection_name=collection_name)

    pipeline_role_arn = modify_pipeline_role(pipeline_role_name=pipeline_role_name,
                                             collection_arn=collection_arn,
                                             table_arn=table_arn,
                                             bucket_arn=bucket_arn,
                                             collection_name=collection_name)

    create_opensearch_ingestion_pipeline(table_arn=table_arn,
                                         bucket_name=bucket_name,
                                         pipeline_name=pipeline_name,
                                         collection_endpoint=collection_endpoint,
                                         network_policy_name=network_policy_name,
                                         pipeline_role_arn=pipeline_role_arn,
                                         log_group_name=log_group_name,
                                         subnet_ids_pipeline=subnet_ids_pipeline, 
                                         security_group_ids=security_group_ids,
                                         region=region)

    return


def on_update(event):
    props = event["ResourceProperties"]
    print("update resource %s with props %s" % (props))
    return


def on_delete(table_name, pipeline_name, account_id,
              pipeline_role_name, bucket_name, vpc_endpoint_name):
    print("delete resource")
    delete_dynamo_table(table_name=table_name)
    delete_opensearch_ingestion_pipeline(pipeline_name=pipeline_name)
    delete_role_and_policies(pipeline_role_name=pipeline_role_name, account_id=account_id)
    empty_and_delete_bucket(bucket_name=bucket_name)
    delete_vpc_endpoint(vpc_endpoint_name=vpc_endpoint_name)
    return


def create_and_populate_table(table_name):

    table_kwargs = table_attributes
    table_kwargs['TableName'] = table_name

    table_arn = dynamodb_client.create_table(
        **table_kwargs)['TableDescription']['TableArn']

    # Pause to make sure the table is created
    time.sleep(45)

    # Loop through each item and convert to the required format
    # and write to the table in batches of 25
    request_items = {table_name: []}
    for i in range(0, len(table_data), 25):
        batch = []
        for item in table_data[i:i+25]:
            if "__id" in item and not item["__id"]:
                del item["__id"]
            batch.append({"PutRequest": {"Item": item}})

        request_items[table_name].extend(batch)
        dynamodb_client.batch_write_item(RequestItems=request_items)
        request_items[table_name] = []

    dynamodb_client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={
            'PointInTimeRecoveryEnabled': True
    })

    return table_arn


def modify_pipeline_role(pipeline_role_name, collection_arn, table_arn, 
                         bucket_arn, collection_name):

    role = iam_client.get_role(RoleName=pipeline_role_name)

    ingestion_pipeline_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["aoss:*"],
            "Resource": \"""" + collection_arn + """\"
        },
        {
            "Effect": "Allow",
            "Action": [
                "aoss:CreateSecurityPolicy",
                "aoss:GetSecurityPolicy",
                "aoss:UpdateSecurityPolicy"
            ],
            "Condition": {
                "StringEquals": {
                    "aoss:collection": \"""" + collection_name + """\"
                }
            },
            "Resource": "*"
        }
    ]
   }"""

    dynamodb_ingestion_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "allowRunExportJob",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable",
                "dynamodb:DescribeContinuousBackups",
                "dynamodb:ExportTableToPointInTime"
            ],
            "Resource": \"""" + table_arn + """\"
        },
        {
            "Sid": "allowCheckExportjob",
            "Effect": "Allow",
            "Action": ["dynamodb:DescribeExport"],
            "Resource": \"""" + table_arn + """/export/*" 
        },
        {
            "Sid": "allowReadFromStream",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator"
            ],
            "Resource": \"""" + table_arn + """/stream/*"
        },
        {
            "Sid": "allowReadAndWriteToS3ForExport",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:AbortMultipartUpload",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": \"""" + bucket_arn + """/*" 
        }
    ]
   }"""

    ingestion_pipeline_policy_arn = iam_client.create_policy(
        PolicyName='IngestionPipelinePolicy',
        PolicyDocument=ingestion_pipeline_policy)['Policy']['Arn']

    dynamodb_ingestion_policy_arn = iam_client.create_policy(
        PolicyName='DynamoDBIngestionPolicy',
        PolicyDocument=dynamodb_ingestion_policy)['Policy']['Arn']

    iam_client.attach_role_policy(
        RoleName=role['Role']['RoleName'],
        PolicyArn=ingestion_pipeline_policy_arn
    )

    iam_client.attach_role_policy(
        RoleName=role['Role']['RoleName'],
        PolicyArn=dynamodb_ingestion_policy_arn
    )

    # Pause to make sure the policies are attached 
    # and CloudWatch log group is created
    time.sleep(10)

    return role['Role']['Arn']


def create_vpc_endpoint(vpc_id, vpc_endpoint_name,
                        security_group_ids, subnet_ids_pipeline):
    
    args = {
        'name': vpc_endpoint_name,
        'securityGroupIds': security_group_ids,
        'subnetIds': subnet_ids_pipeline,
        'vpcId': vpc_id
    }

    return aoss_client.create_vpc_endpoint(**args)['createVpcEndpointDetail']['id']


def update_network_policy(network_policy_name, vpc_endpoint_id, collection_name):

    policy_version = aoss_client.get_security_policy(
        name=network_policy_name,
        type='network'
    )['securityPolicyDetail']['policyVersion']

    updated_policy = f"""
[
  {{
    "Description":"VPC Only Access to OpenSearch APIs (Create by AWS CDK)",
    "Rules": [
      {{
        "ResourceType": "collection", 
        "Resource": ["collection/{collection_name}"]
      }}
    ],
    "AllowFromPublic": false,
    "SourceVPCEs": [
        "{vpc_endpoint_id}"
    ]
  }}, 
  {{
    "Description":"Public Access to the Dashboard (Created by AWS CDK)",
      "Rules":[ 
      {{  
        "ResourceType": "dashboard",
        "Resource": ["collection/{collection_name}"]  
      }}
    ],
    "AllowFromPublic": true
  }} 
]
"""
    args = {
        'description': f'Network policy for {collection_name} collection.',
        'name': network_policy_name,
        'policy': updated_policy,
        'policyVersion': policy_version,
        'type': 'network'
    }

    return aoss_client.update_security_policy(**args)


def create_opensearch_ingestion_pipeline(table_arn, bucket_name, collection_endpoint,
                                         network_policy_name, pipeline_role_arn, region,
                                         log_group_name, pipeline_name,
                                         subnet_ids_pipeline, security_group_ids):

    body_config = f"""\
version: "2"
dynamodb-pipeline:
  source:
    dynamodb:
      tables:
      - table_arn: "{table_arn}"
        stream:
          start_position: "LATEST"
        export:
          s3_bucket: "{bucket_name}"
          s3_region: "{region}" 
          s3_prefix: "export/"
      aws:
        sts_role_arn: "{pipeline_role_arn}"
        region: "{region}"
  route:
    - fare: '/type == "fare"'
    - flight: '/type == "flight"'
  sink: 
    - opensearch:
        hosts:
          - "{collection_endpoint}"
        index: "fare"
        routes:
          - "fare"
        document_id: "${{getMetadata(\\"primary_key\\")}}"
        action: "${{getMetadata(\\"opensearch_action\\")}}"
        aws:
          sts_role_arn: "{pipeline_role_arn}"
          region: "{region}"
          serverless: true
          serverless_options:
            network_policy_name: "{network_policy_name}"
        dlq:
          s3:
            bucket: "{bucket_name}"
            key_path_prefix: "dlq/fare"
            region: "{region}"
            sts_role_arn: "{pipeline_role_arn}"
    - opensearch:
        hosts:
          - "{collection_endpoint}"
        index: "flight"
        routes:
          - "flight"
        document_id: "${{getMetadata(\\"primary_key\\")}}"
        action: "${{getMetadata(\\"opensearch_action\\")}}"
        aws:
          sts_role_arn: "{pipeline_role_arn}"
          region: "{region}"
          serverless: true
          serverless_options:
            network_policy_name: "{network_policy_name}"
        dlq:
          s3:
            bucket: "{bucket_name}"
            key_path_prefix: "dlq/flight"
            region: "{region}"
            sts_role_arn: "{pipeline_role_arn}"\
"""

    return osis_client.create_pipeline(
        PipelineName=pipeline_name,
        MinUnits=1,
        MaxUnits=4,
        LogPublishingOptions={
            'IsLoggingEnabled': True,
            'CloudWatchLogDestination': {
                'LogGroup': log_group_name,
            }
        },
        VpcOptions={
            'SubnetIds': [
                subnet_id for subnet_id in subnet_ids_pipeline
            ],
            'SecurityGroupIds': [
                security_group_id for security_group_id in security_group_ids
            ]
        },
        PipelineConfigurationBody=body_config)


def delete_dynamo_table(table_name):
    return dynamodb_client.delete_table(TableName=table_name)


def delete_opensearch_ingestion_pipeline(pipeline_name):
    return osis_client.delete_pipeline(PipelineName=pipeline_name)


def delete_role_and_policies(pipeline_role_name, account_id):
    for policy in ['IngestionPipelinePolicy', 'DynamoDBIngestionPolicy']:
        iam_client.detach_role_policy(RoleName=pipeline_role_name, PolicyArn=f'arn:aws:iam::{account_id}:policy/{policy}')
    time.sleep(5)
    iam_client.delete_role(RoleName=pipeline_role_name)
    time.sleep(5)
    pattern = r"^(IngestionPipelinePolicy|DynamoDBIngestionPolicy)$"
    paginator = iam_client.get_paginator('list_policies')
    for page in paginator.paginate(Scope='Local'):
        for policy in page['Policies']:
            name = policy['PolicyName']
            if re.search(pattern, name):
                iam_client.delete_policy(PolicyArn=policy['Arn'])


def empty_and_delete_bucket(bucket_name):
    try: 
        objects = s3_client.list_objects(Bucket=bucket_name)  
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key']) 
        return s3_client.delete_bucket(Bucket=bucket_name)
    except:
        # If no contents, bucket is already empty
        return s3_client.delete_bucket(Bucket=bucket_name) 
    

def delete_vpc_endpoint(vpc_endpoint_name):
    vpc_endpoint_list = ec2_client.describe_vpc_endpoints()
    for vpc_endpoint in vpc_endpoint_list['VpcEndpoints']:
        for tag in vpc_endpoint['Tags']:
            if tag['Key'] == "Name" and tag['Value'] == vpc_endpoint_name:
                endpoint_id = vpc_endpoint['VpcEndpointId']
                ec2_client.delete_vpc_endpoints(VpcEndpointIds=[endpoint_id])
                return