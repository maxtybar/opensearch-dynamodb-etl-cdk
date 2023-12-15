import path = require("path");
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as oss from "aws-cdk-lib/aws-opensearchserverless";

export interface Props extends cdk.StackProps {
  readonly collectionName?: string;
  readonly tableName?: string;
  readonly bucketName?: string;
  readonly pipelineName?: string;
  readonly logGroupName?: string;
  readonly vpcName?: string;
  readonly vpcEndpointName?: string;
  readonly currentUserArn: string;
}

const defaultProps: Partial<Props> = {
  collectionName: 'dynamodb-etl-collection',
  tableName: 'opensearch-etl-table',
  bucketName: `dynamodb-oss-etl-bucket-${Math.floor(Math.random() * (1000 - 100) + 100)}`,
  pipelineName: 'dynamodb-etl-pipeline',
  logGroupName: '/aws/vendedlogs/OpenSearchIngestion/dynamodb-osis-pipeline/audit-logs',
  vpcName: 'dynamodb-opensearch-etl-vpc',
  vpcEndpointName: 'dynamodb-etl-collection-endpoint',
};


export class OpensearchDynamodbETLCdkStack extends cdk.Stack {

  readonly collectionName: string;
  readonly tableName: string;
  readonly bucketName: string;
  readonly pipelineName: string;
  readonly logGroupName: string;
  readonly currentUserArn: string;
  readonly vpcName: string;
  readonly vpcEndpointName: string;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    props = { ...defaultProps, ...props };
    
    this.collectionName = props.collectionName as string;
    this.tableName = props.tableName as string;
    this.bucketName = props.bucketName as string;
    this.pipelineName = props.pipelineName as string;
    this.logGroupName = props.logGroupName as string;
    this.currentUserArn = props.currentUserArn;
    this.vpcName = props.vpcName as string; 
    this.vpcEndpointName = props.vpcEndpointName as string;

    // Create VPC for OpenSearchServerless
    const vpc = new ec2.Vpc(this, 'OpenSearchServerlessVpc', {
      vpcName: this.vpcName,
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
      maxAzs: 3,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'private-oss-pipeline-',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
        },
      ]
    });

    // Create security group
    const securityGroup = new ec2.SecurityGroup(this, "OpenSearchServerlessSecurityGroup", {
      vpc: vpc
    });

    // Allow HTTPS ingress from the VPC CIDR
    securityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
    );

    // Allow HTTP ingress from the VPC CIDR
    securityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(80),
    );

    // Create Opensearch servelrless collection
    const collection = new oss.CfnCollection(this, 'OpenSearchServerlessCollection', {
      name: this.collectionName,
      description: 'Collection created by CDK to explore DynamoDB to OpenSearch Pipeline ETL Integration.',
      type: 'SEARCH'
    });

    // S3 bucket for DynamoDB initial export
    const bucket = new s3.Bucket(this, 'OpenSearchIngestionBucket', {
      bucketName: this.bucketName,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Create CloudWatch logs for Ingestion Pipeline
    const ingestionLogGroup = new cdk.aws_logs.LogGroup(this, 'IngestionPipelineLogGroup', {
      logGroupName: this.logGroupName,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      retention: cdk.aws_logs.RetentionDays.ONE_DAY
    });

    // Create OpenSearch Ingestion Pipeline Role
    const pipelineRole = new iam.Role(this, 'IngestionRole', {
      assumedBy: new iam.ServicePrincipal('osis-pipelines.amazonaws.com')
    });

    // Create an IAM role for custom resource
    const dynamoDbPipelineCustomResourceRole = new cdk.aws_iam.Role(this, 'DynamoDbPipelineCustomResourceRole', {
      assumedBy: new cdk.aws_iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    // Add policy to it to allow write, create, delete and update backups on our dynamodb  
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'dynamodb:BatchWriteItem', 
        'dynamodb:CreateTable', 
        'dynamodb:DeleteTable',
        'dynamodb:UpdateContinuousBackups'
      ],
      conditions: {
        StringEquals: {
          'dynamodb:TableName': this.tableName
        }
      },
      resources: ['*'],
    }));

    // Add policy to it to allow pipeline create and delete  
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'osis:CreatePipeline', 
        'osis:DeletePipeline',
        'osis:StopPipeline'
      ],
      resources: [`arn:aws:osis:${this.region}:${this.account}:pipeline/${this.pipelineName}`],
    }));

    // Add policy to it to allow create and modify IAM roles on pipelineRole 
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'iam:PassRole',
        'iam:CreateRole',
        'iam:AttachRolePolicy',
        'iam:DetachRolePolicy',
        'iam:GetRole',
        'iam:DeleteRole'
      ],
      resources: [`${pipelineRole.roleArn}`],
    }));

    // Add policy to allow list policies to delete created policies 
    // on delete event
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'iam:ListPolicies',
      ],
      resources: ['*'],
    }));

    // Add policy to it to allow create policy for OpenSearch Ingestion Pipeline Role  
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'iam:CreatePolicy',
        'iam:DeletePolicy',
      ],
      conditions: {
        StringEquals: {
          'iam:PolicyName': [
            'IngestionPipelinePolicy', 
            'DynamoDBIngestionPolicy'
          ],
        }
      },
      resources: ['*'],
    }));

    // Add policy to it to allow CloudWatch Logs creation
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'logs:CreateLogDelivery',
        'logs:PutResourcePolicy',
        'logs:UpdateLogDelivery',
        'logs:DeleteLogDelivery',
        'logs:DescribeResourcePolicies',
        'logs:GetLogDelivery',
        'logs:ListLogDeliveries'
      ],
      resources: ['*']
    }))

    // Add policy to allow deletion of s3 bucket 
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        's3:ListObjects',
        's3:DeleteObject',
        's3:DeleteObjectVersion',
        's3:ListBucket',
        's3:DeleteBucket'
      ],
      resources: [
        `${bucket.bucketArn}`,
        `${bucket.bucketArn}/*`
      ]
    }))

    // Add poliucy to allow creation and deletion of OpenSearchServerless VPC Enpoint, 
    // as well as updating Network Policy
    dynamoDbPipelineCustomResourceRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      effect: cdk.aws_iam.Effect.ALLOW,
      actions: [
        'aoss:CreateVpcEndpoint',
        'aoss:DeleteVpcEndpoint',
        'aoss:ListVpcEndpoints',
        'aoss:GetSecurityPolicy',
        'aoss:UpdateSecurityPolicy',
        'ec2:CreateVpcEndpoint',
        'ec2:DeleteVpcEndpoints',
        'ec2:ListVpcEndpoints',
        'ec2:DescribeVpcEndpoints',
        'ec2:DescribeVpcs',
        'ec2:DescribeSubnets',
        'ec2:DescribeSecurityGroups',
        'ec2:CreateTags',
        'ec2:DeleteTags',
        'route53:AssociateVPCWithHostedZone',
        'route53:DisassociateVPCFromHostedZone',
      ],
      resources: ['*'],
    }))

    // Opensearch encryption policy
    const encryptionPolicy = new oss.CfnSecurityPolicy(this, 'EncryptionPolicy', {
      name: 'ddb-etl-encryption-policy',
      type: 'encryption',
      description: `Encryption policy for ${this.collectionName} collection.`,
      policy: `
      {
        "Rules": [
          {
            "ResourceType": "collection",
            "Resource": ["collection/${this.collectionName}*"]
          }
        ],
        "AWSOwnedKey": true
      }
      `,
    });

    // Opensearch network policy
    const networkPolicy = new oss.CfnSecurityPolicy(this, 'NetworkPolicy', {
      name: 'ddb-etl-network-policy',
      type: 'network',
      description: `Network policy for ${this.collectionName} collection.`,
      policy: `
        [
          {
            "Rules": [
              {
                "ResourceType": "collection",
                "Resource": ["collection/${this.collectionName}"]
              },
              {
                "ResourceType": "dashboard",
                "Resource": ["collection/${this.collectionName}"]
              }
            ],
            "AllowFromPublic": true
          }
        ]
      `,
    });

    // Opensearch data access policy
    const dataAccessPolicy = new oss.CfnAccessPolicy(this, 'DataAccessPolicy', {
      name: 'ddb-etl-access-policy',
      type: 'data',
      description: `Data access policy for ${this.collectionName} collection.`,
      policy: `
        [
          {
            "Rules": [
              {
                "ResourceType": "collection",
                "Resource": ["collection/${this.collectionName}*"],
                "Permission": [
                  "aoss:CreateCollectionItems",
                  "aoss:DescribeCollectionItems",
                  "aoss:DeleteCollectionItems",
                  "aoss:UpdateCollectionItems"
                ]
              },
              {
                "ResourceType": "index",
                "Resource": ["index/${this.collectionName}*/*"],
                "Permission": [
                  "aoss:CreateIndex",
                  "aoss:DeleteIndex",
                  "aoss:UpdateIndex",
                  "aoss:DescribeIndex",
                  "aoss:ReadDocument",
                  "aoss:WriteDocument"
                ]
              }
            ],
            "Principal": [
              "${pipelineRole.roleArn}",
              "${this.currentUserArn}",
              "arn:aws:iam::${this.account}:user/Admin"
            ]
          }
        ]
      `,
    });

    // Custom resource to populate DynamoDB with dummy data
    const onEvent = new cdk.aws_lambda.Function(this, 'DynamoDBPipelineCustomFunction', {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_12,
      handler: 'custom_resource.on_event',
      code: cdk.aws_lambda.Code.fromAsset(path.join(__dirname, '../assets')),
      architecture: cdk.aws_lambda.Architecture.X86_64,
      timeout: cdk.Duration.seconds(600),
      environment: {
        TABLE_NAME: this.tableName,
        PIPELINE_NAME: this.pipelineName,
        PIPELINE_ROLE_NAME: pipelineRole.roleName,
        REGION: this.region,
        ACCOUNT_ID: this.account,
        NETWORK_POLICY_NAME: networkPolicy.name,
        BUCKET_NAME: this.bucketName,
        BUCKET_ARN: bucket.bucketArn,
        COLLECTION_ARN: collection.attrArn,
        COLLECTION_NAME: this.collectionName,
        COLLECTION_ENDPOINT: collection.attrCollectionEndpoint,
        LOG_GROUP_NAME: this.logGroupName,
        VPC_ID: vpc.vpcId,
        VPC_ENDPOINT_NAME: this.vpcEndpointName,
        SECURITY_GROUP_IDS: securityGroup.securityGroupId,
        SUBNET_IDS_ISOLATED: JSON.stringify([
          // Get private subnet for pipeline from AZ that ends with 'a'
          ...vpc.isolatedSubnets
                .filter(subnet => subnet.availabilityZone.endsWith('a'))
                .map(subnet => subnet.subnetId),
          // Get private subnet for pipeline from AZ that ends with 'b'
          ...vpc.isolatedSubnets
                .filter(subnet => subnet.availabilityZone.endsWith('b'))
                .map(subnet => subnet.subnetId)
        ]),
      },
      role: dynamoDbPipelineCustomResourceRole
    });

    const dynamoDbPipelineCustomResourceProvider = new cdk.custom_resources.Provider(this, 'DynamoDBPipelineCustomResourceProvider', {
      onEventHandler: onEvent,
      logRetention: cdk.aws_logs.RetentionDays.ONE_DAY
    });

    const customResource = new cdk.CustomResource(this, 'DynamoDBPipelineCustomResource', {
      serviceToken: dynamoDbPipelineCustomResourceProvider.serviceToken,
    });

    collection.node.addDependency(encryptionPolicy);
    collection.node.addDependency(networkPolicy);
    collection.node.addDependency(dataAccessPolicy);

    new cdk.CfnOutput(this, 'BucketName', {
      value: `${this.bucketName}`
    });

    new cdk.CfnOutput(this, 'TableName', {
      value: `${this.tableName}`
    });

    new cdk.CfnOutput(this, 'OpenSearchServerlessCollectionEndpoint', {
      value: `${collection.attrCollectionEndpoint}`
    });

    new cdk.CfnOutput(this, 'OpenSearchIngestionPipelineName', {
      value: `${this.pipelineName}`
    });

    new cdk.CfnOutput(this, 'OpenSearchIngestionLogGroup', {
      value: `${this.logGroupName}`
    });

    new cdk.CfnOutput(this, 'OpenSearchServerlessVPCId', {
      value: vpc.vpcId
    });
  }
}