#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { OpensearchDynamodbETLCdkStack } from '../lib/opensearch-dynamodb-etl-cdk-stack';

const app = new cdk.App();
new OpensearchDynamodbETLCdkStack(app, 'OpensearchDynamodbETLStack', {
  env: { 
    account: process.env.CDK_DEFAULT_ACCOUNT, 
    region: process.env.CDK_DEFAULT_REGION 
  },
  currentUserArn: app.node.tryGetContext("UserArn")
});