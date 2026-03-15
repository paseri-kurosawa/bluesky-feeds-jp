import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigatewayv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as apigatewayv2_integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as path from 'path';

export class BlueskyFeedJpStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const env = process.env;

    // === VPC Configuration ===
    const vpc = new ec2.Vpc(this, 'BlueskyFeedVpc', {
      maxAzs: 2,
      natGateways: 0,  // Strict: no NAT Gateway
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
        },
      ],
    });

    // === Security Groups ===
    const lambdaSecurityGroup = new ec2.SecurityGroup(this, 'LambdaSecurityGroup', {
      vpc,
      description: 'Security group for Lambda functions',
      allowAllOutbound: true,
    });

    const valkeySecurityGroup = new ec2.SecurityGroup(this, 'ValkeySecurityGroup', {
      vpc,
      description: 'Security group for Valkey Serverless',
      allowAllOutbound: true,
    });

    // Allow Lambda to connect to Valkey
    valkeySecurityGroup.addIngressRule(
      lambdaSecurityGroup,
      ec2.Port.tcp(6379),
      'Allow Lambda to connect to Valkey'
    );

    // === Valkey Serverless Cache ===
    const subnetGroup = new elasticache.CfnSubnetGroup(this, 'ValkeySubnetGroup', {
      description: 'Subnet group for Valkey Serverless',
      subnetIds: vpc.isolatedSubnets.map(subnet => subnet.subnetId),
      cacheSubnetGroupName: 'bluesky-feed-valkey-subnet-group',
    });

    const valkeyCache = new elasticache.CfnServerlessCache(this, 'ValkeyServerlessCache', {
      engine: 'valkey',
      serverlessCacheName: 'bluesky-feed-cache',
      description: 'Valkey Serverless cache for Bluesky feed',
      subnetIds: vpc.isolatedSubnets.map(subnet => subnet.subnetId),
      securityGroupIds: [valkeySecurityGroup.securityGroupId],
    });

    valkeyCache.addDependency(subnetGroup);

    // Get Valkey endpoint (placeholder - will be filled after creation)
    const valkeyEndpoint = env.VALKEY_ENDPOINT || 'bluesky-feed-cache.serverless.apne1.cache.amazonaws.com';

    // === Lambda Layers ===
    const redisLayer = new lambda.LayerVersion(this, 'RedisLayer', {
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/layers/redis')),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
    });

    // === Lambda Functions ===

    // 1. DID Handler Lambda
    const didHandlerLambda = new lambda.Function(this, 'DidHandlerLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/handlers/did')),
      handler: 'handler.lambda_handler',
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      logRetention: logs.RetentionDays.ONE_WEEK,
      environment: {
        FEED_DID: env.FEED_DID || 'did:web:example.com',
        SERVICE_ENDPOINT: env.SERVICE_ENDPOINT || 'https://example.com',
      },
    });

    // 2. Describe Feed Lambda
    const describeFeedLambda = new lambda.Function(this, 'DescribeFeedLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/handlers/describe')),
      handler: 'handler.lambda_handler',
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      logRetention: logs.RetentionDays.ONE_WEEK,
      environment: {
        FEED_DID: env.FEED_DID || 'did:web:example.com',
      },
    });

    // 3. Get Feed Lambda (VPC)
    const getFeedLambda = new lambda.Function(this, 'GetFeedLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/handlers/get_feed')),
      handler: 'handler.lambda_handler',
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      logRetention: logs.RetentionDays.ONE_WEEK,
      layers: [redisLayer],
      vpc,
      securityGroups: [lambdaSecurityGroup],
      environment: {
        VALKEY_ENDPOINT: valkeyEndpoint,
      },
    });

    // === S3 Bucket for Badword Analysis ===
    const badwordBucket = new s3.Bucket(this, 'BadwordAnalysisBucket', {
      bucketName: `bluesky-feed-badword-analysis-${env.CDK_DEFAULT_ACCOUNT}`,
      versioned: false,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          // Delete badword-analysis files after 3 days (intermediate analysis files)
          prefix: 'badword-analysis/',
          expiration: cdk.Duration.days(3),
        },
      ],
    });

    // === S3 Bucket for Statistics ===
    const statisticsBucket = new s3.Bucket(this, 'StatisticsBucket', {
      bucketName: `bluesky-feed-statistics-${env.CDK_DEFAULT_ACCOUNT}`,
      versioned: false,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          // Keep statistics for 30 days
          prefix: 'stats/',
          expiration: cdk.Duration.days(30),
        },
      ],
    });

    // 4. Ingest Lambda (Container Image - VPC外)
    const ingestLambda = new lambda.DockerImageFunction(this, 'IngestLambda', {
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../lambda/ingest')),
      timeout: cdk.Duration.seconds(300),
      memorySize: 3008,
      logRetention: logs.RetentionDays.TWO_WEEKS,
      environment: {
        BSKY_HANDLE: env.BSKY_HANDLE || '',
        BSKY_APP_PASSWORD: env.BSKY_APP_PASSWORD || '',
        S3_BUCKET: badwordBucket.bucketName,
        STATISTICS_BUCKET: statisticsBucket.bucketName,
        STORE_FUNCTION_NAME: '', // Will be set after creation
      },
    });

    // 5. Store Lambda (VPC)
    const storeLambda = new lambda.Function(this, 'StoreLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/handlers/store')),
      handler: 'handler.lambda_handler',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      logRetention: logs.RetentionDays.ONE_WEEK,
      layers: [redisLayer],
      vpc,
      securityGroups: [lambdaSecurityGroup],
      environment: {
        VALKEY_ENDPOINT: valkeyEndpoint,
      },
    });

    // Set Store Lambda name in Ingest environment
    ingestLambda.addEnvironment('STORE_FUNCTION_NAME', storeLambda.functionName);

    // Grant Ingest permission to invoke Store
    storeLambda.grantInvoke(ingestLambda);

    // Grant Ingest Lambda permission to read and write to S3
    badwordBucket.grantReadWrite(ingestLambda);
    statisticsBucket.grantWrite(ingestLambda);
    statisticsBucket.grantRead(ingestLambda); // For listing files to update index

    // === HTTP API Gateway ===
    const httpApi = new apigatewayv2.HttpApi(this, 'BlueskyFeedApi', {
      apiName: 'BlueskyFeedApi',
      description: 'HTTP API for Bluesky feed generator',
      corsPreflight: {
        allowOrigins: ['*'],
        allowMethods: [apigatewayv2.CorsHttpMethod.GET, apigatewayv2.CorsHttpMethod.POST],
      },
    });

    // Route: /.well-known/did.json
    httpApi.addRoutes({
      path: '/.well-known/did.json',
      methods: [apigatewayv2.HttpMethod.GET],
      integration: new apigatewayv2_integrations.HttpLambdaIntegration('DidIntegration', didHandlerLambda),
    });

    // Route: /xrpc/app.bsky.feed.describeFeedGenerator
    httpApi.addRoutes({
      path: '/xrpc/app.bsky.feed.describeFeedGenerator',
      methods: [apigatewayv2.HttpMethod.GET],
      integration: new apigatewayv2_integrations.HttpLambdaIntegration('DescribeIntegration', describeFeedLambda),
    });

    // Route: /xrpc/app.bsky.feed.getFeedSkeleton
    httpApi.addRoutes({
      path: '/xrpc/app.bsky.feed.getFeedSkeleton',
      methods: [apigatewayv2.HttpMethod.GET, apigatewayv2.HttpMethod.POST],
      integration: new apigatewayv2_integrations.HttpLambdaIntegration('GetFeedIntegration', getFeedLambda),
    });

    // === EventBridge Scheduling ===
    const ingestRule = new events.Rule(this, 'IngestScheduleRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(20)),
      description: 'Run feed ingest every 20 minutes (100 posts × 3 per hour for pseudo-streaming)',
    });

    ingestRule.addTarget(new targets.LambdaFunction(ingestLambda));

    // === Outputs ===
    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: httpApi.url || '',
      description: 'HTTP API endpoint URL',
    });

    new cdk.CfnOutput(this, 'ValkeyEndpoint', {
      value: valkeyEndpoint,
      description: 'Valkey Serverless endpoint',
    });

    new cdk.CfnOutput(this, 'BadwordBucket', {
      value: badwordBucket.bucketName,
      description: 'S3 bucket for badword analysis output',
    });

    new cdk.CfnOutput(this, 'StatisticsBucketOutput', {
      value: statisticsBucket.bucketName,
      description: 'S3 bucket for ingest statistics reports',
    });
  }
}
