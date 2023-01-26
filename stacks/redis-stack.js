import {
  Api
} from '@serverless-stack/resources'

import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as memorydb from 'aws-cdk-lib/aws-memorydb'

import {
  setupSentry,
  getCustomDomain,
  getRedisLambdaRole,
  AGGREGATE_KEY
} from './config.js'

/**
 * Setup a fully managed, Redis-compatible database, that delivers both in-memory performance
 * and Multi-AZ durability. MemoryDB is a database purpose-built for applications that need
 * ultra-fast performance, and is a great choice for persistent shared state for Lambda functions.
 * A MemoryDB endpoint must be created in a VPC, so that a Lambda function always runs inside a VPC
 * owned by Lambda. Lambda applies network access and security rules to this VPC and maintains and
 * monitors the VPC automatically. To enable access to the MemoryDB cluster, Lambda function’s needs
 * VPC and security settings properly configured.
 * 
 * See more https://aws.amazon.com/blogs/database/access-amazon-memorydb-for-redis-from-aws-lambda/
 * 
 * @param {import('@serverless-stack/resources').StackContext} properties
 */
export function RedisStack({ stack, app }) {
  stack.setDefaultFunctionProps({
    srcPath: 'redis'
  })

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const id = `${stack.stackName}`

  // TODO redisCluster.attrClusterEndpointPort returns bad port
  // https://github.com/aws/aws-cdk/issues/23694
  const REDIS_PORT = 6379

  // Setup VPC
  const redisVpc = new ec2.Vpc(stack, `${id}-redis-vpc`, {
    subnetConfiguration: [
      {
        cidrMask: 24,
        name: `${id}-pi`.toLowerCase(),
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      },
    ],
  })

  // VPC Endpoints improve security, as data does not mix with public Internet traffic
  // we can whitelist aws services that will need to run
  redisVpc.addGatewayEndpoint('dynamoDBEndpoint', {
    service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
  })

  // Create Security group and subnet group to configure right permissions between
  // Redis Cluster and IAM Role to use in lambda functions
  const redisSecurityGroup = new ec2.SecurityGroup(stack, `${id}-redis-security-group`, {
    vpc: redisVpc,
    allowAllOutbound: true,
  })

  // Needed to allow inbound traffic through isolated local network
  redisSecurityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.allTraffic())

  const redisSubnetGroup = new memorydb.CfnSubnetGroup(stack, `${id}-redis-subnet-group`, {
    subnetIds: redisVpc.isolatedSubnets.map(sn => sn.subnetId),
    subnetGroupName: `${id}-sg`.toLowerCase(),
  })

  // Create redis cluster with security group and subnet group created
  const redisCluster = new memorydb.CfnCluster(stack, `${id}-redis-aggregate-cluster`, {
    aclName: 'open-access', // open access within configured VPS subnet
    clusterName: `${id}-redis`.toLowerCase(),
    numShards: 1,
    numReplicasPerShard: 0,
    // https://aws.amazon.com/memorydb/pricing/
    // Only one key, we can use smaller instance for now
    nodeType: 'db.t4g.small',
    port: REDIS_PORT,
    securityGroupIds: [redisSecurityGroup.securityGroupId],
    subnetGroupName: redisSubnetGroup.subnetGroupName,
  })

  // Create role with required policies for lambda to interact with redis cluster
  const role = getRedisLambdaRole(stack)

  const customDomain = getCustomDomain(stack.stage, process.env.HOSTED_REDIS_ZONE)
  const api = new Api(stack, 'redis-api', {
    customDomain,
    defaults: {
      function: {
        timeout: 15 * 60,
        environment: {
          REDIS_HOST: redisCluster.attrClusterEndpointAddress,
          REDIS_KEY: AGGREGATE_KEY
        },
        role,
        vpc: redisVpc,
        securityGroups: [redisSecurityGroup]
      },
    },
    routes: {
      'GET /':        'functions/redis.get',
    }
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
    CustomDomain:  customDomain ? `https://${customDomain.domainName}` : 'Set HOSTED_REDIS_ZONE in env to deploy to a custom domain'
  })

  return {
    redisVpc,
    redisEndpoint: redisCluster.attrClusterEndpointAddress,
    redisPort: REDIS_PORT,
    redisSecurityGroup
  }
}
