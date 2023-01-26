<h1 align="center">⁂<br/>web3.storage</h1>
<p align="center">

The filecoin pipeline infra for [w3protocol] built on [SST].

## Getting Started

The repo contains the infra deployment code and the service implementation.

```
├── stacks      - sst and aws cdk code to deploy all the things
└── api     - lambda & dynamoDB implementations for the filecoin pipeline
```

To work on this codebase **you need**:

- Node.js >= v16 (prod env is node v16)
- An AWS account with the AWS CLI configured locally
- Copy `.env.tpl` to `.env.local`
- Install the deps with `npm i`

Deploy dev services to your aws account and start dev console

```console
npm start
```

See: https://docs.sst.dev for more info on how things get deployed.

## Deployment 

Deployment is managed by [seed.run]. PR's are deployed automatically to `https://<pr#>.filecoin.web3.storage`. 

The `main` branch is deployed to https://staging.filecoin.web3.storage and staging builds are promoted to prod manually via the UI at https://console.seed.run

### Environment Variables

Ensure the following variables are set in the env when deploying

#### `HOSTED_ZONE`

The root domain to deploy the API to. e.g `filecoin.web3.storage`. The value should match a hosted zone configured in route53 that your aws account has access to.

#### `HOSTED_REDIS_ZONE`

The root domain to deploy the Redis specific API to. e.g `redis-filecoin.web3.storage`. The value should match a hosted zone configured in route53 that your aws account has access to.

</p>

[SST]: https://sst.dev
[seed.run]: https://seed.run
[w3protocol]: https://github.com/web3-storage/w3protocol