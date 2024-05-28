# w3filecoin Tools

> A set of tools to facilitate diagnosing and debugging incidents.

## Getting Started

Run in the main folder of this repo `npm install` to guarantee all the dependencies needed are properly installed.

To be able to run these tools, an AWS account with the AWS CLI MUST be configured locally and have access to the resources of w3filecoin.

## Tools

### Get aggregate deals

Get all known deals for a given aggregate.

```sh
$ npm run get-aggregate-deals bafkz...
> @w3filecoin/tools@0.0.0 get-aggregate-deals
> node get-aggregate-deals.js bafkz...

known information for aggregate bafkz...:
{
  deals: {
    '123': { provider: '111' },
    '124': { provider: '222' }
  }
}
```

### Get aggregates pending deals

```sh
$ npm run get-aggregates-pending-deals

> @w3filecoin/tools@0.0.0 get-aggregates-pending-deals
> node get-aggregates-pending-deals.js

Offered aggregates page size:  4 

Aggregate offer list:
bafkzcibcaapg... at 2024-05-28T06:52:25.779Z
bafkzcibcaapn... at 2024-05-28T07:12:45.567Z
bafkzcibcaapb... at 2024-05-28T07:48:21.420Z
bafkzcibcaapi... at 2024-05-28T08:23:59.203Z
```
