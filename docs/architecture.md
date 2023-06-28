# Architecture

> The w3filecoin pipeline architecture.

## Background

[web3.storage](http://web3.storage) APIs enable users to easily upload CAR files, while getting them available on the IPFS Network and stored in multiple locations via Filecoin Storage Providers. 

When CAR files are uploaded they go through a pipeline with multiple steps (like indexing blocks, validating blocks, generating commitment proofs, etc). There are two requirements for these CAR files to make their way to the w3filecoin pipeline via its consumer stack:

- CAR file is written into web3.storage’s `carpark` bucket in R2
- The piece commP and piece size are computed for the CAR file

[web3.storage](http://web3.storage) relies on Spade as a broker to get their user data into Filecoin Storage Providers. Currently, Spade requires a Filecoin Piece with size between 15.875GiB and 31.75GiB to create deals with Filecoin Storage Providers. Moreover, the closer a Filecoin Piece is closer to the upper bound, the most optimal are the associated storage costs.

Taking into account that [web3.storage](http://web3.storage) onboards any type of content (up to a maximum of 4GiB-padded shards to have better utilization of Fil sector space), multiple CAR files uploaded need to be aggregated into a bigger Piece that can be offered to Filecoin Storage Providers. w3filecoin pipeline keeps track of queued CARs (cargo) to be included in Aggregates (ferry), in order to offer these aggregates to Storage Providers. Moreover, w3filecoin operates the lifecycle of these cargo and ferries from ingestion until either succeeding landing into Filecoin deals, or failing.

## High Level design

The high level flow for the w3filecoin Pipeline is:

- Event is triggered once each CAR is copied into R2, its metadata {pieceSize+pieceLink+carLink} is added to the consumer of the w3filecoin pipeline (AWS SQS).
    - This can be achieved by either waiting for a receipt of replication, or AWS Event Bridge event.
- SQS lambda consumer will add given entries to a cargo tracking DB where we keep track of the CARs awaiting to be included into Storage Providers deals.
- “Aggregates” (multiple CARs) will be offered to Storage Providers once the available CARs have enough size to fill up one.
- w3filecoin will keep looking for updates about previously offered aggregates

The w3filecoin pipeline is modeled into 4 different SST Stacks that will be have their infrastructure provisioned in AWS via AWS CloudFormation. These are:

- API Stack
- Consumer Stack
- DB Stack
- CRON Stack

![Architecture](./architecture.png)

## API Stack

TODO
API Gateway to expose:
- Report API for failed aggregates to land into Storage Providers
- Get to know state of deals
- ...

## Consumer Stack

w3filecoin relies on R2 bucket HTTP URLs as the source that Storage Providers will use to fetch CAR files. This way, we need to wait on the replicator to write CAR files into R2. In addition, w3filecoin requires that a `pieceCid` and `pieceSize` is computed for 

Once the above conditions are met, an external source triggers an event to the filecoin pipeline to let it know of a new car to be aggregated. Note that this allows us to make filecoin pipeline work with both w3up and CF Hosted APIs.

Further down the line, in case we make writing to R2 first to better optimize gateway performance, it will also be an easy path forward to hook Filecoin pipeline with it, by leveraging receipts.

For MVP the w3infra replicator lambda sends event to the event bridge, being `w3filecoin` responsible for listening on the event bridge for `car-replicated` events.

## DB Stack

To get CAR files into an aggregated deal, we will need to persist the metadata of these files pending being added to a Filecoin deal in a queue data structure. Potentially, this data structure could also have the concept of priority.

Queue consumers can be triggered (e.g. via a CRON job) to grab items from the Queue and attempt to create an aggregate with them. In case an aggregate can be created, their state should be modified. We can see this as loading up a ferry with cargo once this shipment is ready to being offered via a broker.

### Schema

There are two tables within the DB Stack:
* Cargo - keeps track of cargo (CAR Files ingested by web3.storage) state
* Ferry - keeps track of aggregates state

Both tables enable an implementation of a Priority Queued based on a State Machine on top of AWS RDS.

```sql
CREATE TABLE cargo
(
	-- Filecoin Piece CID - commP of CAR file
	link TEXT PRIMARY KEY,
	-- Filecoin Piece Size
  size number NOT NULL,
	-- CAR file CID
	car_link TEXT NOT NULL,
  -- State of the cargo in the pipeline
  state CARGO_STATE NOT NULL,
  -- Priority in the queue - for now likely same as queuedAt
  priority TEXT NOT NULL,
  -- Timestamp
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  -- TODO: Maybe timestamps for other stats?
  -- Filecoin Aggregate CID - commP of commPs
  ferry_link TEXT REFERENCES ferry(link),
  -- Failed to add into aggregate code
  ferry_failed_code TEXT,
);

CREATE INDEX cargo_stat_idx ON cargo (stat);
CREATE INDEX cargo_car_link_idx ON cargo (car_link);
CREATE INDEX cargo_aggregate_link_idx ON cargo (ferrylink);

CREATE TABLE ferry
(
	-- Filecoin Aggregate CID - commP of commPs
  link TEXT PRIMARY KEY,
  -- Aggregate size in bytes - TODO: maybe nice to have for metrics
  size number NOT NULL,
  -- State of the ferry in the pipeline
  state FERRY_STATE NOT NULL,
  -- Priority in the queue - for now likely same as queuedAt
  priority TEXT NOT NULL,
  -- Timestamp
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
);

CREATE INDEX ferry_stat_idx ON ferry (stat);

-- State of Cargo state machine
CREATE TYPE CARGO_STATE AS ENUM
(
	'QUEUED',
	'OFFERING',
	'SUCCEED',
	'FAILED'
);

-- State of Ferry state machine:
CREATE TYPE FERRY_STATE AS ENUM
(
	'QUEUED',
	'ARRANGING',
	'SUCCEED',
	'FAILED'
);
```

![DB Schema](./db-schema.png)
https://dbdiagram.io/d/649af07402bd1c4a5e249feb

### State Machine

Cargo State Machine might have the following state changes:
* `QUEUED` -> `OFFERING` - when cargo item is associated with an aggregate to offer for storage
* `OFFERING` -> `SUCCEED` - end state as cargo is already available in Storage Providers
* `OFFERING` -> `FAILED` - cargo could not make it to Storage Provider because this specific cargo failed (e.g. wrong commP, or could not be fetched)
* `OFFERING` -> `QUEUED` - cargo could not make it to Storage Provider because other cargo in same aggregate failed, but there is no issue with this specific cargo reported. Therefore, it can be queued for other Aggregate inclusion
* `FAILED` -> `SUCCEED` - cargo previously failed but reason behind it is now solved

Ferry State Machine might have the following state changes:
* `QUEUED` -> `ARRANGING` - when given ferry was included in an `aggregate/offer` invocation to Storage Broker
* `ARRANGING` -> `SUCCEED` - when `aggregate/offer` for ferry succeeded
* `ARRANGING` -> `FAILED` - when `aggregate/offer` for ferry failed

### Flow

1. CAR Files get inserted into `cargo` Table once `R2 write` AND `commP write` events happen (Consumer stack context)
2. CRON JOB triggers lambda function over time. Lambda performs:
    1. queries cargo table for a page of cargo with stat `QUEUED`
    2. sorts page results by their size and attempts to create an aggregate with a compatible size within the results. In case size is not enough, it attempts to get more pages until either having enough cargo or stopping until next cron call.
    3. performs a DB transaction updating `stat` to `OFFERING` and setting `aggregateLink` AND insert entry to `ferry` Table with the `aggregate` information (it is required to guarantee previous state are the same and no concurrent job added something to other aggregate in the meantime)
3. CRON JOB triggers lambda function over time. Lambda performs:
    1. queries `ferry` table for an entry of stat `QUEUED`
    2. invokes `aggregate/offer` to spade-proxy (Must be idempotent!!)
    3. mutates stat to `ARRANGING` in case of partial failure in second write (first was offer invocation), 
4. CRON keeps triggering Lambda function to check for `Receipts` for ferries with stat `ARRANGING`
    1. Once receipt is available, `stat` is mutated to either `SUCCEED` or `FAILED`. In case `FAILED`, `cargo` should also have `aggregateFailedCode` updated.
5. Exposed API endpoint can at any time receive report of offered aggregates failing to get into Filecoin Storage Providers
    1. performs a DB transaction updating `stat` of ferry from `ARRANGING` to `FAILED`, as well as affected `cargo` to from `OFFERING` to either `QUEUED` or `FAILED` depending if the reason for aggregate to fail was that cargo itself, or not. When a cargo item was responsible for the failure of the aggregate offer (for instance due to wrong commP value provided) a reason code can also be persisted on this transaction.
6. Exposed API endpoint can at any time receive requests for the state of a given aggregate or CAR file in a deal.
    1. invokes `aggregate/get` to spade-proxy to grab information about it
    2. Note that this should ideally be cached to avoid hammering Spade Oracle


## Cron Stack

The w3infra cron stack manages the deployed lambda functions that run to consume the DB stack queues and mutate their states as needed.

For more details on these, please refer to the DB Stack Flow subsection.
