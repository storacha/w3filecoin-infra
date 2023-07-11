# Architecture

> The w3filecoin pipeline architecture.

## Background

[web3.storage](http://web3.storage) is a Storefront providing APIs to enable users to easily upload CAR files, while getting them available on the IPFS Network and stored in multiple locations via Filecoin Storage Providers. It relies on Spade as a broker to get their user data into Filecoin Storage Providers. Currently, Spade requires a Filecoin Piece with size between 15.875GiB and 31.75GiB to create deals with Filecoin Storage Providers. Moreover, the closer a Filecoin Piece is closer to the upper bound, the most optimal are the associated storage costs.

Taking into account that [web3.storage](http://web3.storage) onboards any type of content (up to a maximum of 4GiB-padded shards to have better utilization of Fil sector space), multiple CAR files uploaded need to be aggregated into a bigger Piece that can be offered to Filecoin Storage Providers. w3filecoin pipeline keeps track of queued CARs (cargo) to be included in Storage Provider deals.

When a CAR file is written into a given web3.storage's bucket, its metadata gets into the w3filecoin processing pipeline. This pipeline is composed of multiple processing queues, together with a job scheduler per queue that will perform the processing. Each queue handles a processing stage with the goal of getting CAR files into Filecoin deals with Storage Providers.

## High Level design

The high level flow for the w3filecoin Pipeline is:

<<<<<<< Updated upstream
- **Consume request** is received once a CAR file is written into a bucket with its metadata {`link`, `size`, `bucketName`, `bucketEndpoint`}. This event is added to a `content_validator_queue`.
- **Content Validator process** validates CARs and writes their references to a `content` table.
=======
- **Event** is triggered once a CAR file is written into a bucket with its metadata {`link`, `size`, `bucketName`, `bucketUrl`, `bucketRegion`, `key`}. This event is added to a `content_validator_queue`.
- **Content Validator process** validates CARs and writes references to a `content` table.
>>>>>>> Stashed changes
- On its own schedule, **Piece maker process** can pull queued content from a `content_queue`, derive piece info for them and write records to a `piece` & `inclusion` tables.
  - a `inclusion` table enables same piece to be included in another aggregate if a deal fails.
- **Agregagtor process** reads from a `cargo_queue` (backed by `inclusion` table), attempts to create an aggregate and if successful it writes to an `aggregate` table.
- **Submission process** reads from the `aggregate_queue`, submits aggregates to the agency (spade proxy) and writes deal record with status "PENDING".
- TBD deal flow

![Pipeline processes](./processes.svg)

The w3filecoin pipeline is modeled into 3 different SST Stacks that will have their infrastructure provisioned in AWS via AWS CloudFormation. These are:

- API Stack
- DB Stack
- Processor Stack

![Architecture](./architecture.png)

## API Stack

TBC
API Gateway to expose:
- Report API for failed aggregates to land into Storage Providers
- Get to know state of deals
- Receive signature requests
- Receive requests to consume content - w3filecoin is designed to enable multiple sources of CAR files to be easily integrated into the pipeline
- ...

## DB Stack

The DB stack relies on Amazon RDS Database to keep the necessary state for the w3filecoin pipeline. Its data model was designed with the aim of being the data structure for each of the processors running within the pipeline, while also enabling the tracking of state of each item in the pipeline and to get a mapping between content CIDs and piece CIDs.

### Schema

```sql
-- Table describes queue of verified CARs to be stored in filecoin
-- CAR is considered in queue when there is no piece referencing it
CREATE TABLE content
(
  -- CAR CID
  link TEXT PRIMARY KEY,
  -- CAR Size
  size number NOT NULL,
  -- Source where the content can be fetched from.
  -- It includes an array of { bucketName: string, bucketRegion: string, key: string, bucketUrl?: string }
  source JSONB NOT NULL,
  -- Timestamp
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Table describes pieces derived corresponding to CARs in the content table. Link (commP) is
-- unique even though cargo reference is not, that is because there may be an error in piece
-- derivation and in that case another correct piece will reference the same content.
CREATE TABLE piece
(
  -- Piece CID
  link TEXT NOT NULL PRIMARY KEY,
  -- Piece size
  size number NOT NULL,
  -- Reference to the content of this piece (CAR CID).
  content TEXT NOT NULL REFERENCES content(link),
  -- Time when the piece was derived from the content
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

CREATE INDEX piece_content_idx ON piece (content);
CREATE INDEX piece_inserted_idx ON piece (inserted);

-- Content for which we need to derive piece CIDs. We will have a process that
-- reads from this queue and writes into `piece` table.
CREATE VIEW content_queue AS
  SELECT content.*
  FROM content
  LEFT OUTER JOIN piece ON content.link = piece.content
  WHERE piece.content IS NULL
  ORDER BY piece.inserted;

-- Table describing pieces to be included into aggregates. If aggregate is NULL then the
-- piece is queued for the aggregation.
CREATE TABLE inclusion
(
  -- Piece CID. Notice that it is not unique because in case of bad piece
  -- aggregate will be rejected and good pieces will be written back here
  -- to be included into new aggregate
  piece TEXT NOT NULL REFERENCES piece(link),
  
  -- Aggregate CID, if NULL the the piece is queued for the aggregation
  aggregate TEXT REFERENCES aggregate(link) NULL,

  -- Priority in the queue. A counter on initial inserts hence providing FIFO order.
  -- When piece is retried (after aggregate is rejected), it can keep original priority
  -- hence prioritizing it over new items yet keeping original FIFO order among rejects.
  priority NUMBER NOT NULL,

  -- Time when the piece was added to the queue.
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,

   -- We may also want to write inclusion proof here
);

CREATE INDEX inclusion_priority_idx ON inclusion (priority DESC, inserted);

-- Piece may end up in multiple aggregates e.g. if aggregate was rejected
-- coalesce is used to create unique constraint with null aggregate column
-- @see https://dba.stackexchange.com/questions/299098/why-doesnt-my-unique-constraint-trigger/299107#299107
CREATE INDEX piece_aggregate_unique_idx ON inclusion (piece, COALESCE(aggregate, ''));

-- Table for created aggregates. 
CREATE TABLE aggregate
(
  -- commP of the aggregate (commP of the commPs)
  link TEXT PRIMARY KEY NOT NULL,
  -- Aggregate size
  size number NOT NULL,
  -- Time when the aggregate was created
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- View for inclusion records that do not have an aggregate.
CREATE VIEW cargo AS
  SELECT *
  FROM inclusion
  WHERE aggregate IS NULL
  ORDER BY priority DESC, inserted;

-- State of aggregate deals. When aggregate is sent to spade-proxy status is 'PENDING'.
-- When spade-proxy requests a wallet signature, status will be updated to 'SIGNED'. Once
-- deal is accepted status changes to `APPROVED`, but if deal fails status will be set to
-- `REJECTED`.
CREATE TABLE deal (
  aggregate TEXT PRIMARY KEY NOT NULL REFERENCES aggregate(link),
  status DEAL_STATUS DEFAULT 'PENDING',
  -- if status is an error this may contain details about the error e.g. json containing
  -- piece CIDs that were invalid.
  detail JSONB,
  -- Time when aggregate was send to spade-proxy
  inserted TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  -- Time when aggregate was signed
  signed TIMESTAMP WITH TIME ZONE,
  -- Time when aggregate was processed (became APPROVED or REJECTED)
  processed TIMESTAMP WITH TIME ZONE
);

CREATE INDEX deal_inserted_idx ON deal (inserted);
CREATE INDEX deal_signed_idx ON deal (signed);
CREATE INDEX deal_signed_idx ON deal (signed);
CREATE INDEX deal_status_idx ON deal (status);

CREATE TYPE DEAL_STATUS AS ENUM
(
  'PENDING',
  'SIGNED',
  'APPROVED',
  'REJECTED'
);

-- View of aggregates to be submitted to spade, that is all aggregates that we do not
-- have deal records for.
CREATE VIEW aggregate_queue AS
  SELECT aggregate.*
  FROM aggregate
  LEFT OUTER JOIN deal ON aggregate.link = deal.aggregate
  WHERE deal.aggregate IS NULL;

-- View for deals pending.
CREATE VIEW deal_pending AS
  SELECT *
  FROM deal
  WHERE status = "PENDING"
  ORDER BY inserted;

-- View for deals approved by storage providers.
CREATE VIEW deal_approved AS
  SELECT *
  FROM deal
  WHERE status = "APPROVED"
  ORDER BY processed;

-- View for deals rejected by storage providers.
CREATE VIEW deal_rejected AS
  SELECT *
  FROM deal
  WHERE status = "REJECTED"
  ORDER BY processed;
```

![DB Schema](./db-schema.png)
https://dbdiagram.io/d/649af07402bd1c4a5e249feb

## Processor Stack

The w3infra processor stack manages several concurrent workflows, deployed as lambda functions, that move items from one stage of the pipeline into another. Each workflow operates independently and on its own schedule, pulling items from one DB stack queue, and pushing into another after some processing.

The workflows running in these pipeline are:

1. **Content Validator workflow** validates CARs and writes references to `content` table.
2. **Piece maker workflow** pulls queued content from `content_queue`, derive piece info for them and write records to the `piece` & `inclusion` tables.
3. **Aggregator workflow** reads from the `cargo` view, attempts to create an aggregate and, if successful, writes to aggregate table.
4. **Submission workflow** reads from the `aggregate_queue`, submits aggregates to the agency (spade proxy) and writes pending deal record.
5. **Deal workflow** ... TBD