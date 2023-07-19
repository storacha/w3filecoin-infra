# Architecture

> The w3filecoin pipeline architecture.

## Background

[web3.storage](http://web3.storage) is a Storefront providing APIs to enable users to easily upload CAR files, while getting them available on the IPFS Network and stored in multiple locations via Filecoin Storage Providers. It relies on Spade as a broker to get their user data into Filecoin Storage Providers. Currently, Spade requires a Filecoin Piece with size between 15.875GiB and 31.75GiB to create deals with Filecoin Storage Providers. Moreover, the closer a Filecoin Piece is closer to the upper bound, the most optimal are the associated storage costs.

Taking into account that [web3.storage](http://web3.storage) onboards any type of content (up to a maximum of 4GiB-padded shards to have better utilization of Fil sector space), multiple CAR files uploaded need to be aggregated into a bigger Piece that can be offered to Filecoin Storage Providers. w3filecoin pipeline keeps track of queued CARs (cargo) to be included in Storage Provider deals.

When a CAR file is written into a given web3.storage's bucket, its piece is computed and sent into the w3filecoin processing pipeline. This pipeline is composed of multiple processing queues that will perform the processing of pieces until they land into a Filecoin deal.

## High Level design

The high level flow for the w3filecoin Pipeline is:

- **piece inclusion request** is received by a Storefront with `pieceCid` (TBD `endpoint` to get presigned urls from given we want to support w3up, web3.storage, ...?)
- **piece queued** to be aggregated
- **piece contenation** into ferries until a ferry has enough load to become an aggregate offer
- **aggregate submission** to Storage Provider broker
- **deal tracking** and **deal recording** once fulfilled

TODO: Update diagram

![Pipeline processes](./processes.svg)

The w3filecoin pipeline is modeled into 3 different SST Stacks that will have their infrastructure provisioned in AWS via AWS CloudFormation. These are:

- API Stack
- Processor Stack
- Data Stack

TODO: Update diagram

![Architecture](./architecture.png)

## API Stack

The w3filecoin API Stack exposes a HTTP API that both enables storefront APIs to request `pieces` to be included into Filecoin deals and get status of a Filecoin deal, as well as to receive reports of aggregates that failed to land into a Filecoin Deal.

TODO complete this subsection

- Post piece - w3filecoin is designed to enable multiple sources of CAR files to be easily integrated into the pipeline
  - parties with permissions to write into the system can do so
  - Piece should make its way for the queue if not there yet
- Report API for failed aggregates to land into Storage Providers?
- Get to know state of deals
- ...

## Processor Stack

When a `piece` is posted into the w3filecoin pipeline, its journey starts by getting queued to be aggregated into a larger `piece` that will be offered to Storage Providers, i.e. an aggregate. The `Queue stack` consists of a **multiple queue system**, where the individual pieces will be buffered together in several stages until they are ready to form an aggregate (**32 GiB** piece, or close to this size).

This design is built on top of the following assumptions:
- Maximum SQS batch size for standard queue is **10_000**
- Maximum SQS batch size for FIFO queue is **10**
- SQS FIFO queue garantees **exactly-once** processing
- Maximum number of pieces for a **32 GiB** aggregate is **262_144**

The `piece-queue` is the first queue in this system and is a standard SQS queue. It buffers individual pieces until a batch of **10_000** is ready. Once this batch is ready, a SQS consumer will try to create smaller piece aggregates named Ferries. A ferry is a `dag-cbor` encoded data structure that contains a set of pieces that form a partial aggregate. This data structure will be stored so that only its CID is sent in SQS Messages.

```typescript
interface Ferry {
  // Pieces inside the ferry
  pieces: FerryPiece[],
  // Aggregate composed by pieces in ferry enabling us to derive its size
  aggregate: FerryPiece,
}

interface FerryPiece {
  link: PieceCID,
  // timestamp that piece was received
  inserted: string,
  // Policies that this piece is under
  policies: PiecePolicy[]
}

type PiecePolicy = 'PREVIOUS_SUBMISSION_FAILED'
```

A ferry consists of a buffer of pieces that are getting filled up to become an aggregate ready for a Filecoin deal. This SHOULD allow a batch size of **10_000** to at least be able to create a ferry with size **1 GiB**.

To create ferries from a a batch, the SQS consumer SHOULD start by sorting the received batch by `piece` size and start filling up aggregates. If an aggregate gets to its desirable size directly from the batch **32 GiB**, it should be stored and added to the `submission-queue` right away. Otherwise, the ferry is stored and added to the `ferry-queue` once all the batch is processed. Note that:
- when a **32 GiB** is built from the initial batch, it is possible that no other ferry can get loaded with **1 GiB** of pieces. If that is the case, consumer can discard all the remaining pieces back to the queue.

The second queue in this system is the `ferry-queue`, a FIFO queue that acts as a reducer by concatenating the pieces of multiple ferries together generating bigger and bigger feries until one has the desirable size. In other words, the load of each ferry is concatenated, and its resulting ferry is added to the `ferry-queue` again until an aggregate can be built. A queue consumer can act as soon as a batch of 2 is in the queue, so that aggregates can be created as soon as possible.

The SQS consumer MUST start by fetching all the `dag-cbor` encoded data of both ferries in the batch. Afterwards, their pieces SHOULD be sorted by its size and a new aggregate is created. In case it has the desired **32 GiB** size, it should be stored and its CID sent into the `submission-queue`, otherwise the new ferry should be stored and put back into the queue. Note that:
- While pieces should be sorted by size, some policies in a piece might impact this sorting. For instance, if a piece was already in a previous aggregate that failed to be stored by a Storage Provider, it can be included faster into an aggregate
- Note that excess pieces not included in **32 GiB** aggregate when ferries are concatenated MUST be included into a new ferry and put back into the queue
- Minimum size for an aggregate can also be specified to guarantee we don't need to have exactly **32 GiB** to offer it

Once a ferry reaches the desired size of **32 GiB** it is written into the `submission-queue`. Consumers for this queue can be triggered once a single item is in the batch, so that an aggregate offer can be submitted to spade and a `DealTracker` of the aggregate offer is added to the `deal-queue`.

```typescript
interface DealTracker {
  // content of the offer
  ferryCid: Link
  // invocation and task CID to be able to go through receipts
  invocation: Link
  task: Link
  // timestamp of invocation submitted
  invoked: string
  // timestamp of oldest piece
  oldestPiece: string
}
```

The `deal-queue` is the final stage of this multiple queue system. It tracks deal status until there is an update on the state of the deal to either `Approved` or `Rejected` by checking if a receipt exists. Once deal is settled, the ferry content is fetched and piece/aggregate mapping is written together with Deal status.

![queues](./queue.svg)

### Queues overview

| name       | type     | batch | window | DLQ |
|------------|----------|-------|--------|-----|
| piece      | standard | 10000 | 300 s  | TBD |
| ferry      | FIFO     | 2     | 5 m    | TBD |
| submission | FIFO     | 1     | 5 m    | TBD |
| deal | FIFO     | 10     | 5 m    | TBD |

Other relevant notes:
- with current approach, we have an append only log where writes into the Database only happen when we have a deal in the very end, also resulting in a super small amount of operations on the DB
- while `w3up` CAR files can be limited to `4 GiB` to have a better utilization of Fil sector space, same does not currently happen with `pickup` (and perhaps other systems in the future). Designing assuming maximum will be that value is not a good way to go.
- current design enables us to quite easily support bigger deals.
- if an aggregate fails to land into a Storage Provider, the problematic piece(s) can be removed and a ferry created without that piece. This way, it can already be added to the `ferry queue`

Challenges/compromises:
- where to hook alerts when things are getting delayed? we can hook alerts when requests to spade are failing, but will that be enough?
  - perhaps we should handle failure tolerance (if needed) in `spade-proxy` where we keep track of items failing in a queue that we can hook up a Filecoin lite node

## Data Stack

The Data stack is responsible for the state of the w3filecoin.

It keeps track of the received pieces, created aggregates, as well as in which aggregate a given piece is. In addition, it must keep the necessary state for the Ferry queue to operate, as well as the state of a given aggregate over time until a deal is fulfilled. It is worth mentioning, that keeping track of problematic pieces that could not be added to an aggregate should also be properly tracked.

To achieve required state management, we will be relying on a S3 Bucket and a dynamoDB table.

### Queries and Resources

1. Check if piece is already in the pipeline (S3 Bucket)
  - Before getting a piece into the pipelined queue, it should be stored to guarantee uniqueness
  - Also important to be able to report back on in progress work when deal state of a piece is still unknown by fallbacking to Head Request in bucket
  - Key `${pieceCid}/{pieceCid}`, Value empty
    - TODO: actually we might need to use the value to be able to grab the URLs for the piece...
2. Put and Get ferry blocks (S3 Bucket)
  - While `ferry-queue` is working, these blocks will be stored to be propagated through queue stages via their CIDs
  - Key `${blockCid}/{blockCid}`, Value empty with expiration date
3. Write pieces together with the aggregate they are part of (DynamoDB)
  - Based on schema below
4. Get deal state of a given piece
  - Rely on DynamoDB to get `link` of the aggregate within the piece (secondary index) and use it ask Spade for details for the deal
  - In case there is no record in Dynamo, we can fallback to check S3 bucket, in order to reply that piece is being aggregated if it is there

### DynamoDB Schema

```typescript
interface aggregateEntry {
  // CID of the aggregate (primary index)
  link: PieceCID
  // CID of the piece (can be secondary index)
  piece: PieceCID
  // timestamp for piece inserted
  inserted: string
  // timestamp for aggregate submission
  submited: string
  // timestamp for aggregate deal resolution
  resolved: string
  // TODO: perhaps timestamp for expired so that we can query future aggregates?
  // TODO: inclusion proof?
  // status of the deal
  status: 'APPROVED' | 'REJECTED'
  // failed reason
  failedReaon?: string
  // invocation and task CID to be able to go through receipts
  invocation: CID
  task: CID
}
```

Note: we can add a different table to track Deal specific metrics if needed?
