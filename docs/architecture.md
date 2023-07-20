# Architecture

> The w3filecoin pipeline architecture.

## Background

[web3.storage](http://web3.storage) is a Storefront providing APIs to enable users to easily upload CAR files, while getting them available on the IPFS Network and stored in multiple locations via Filecoin Storage Providers. It relies on Spade as a broker to get their user data into Filecoin Storage Providers. Currently, Spade requires a Filecoin Piece with size between 16GiB and 32GiB to create deals with Filecoin Storage Providers. Moreover, the closer a Filecoin Piece is closer to the upper bound, the most optimal are the associated storage costs.

Taking into account that [web3.storage](http://web3.storage) onboards any type of content (up to a maximum of 4GiB-padded shards to have better utilization of Fil sector space), multiple CAR files uploaded need to be aggregated into a bigger Piece that can be offered to Filecoin Storage Providers. w3filecoin pipeline keeps track of queued CARs (cargo) to be included in Storage Provider deals.

When a CAR file is written into a given web3.storage's bucket, its piece is computed and sent into the w3filecoin processing pipeline. This pipeline is composed of multiple processing queues that accumulate pieces into aggregates and submit them into a Filecoin deal queue.

## High Level design

The high level flow for the w3filecoin Pipeline is:

- **piece inclusion request** is received by a Storefront with `pieceCid` (TBD `endpoint` to get presigned urls from given we want to support w3up, web3.storage, ...?)
- **piece queued** to be aggregated
- **piece concatenation** into ferries until a ferry has enough load to become an aggregate offer
- **aggregate submission** to Storage Provider broker
- **deal tracking** and **deal recording** once fulfilled

![Pipeline processes](./pipeline.svg)

The w3filecoin pipeline is modeled into 3 different SST Stacks that will have their infrastructure provisioned in AWS via AWS CloudFormation. These are:

- API Stack
- Processor Stack
- Data Stack

![Architecture](./architecture.svg)

## API Stack

The w3filecoin stack has an API that authorized _Storefront_s can use in order to:

- Submit _piece_s for to be included into the aggregates for which Filecoin deals are arranged.
- Query Filecoin deal status of the aggregate by submitted _piece_s.

And an API for the authorized deal _Broker_s in order to:

- Report failed aggregate deals

TODO complete this subsection

- Piece submission - w3filecoin is designed to enable CARs from multiple sources to easily enter into the pipeline
  - Authorized actors can submit pieces into the system
  - Submitted pieces go through aggregation and deal submission process
- Report API for failed aggregates to land into Storage Providers?
- Get to know state of deals
- ...

## Processor Stack

When a `piece` is submitted into the w3filecoin pipeline, its journey starts by getting queued to be included into an `aggregate` piece (large compound piece) that is offered to Storage Providers. The `Queue stack` consists of a **multiple queue system**, where individual pieces get accumulated until they can be formed into (**32GiB** piece) aggregate.

This design is built on top of the following assumptions:
- Maximum SQS batch size for standard queue is **10_000**
- Maximum SQS batch size for FIFO queue is **10**
- SQS FIFO queue garantees **exactly-once** processing
- Maximum number of pieces in a **32 GiB** aggregate is **262_144**

The `piece-queue` is the first queue in this system and is a standard SQS queue. It buffers individual pieces received into a batch of **10_000**. Once this batch is ready, a SQS consumer encodes set of pieces into a `PieceBatch` structure in DAG-CBOR format, stores it in the `buffer-store` and submits it's CID into a second processing queue `batch-queue`. This allows us to keep messages in the queue small.

```typescript
interface PieceBatch {
  // Pieces inside the batch
  pieces: BatchPiece[]
}

interface BatchPiece {
  piece: PieceCID
  // number of milliseconds elapsed since the epoch when piece was received
  inserted: number
  // Policies that this piece is under
  policy: PiecePolicy
}

type PiecePolicy =
  | NORMAL
  | RETRY

type NORMAL = 0
type RETRY = 1
```

A `PieceBatch` consists of a buffer of pieces that are getting filled up to become an aggregate ready for a Filecoin deal. This SHOULD allow a batch size of **10_000** to at least be able to create a batch with size **1 GiB**.

The second queue in this system is the `batch-queue`, a FIFO queue that acts as a reducer by concatenating the pieces of multiple batches together generating bigger and bigger sets until one has the desirable size for an aggregate. A queue consumer can act as soon as a batch of 2 is in the queue, so that aggregates can be created as soon as possible.

The SQS consumer MUST start by fetching the `dag-cbor` encoded data of both batches in the batch. Afterwards, their pieces SHOULD be sorted by its size and an aggregate is built with them. In case it has the desired **32 GiB** size, it should be stored and its CID sent into the `submission-queue`, otherwise the new batch should be stored and put back into the queue. Note that:
- While pieces should be sorted by size, policies in a piece might impact this sorting. For instance, if a piece was already in a previous aggregate that failed to be stored by a Storage Provider, it can be included faster into an aggregate
- Note that excess pieces not included in **32 GiB** aggregate when batches are concatenated MUST be included into a new batch and put back into the queue
- Minimum size for an aggregate can also be specified to guarantee we don't need to have exactly **32 GiB** to offer it

Once a built aggregate reaches the desired size of **32 GiB** it is written into the `submission-queue`. Consumers for this queue can be triggered once a single item is in the batch, so that an aggregate offer can be submitted to spade and a `DealTracker` of the aggregate offer is added to the `deal-queue`.

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
| batch      | FIFO     | 2     | 5 m    | TBD |
| submission | FIFO     | 1     | 5 m    | TBD |
| deal       | FIFO     | 10    | 5 m    | TBD |

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
