# Deal Tracker

## Background

Current solutions to track if contracted Filecoin Deals are sealed rely on tracking the events on the Filecoin chain. Once Filecoin SP(s) are contracted to store a given Piece, this Piece will be fetched and validated by SP(s). Once that happens, deal is sealed and written on chain.

w3filecoin is an end-to-end verifiable Filecoin deals pipeline. Dealer implementations MUST issue signed receipts when offered aggregates are stored with SPs, so that Storefront implementations can issue signed receipts that pieces offered by users were added into Filecoin Deals.

As part of the w3filecoin pipeline, a Deal Tracker implementation will be the service that Storefront and Dealer implementations can rely on, in order to get to know whether a given aggregate is into one/or more deals.

To track updates on chain in the future, it MAY make sense to implement web3.storage's own scraper and oracle for Filecoin. In the meantime, Spade team is providing w3filecoin an "access" to their Oracle via an endpoint. This way, it is possible to get to know the deals that Spade put together on the behalf of Storefront.

Deal Tracker MUST be compatible with deals that are not handled by Spade. This is particularly important within the context of web3.storage Filecoin SLA. In case Spade is down, web3.storage's team MAY need to offer a deal in other way. Accordingly, Deal Tracker should be pluggable and rely on multiple sources to get to know if a Deal was successful, so that receipt chain works as expected for Storefront and Dealer implementations.

## Spade oracle access

Spade's oracle in its current form will hard-stop functioning in ~November (due to network upgrades). To not cause disruption in services like retrieval bot and Deal Tracker, Spade team put together an endpoint that we can rely to track what is on chain over time.

> https://cargo.dag.haus/active_replicas.json.zst

Note that the file provided is compressed and must be decompressed to use. At the time of writing, the compressed file has a size of ~3MB and the decompressed file has a size of ~29MB.

The provided active replicas file is updated each hour and includes a `state_epoch` property with the Fil epoch the state represents. The provided file includes all the active deals for web3.storage tenant ID, and not the entire chain view.

The provided file is not an incrementally updated list. But it is partially a copy of what's in the original deals table (that web3.storage currently FDW to).

Format:

```json
{
  "state_epoch": 3273364,
  "active_replicas": [
    {
      "contracts": [
        {
          "provider_id": 2095132,
          "legacy_market_id": 40745772,
          "legacy_market_end_epoch": 4477915
        },
        {
          "provider_id": 20378,
          "legacy_market_id": 41028577,
          "legacy_market_end_epoch": 4482396
        }
      ],
      "piece_cid": "baga6ea4seaqhmw7z7q3jypdr54xaluhzdn6syn7ovovvjpaqul2qqenhmg43wii",
      "piece_log2_size": 35,
      "optional_dag_root": "bafybeia22rwl3x3kwyfe6k4hnh3wwzhyr4uulm4rqkxd6ydjm5ut5cqinu"
    }
  ]
}     
```

See: https://filfox.info/en/deal/41028577

## Use cases

Currently, there are two main use cases for a Deal Tracker implementation. They are both specified in the [w3-filecoin spec](https://github.com/web3-storage/specs/blob/main/w3-filecoin.md#deal-tracker-can-be-queried-for-the-aggregate-status).

A Deal Tracker implementation must provide `deal/info` capability that:
- Dealer implementation can invoke to issue `aggregate/accept` receipts when there is a deal
- Storefront can invoke on behalf of users to get deals available for given upload/aggregate.

## Architecture

Deal Tracker has a store where it keeps track of available contracts for each piece. Each time `deal/info` is invoked, lambda will look into store and return available entries for the piece.

A CRON job will wake up each hour and update store with new contracts that were written on chain from several sources (like Spade Oracle).

## Stores

### deal-store

```typescript
interface Deal {
  // PieceCid of an Aggregate `bagy...aggregate` (primary index, partition key)
  piece: PieceCID
  // address of the Filecoin storage provider storing deal
  provider: string
  // Deal identifier
  dealId: number
  // Epoch of deal expiration
  expirationEpoch: number
  // Identifier of the source of the deal information
  source: string
  // Date when deal was added as ISO string
  insertedAt: string
}
```

### spade-oracle-store

Key: `cargo.dag.haus`
Value: decompressed JSON dowloaded from https://cargo.dag.haus/active_replicas.json.zst

## Spade Oracle source

When `active_replicas` file is downloaded, it is decompressed and compared with previous downloaded list. The new `active_replicas` are written into the `deal-store`. Afterwards, the downloaded file is written into the Bucket store (replacing the previous one).

## Other notes

1. It is critical to consider that Spade Oracle currently gives back `PieceCID v1` together with the `log2 size` of the tree. The rest of the system is built on top of `PieceCID v2`, therefore when interacting with `spade-oracle-store`, the CIDs should be normalized.
2. An admin interface (can be a GH action trigger with form) SHOULD exist to enable inserting manual deals into the DB
