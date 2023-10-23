# w3filecoin

> The w3filecoin pipeline services.

## Background

[web3.storage](http://web3.storage) is a Storefront providing APIs to enable users to easily upload CAR files, while getting them available on the IPFS Network and stored in multiple locations via Filecoin Storage Providers. It relies on Spade as a broker to get their user data into Filecoin Storage Providers. Currently, Spade requires a Filecoin Piece with size between 16GiB and 32GiB to create deals with Filecoin Storage Providers. Moreover, the closer a Filecoin Piece is closer to the upper bound, the most optimal are the associated storage costs.

Taking into account that [web3.storage](http://web3.storage) onboards any type of content (up to a maximum of 4GiB-padded shards to have better utilization of Fil sector space), multiple CAR files uploaded need to be aggregated into a bigger Piece that can be offered to Filecoin Storage Providers. w3filecoin pipeline keeps track of queued CARs (cargo) to be included in Storage Provider deals.

After CAR file is added to web3.storage's bucket, its piece is computed and sent into the w3filecoin processing pipeline. This pipeline is composed of multiple processing queues that accumulate pieces into aggregates and submit them into a Filecoin deal queue.

## Services

As specified in [w3-filecoin SPEC](https://github.com/web3-storage/specs/blob/main/w3-filecoin.md), this pipeline is decoupled into 4 roles:
- Storefront
- Aggregator
- Dealer
- Deal Tracker

While Storefront is a storage facilitator like w3up API, the remaining 3 represent services that make user uploads land into filecoin deals. Therefore, [`Aggregator`](./aggregator-architecture.md), `Dealer` and [`Deal Tracker`](./deal-tracker-architecture.md) are implemented by the `w3filecoin` pipeline.

## Infrastructure

![Pipeline infra](./w3filecoin.svg)