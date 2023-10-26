
export type OracleContracts = Map<string, Contract[]>

export interface Contract {
  provider: number
  dealId: number
  expirationEpoch: number
  source: string
}

// Spade types

export interface SpadeContract {
  provider_id: number
  legacy_market_id: number
  legacy_market_end_epoch: number
}

export interface SpadeReplica {
  contracts: SpadeContract[]
  piece_cid: string
  piece_log2_size: number
}

export interface SpadeOracle {
  state_epoch: number
  active_replicas: SpadeReplica[]
}
