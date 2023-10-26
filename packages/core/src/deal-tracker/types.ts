export type PieceContracts = Map<string, Contract[]>

export interface Contract {
  provider: number
  dealId: number
  expirationEpoch: number
  source: string
}

// Spade Oracle types
export interface DealContract {
  provider_id: number
  legacy_market_id: number
  legacy_market_end_epoch: number
}

export interface DealReplica {
  contracts: DealContract[]
  piece_cid: string
  piece_log2_size: number
}

export interface DealArchive {
  state_epoch: number
  active_replicas: DealReplica[]
}
