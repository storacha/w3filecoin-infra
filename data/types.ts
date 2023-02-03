export type AggregateState = 'INGESTING' | 'READY' | 'DEAL_PENDING' | 'DEAL_PROCESSED'

export interface CarItem {
  link: string
  size: number
  commP: string
  url: string
  md5: string
}

export interface CarItemAggregate {
  link: string
  size: number
}

export interface AggregateTable {
  appendCARs: (aggregateId: string, items: CarItemAggregate[]) => Promise<void>
  getAggregateIngesting: () => Promise<string>
  setAsReady: (aggregateId: string) => Promise<void>
  setAsDealPending: (aggregateId: string) => Promise<void>
  setAsDealProcessed: (aggregateId: string, commP: string) => Promise<void>
}

export interface AggregateOpts {
  endpoint?: string
  maxSize?: number
  minSize?: number
}

export interface CarTable {
  batchWrite: (items: CarItem[]) => Promise<void>
}
