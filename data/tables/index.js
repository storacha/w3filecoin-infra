/** @typedef {import('@serverless-stack/resources').TableProps} TableProps */

/** @type TableProps */
export const carTableProps = {
  fields: {
    link: 'string',         // `bagy...1`
    size: 'number',         // `101`
    url: 'string',        // `https://...`
    commP: 'string',       // `commP...a`
    md5: 'string',       // `md5...a`
    aggregateId: 'string',   // `a50b...1`
    insertedAt: 'string',   // `2023-01-17T...`
  },
  // link
  primaryIndex: { partitionKey: 'link' },
}

/** @type TableProps */
export const aggregateTableProps = {
  fields: {
    aggregateId: 'string',   // `a50b...1`
    size: 'number',         // `101`
    // Note: `state` and `status` are reserved keywords in dynamodb
    stat: 'string',        // 'INGESTING' | 'READY' | 'DEAL_PENDING' | 'DEAL_PROCESSED'
    commP: 'string',       // `bafy1...a`
    insertedAt: 'string',   // `2023-01-17T...`
  },
  // link
  primaryIndex: { partitionKey: 'aggregateId' },
  globalIndexes: {
    indexStat: {
      partitionKey: 'stat',
      sortKey: 'aggregateId',
      projection: 'keys_only'
    }
  }
}
