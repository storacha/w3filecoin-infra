/** @typedef {import('@serverless-stack/resources').TableProps} TableProps */

/** @type TableProps */
export const carTableProps = {
  fields: {
    link: 'string',         // `bagy...1`
    size: 'number',         // `101`
    url: 'string',        // `https://...`
    commP: 'string',       // `commP...a`
    insertedAt: 'string',   // `2023-01-17T...`
  },
  // link
  primaryIndex: { partitionKey: 'link' },
}

/** @type TableProps */
export const ferryTableProps = {
  fields: {
    id: 'string',   // `1675425764468`
    size: 'number',         // `101`
    // Note: `state` and `status` are reserved keywords in dynamodb
    stat: 'string',        // 'LOADING' | 'READY' | 'DEAL_PENDING' | 'DEAL_PROCESSED'
    commP: 'string',       // `commP...a`
    insertedAt: 'string',   // `2023-01-17T...`
  },
  // link
  primaryIndex: { partitionKey: 'id' },
  globalIndexes: {
    indexStat: {
      partitionKey: 'stat',
      sortKey: 'id',
      projection: 'keys_only'
    }
  }
}

/** @type TableProps */
export const cargoTableProps = {
  fields: {
    ferryId: 'string',   // `1675425764468`
    link: 'string',         // `bagy...1`
    insertedAt: 'string',   // `2023-01-17T...`
  },
  // link
  primaryIndex: { partitionKey: 'ferryId', sortKey: 'link' },
  globalIndexes: {
    indexLink: {
      partitionKey: 'link',
      sortKey: 'ferryId',
      projection: 'keys_only'
    }
  }
}
