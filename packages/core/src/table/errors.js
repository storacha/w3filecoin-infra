
export class DatabaseOperationError extends Error {
  get name() {
    return /** @type {const} */ ('DatabaseOperationFailed')
  }
}

export class DatabaseUniqueValueConstraintError extends Error {
  get name() {
    return /** @type {const} */ ('DatabaseUniqueValueConstraint')
  }
}

export class DatabaseValueToUpdateNotFoundError extends Error {
  get name() {
    return /** @type {const} */ ('DatabaseValueToUpdateNotFound')
  }
}
