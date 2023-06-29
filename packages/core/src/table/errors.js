
export const DatabaseOperationErrorName = 'DatabaseOperationFailed'
export class DatabaseOperationError extends Error {
  get name() {
    return DatabaseOperationErrorName
  }
}

export const DatabaseUniqueValueConstraintErrorName = 'DatabaseUniqueValueConstraint'
export class DatabaseUniqueValueConstraintError extends Error {
  get name() {
    return DatabaseUniqueValueConstraintErrorName
  }
}

export const DatabaseValueToUpdateNotFoundErrorName = 'DatabaseValueToUpdateNotFound'
export class DatabaseValueToUpdateNotFoundError extends Error {
  get name() {
    return DatabaseValueToUpdateNotFoundErrorName
  }
}
