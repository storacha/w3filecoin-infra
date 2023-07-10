export const DatabaseOperationErrorName = /** @type {const} */('DatabaseOperationFailed')
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

export const DatabaseForeignKeyConstraintErrorName = 'DatabaseForeignKeyConstraint'
export class DatabaseForeignKeyConstraintError extends Error {
  get name() {
    return DatabaseForeignKeyConstraintErrorName
  }
}

export const DatabaseValueToUpdateNotFoundErrorName = 'DatabaseValueToUpdateNotFound'
export class DatabaseValueToUpdateNotFoundError extends Error {
  get name() {
    return DatabaseValueToUpdateNotFoundErrorName
  }
}

export const DatabaseValueToUpdateAlreadyTakenErrorName = 'DatabaseValueToUpdateAlreadyTaken'
export class DatabaseValueToUpdateAlreadyTakenError extends Error {
  get name() {
    return DatabaseValueToUpdateAlreadyTakenErrorName
  }
}
