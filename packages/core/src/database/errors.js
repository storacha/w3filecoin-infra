export const DatabaseOperationErrorName = /** @type {const} */('DatabaseOperationFailed')
export class DatabaseOperationError extends Error {
 get name() {
   return DatabaseOperationErrorName
 }
}

export const DatabaseUniqueValueConstraintErrorName = /** @type {const} */('DatabaseUniqueValueConstraint')
export class DatabaseUniqueValueConstraintError extends Error {
  get name() {
    return DatabaseUniqueValueConstraintErrorName
  }
}

export const DatabaseForeignKeyConstraintErrorName = /** @type {const} */('DatabaseForeignKeyConstraint')
export class DatabaseForeignKeyConstraintError extends Error {
  get name() {
    return DatabaseForeignKeyConstraintErrorName
  }
}

export const DatabaseValueToUpdateNotFoundErrorName = /** @type {const} */('DatabaseValueToUpdateNotFound')
export class DatabaseValueToUpdateNotFoundError extends Error {
  get name() {
    return DatabaseValueToUpdateNotFoundErrorName
  }
}

export const DatabaseValueToUpdateAlreadyTakenErrorName = /** @type {const} */('DatabaseValueToUpdateAlreadyTaken')
export class DatabaseValueToUpdateAlreadyTakenError extends Error {
  get name() {
    return DatabaseValueToUpdateAlreadyTakenErrorName
  }
}
