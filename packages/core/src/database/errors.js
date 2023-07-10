export const DatabaseOperationErrorName = /** @type {const} */('DatabaseOperationFailed')
export class DatabaseOperationError extends Error {
 get name() {
   return DatabaseOperationErrorName
 }
}

export const DatabaseForeignKeyConstraintErrorName = /** @type {const} */('DatabaseForeignKeyConstraint')
export class DatabaseForeignKeyConstraintError extends Error {
  get name() {
    return DatabaseForeignKeyConstraintErrorName
  }
}

export const DatabaseValueToUpdateAlreadyTakenErrorName = /** @type {const} */('DatabaseValueToUpdateAlreadyTaken')
export class DatabaseValueToUpdateAlreadyTakenError extends Error {
  get name() {
    return DatabaseValueToUpdateAlreadyTakenErrorName
  }
}
