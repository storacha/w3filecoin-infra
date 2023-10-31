import { Result } from '../types'

export interface Queue <A> {
  /**
   * Adds message to the queue.
   */
  add(message: A): Promise<Result<{}, QueuePutError>>
}

// Errors
export interface QueuePutError extends Error {

}

// Connectors
export interface QueueConnect {
  region: string
}

export interface QueueOptions {
  disableMessageGroupId?: boolean
}
