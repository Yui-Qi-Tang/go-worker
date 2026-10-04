// Package durable persists reconstructible jobs and dispatches them through a
// worker Master. Task, Master, Worker, and context objects are never serialized.
//
// An accepted job is a committed database record, independent of whether a Master
// can currently accept it. A job's stable ID identifies repeat enqueue requests.
// Tasks must complete their required work before returning success. Durable
// execution permits duplicate attempts; the caller must provide idempotency or
// business-level deduplication before enabling retries.
package durable
