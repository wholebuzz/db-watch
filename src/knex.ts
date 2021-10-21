import Knex from 'knex'
import through2 from 'through2'
import StreamTree, { ReadableStreamTree } from 'tree-stream'
import { StreamRowsOptions } from './dump'
import { InsertRowsOptions } from './restore'

const batch2 = require('batch2')

export function streamFromKnex(
  knex: Knex,
  query: Knex.QueryBuilder,
  options?: StreamRowsOptions
): ReadableStreamTree {
  if (options?.shard) {
    query = query.where(
      knex.raw(`shard(${options.shardKey || 'id'}, ${options.shard.modulus})`),
      options.shardInverse ? '!=' : '=',
      options.shard.index
    )
  }
  if (options?.version) {
    query = query.where([options?.versionKey || 'updated_at', '>', options.version])
  }
  return StreamTree.readable(query.stream())
}

export function streamToKnex(
  source: {
    knex?: Knex
    transaction?: Knex.Transaction
  },
  options: InsertRowsOptions
) {
  const stream = StreamTree.writable(
    through2.obj(function (data: any[], _: string, callback: () => void) {
      let query = source.transaction
        ? source.transaction.batchInsert(options.table, data)
        : source.knex!.batchInsert(options.table, data)
      if (options.returning) query = query.returning(options.returning)
      if (source.transaction) query = query.transacting(source.transaction)
      query
        .then((result) => {
          if (options.returning) this.push(result)
          callback()
        })
        .catch((err) => {
          throw err
        })
    })
  )
  return stream.pipeFrom(batch2.obj({ size: options.batchSize ?? 4000 }))
}
