import Knex from 'knex'
import StreamTree, { ReadableStreamTree } from 'tree-stream'
import { StreamRowsOptions } from './dump'

export function streamKnexRows(
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
