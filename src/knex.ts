import Knex from 'knex'
import StreamTree, { ReadableStreamTree } from 'tree-stream'
import { DatabaseLoaderSource, DatabaseLoadOptions, DatabaseTable } from './load'

export class KnexLoaderSource extends DatabaseLoaderSource {
  constructor(public knex: Knex) {
    super()
  }

  load(table: DatabaseTable, options: DatabaseLoadOptions): ReadableStreamTree {
    let query = this.knex(table.table)
    if (options?.shard) {
      query = query.where(
        this.knex.raw(`shard(${table.shardField || 'id'}, ${options.shard.modulus})`),
        options.shardInverse ? '!=' : '=',
        options.shard.index
      )
    }
    if (options?.version) {
      query = query.where([table.versionField || 'updated_at', '>', options.version])
    }
    return StreamTree.readable(query.stream())
  }
}
