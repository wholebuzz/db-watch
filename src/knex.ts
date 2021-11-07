import Knex from 'knex'
import StreamTree, { ReadableStreamTree } from 'tree-stream'
import {
  DatabaseLoaderSource,
  DatabaseLoaderSourceOptions,
  DatabaseLoadOptions,
  DatabaseTable,
  UpdatedRow,
} from './load'

export class KnexLoaderSource extends DatabaseLoaderSource {
  constructor(public knex: Knex, public options?: DatabaseLoaderSourceOptions) {
    super()
  }

  async close() {
    if (this.options?.knexOwner) await this.knex.destroy()
  }

  async fetch(table: DatabaseTable, update: UpdatedRow) {
    let query = this.knex(table.table)
    for (const keyField of table.keyFields) {
      query = query.where(keyField, update.key[keyField])
    }
    if (update.updated && update.updated.length > 0) {
      query = query.returning(update.updated.filter((x) => x !== 'archiving'))
    }
    const ret = await query
    return ret[0]
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
