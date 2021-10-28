import byline from 'byline'
import {
  defaultSplitterOptions,
  mssqlSplitterOptions,
  mysqlSplitterOptions,
  postgreSplitterOptions,
  sqliteSplitterOptions,
} from 'dbgate-query-splitter/lib/options'
import { SplitQueryStream } from 'dbgate-query-splitter/lib/splitQueryStream'
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

export function streamToKnexRaw(
  source: {
    knex?: Knex
    transaction?: Knex.Transaction
  },
  options?: { returning?: boolean }
) {
  let stream = StreamTree.writable(
    through2.obj(function (data: string, _: string, callback: () => void) {
      const text = data.replace(/\?/g, '\\?')
      const query = source.transaction ? source.transaction.raw(text) : source.knex!.raw(text)
      query
        .then((result) => {
          if (options?.returning) this.push(result)
          callback()
        })
        .catch((err) => {
          throw err
        })
    })
  )
  stream = stream.pipeFrom(
    newStatementSplitterStream(
      ((source.transaction || source.knex) as any).context.client.config.client
    )
  )
  stream = stream.pipeFrom(byline.createStream())
  return stream
}

export function newStatementSplitterStream(type?: any) {
  switch (type) {
    case 'postgresql':
      return new SplitQueryStream(postgreSplitterOptions)
    case 'mysql':
      return new SplitQueryStream(mysqlSplitterOptions)
    case 'mssql':
      return new SplitQueryStream(mssqlSplitterOptions)
    case 'sqlite':
      return new SplitQueryStream(sqliteSplitterOptions)
    default:
      return new SplitQueryStream(defaultSplitterOptions)
  }
}
