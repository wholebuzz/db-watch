import { knexPoolConfig } from 'dbcp'
import Knex from 'knex'
import { ArrayCache } from './cache/sorted'
import { parseTestData, parseTestDataUpdate, TestData, testDataSort } from './data.fixture'
import { postgresConnection } from './db.fixture'
import { KnexLoaderSource } from './knex'
import { PostgresTriggerWatcher } from './postgres/watch'
import { UpdateType } from './watch'

export async function newCache() {
  const loaderSource = new KnexLoaderSource(
    Knex({
      client: 'postgresql',
      connection: postgresConnection,
      pool: knexPoolConfig,
    } as any),
    { knexOwner: true }
  )
  const watcherSource = new PostgresTriggerWatcher(postgresConnection, UpdateType.Key)
  const cache = new ArrayCache<TestData>(testDataSort, [])
  await cache.connect(
    {
      table: 'db_watch_test',
      keyFields: ['date', 'guid'],
      versionField: 'updated_at',
    },
    loaderSource,
    watcherSource,
    {
      parseRow: parseTestData,
      parseUpdate: parseTestDataUpdate,
    }
  )
  return { cache, loaderSource, watcherSource }
}
