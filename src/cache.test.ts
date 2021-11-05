import { LocalFileSystem } from '@wholebuzz/fs'
import { DatabaseCopySourceType, DatabaseCopyTargetType, dbcp, knexPoolConfig } from 'dbcp'
import hasha from 'hasha'
import Knex from 'knex'
import rimraf from 'rimraf'
import { Readable } from 'stream'
import { promisify } from 'util'
import { ArrayCache } from './cache/sorted'
import { KnexLoaderSource } from './knex'
import { PostgresTriggerWatcher } from './postgres/watch'
import { UpdateType } from './watch'

const fileSystem = new LocalFileSystem()
const hashOptions = { algorithm: 'md5' }
const rmrf = promisify(rimraf)
const targetNDJsonUrl = '/tmp/target.jsonl.gz'
const testSchemaTableName = 'db_watch_test'
const testSchemaUrl = './test/schema.sql'
const testNDJsonUrl = './test/test.jsonl.gz'
const testNDJsonHash = '9c51a21c2d8a717f3def11864b62378e'

interface TestData {
  id: number
  date: Date
  guid: string
  link: string
  feed: string
  props: Record<string, any>
  tags: Record<string, any>
}

const testDataSort = (a: TestData, b: TestData) => {
  const dateDiff = b.date.getTime() - a.date.getTime()
  if (dateDiff) return dateDiff
  if (a.guid < b.guid) return -1
  else if (b.guid < a.guid) return 1
  else return 0
}

/*
const mysqlConnection = {
  database: process.env.MYSQL_DB_NAME ?? '',
  user: process.env.MYSQL_DB_USER ?? '',
  password: process.env.MYSQL_DB_PASS ?? '',
  host: process.env.MYSQL_DB_HOST ?? '',
  port: parseInt(process.env.MYSQL_DB_PORT ?? '', 10),
  charset: 'utf8mb4',
}
const mysqlSource = {
  sourceType: DatabaseCopySourceType.mysql,
  sourceHost: mysqlConnection.host,
  sourcePort: mysqlConnection.port,
  sourceUser: mysqlConnection.user,
  sourcePassword: mysqlConnection.password,
  sourceName: mysqlConnection.database,
  sourceTable: testSchemaTableName,
}
const mysqlTarget = {
  targetType: DatabaseCopyTargetType.mysql,
  targetHost: mysqlConnection.host,
  targetPort: mysqlConnection.port,
  targetUser: mysqlConnection.user,
  targetPassword: mysqlConnection.password,
  targetName: mysqlConnection.database,
  targetTable: testSchemaTableName,
}
*/

const postgresConnection = {
  database: process.env.POSTGRES_DB_NAME ?? '',
  user: process.env.POSTGRES_DB_USER ?? '',
  password: process.env.POSTGRES_DB_PASS ?? '',
  host: process.env.POSTGRES_DB_HOST ?? '',
  port: parseInt(process.env.POSTGRES_DB_PORT ?? '', 10),
}
const postgresSource = {
  sourceType: DatabaseCopySourceType.postgresql,
  sourceHost: postgresConnection.host,
  sourcePort: postgresConnection.port,
  sourceUser: postgresConnection.user,
  sourcePassword: postgresConnection.password,
  sourceName: postgresConnection.database,
  sourceTable: testSchemaTableName,
}
const postgresTarget = {
  targetType: DatabaseCopyTargetType.postgresql,
  targetHost: postgresConnection.host,
  targetPort: postgresConnection.port,
  targetUser: postgresConnection.user,
  targetPassword: postgresConnection.password,
  targetName: postgresConnection.database,
  targetTable: testSchemaTableName,
}

it('Should hash test data', async () => {
  expect(
    hasha(
      await readableToString((await fileSystem.openReadableFile(testNDJsonUrl)).finish()),
      hashOptions
    )
  ).toBe(testNDJsonHash)
})

it('Should load test data to PostgreSQL', async () => {
  // Load schema
  await dbcp({
    fileSystem,
    ...postgresTarget,
    sourceFile: testSchemaUrl,
  })

  // Copy from testNDJsonUrl to PostgreSQL
  const knex = Knex({
    client: 'postgresql',
    connection: postgresConnection,
    pool: knexPoolConfig,
  } as any)
  expect((await knex.raw(`SELECT COUNT(*) from ${testSchemaTableName};`)).rows[0].count).toBe('0')
  await dbcp({
    fileSystem,
    ...postgresTarget,
    sourceFile: testNDJsonUrl,
  })
  expect((await knex.raw(`SELECT COUNT(*) from ${testSchemaTableName};`)).rows[0].count).toBe(
    '10000'
  )
  await knex.destroy()

  // Dump and verify PostgreSQL
  await rmrf(targetNDJsonUrl)
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(false)
  await dbcp({
    fileSystem,
    ...postgresSource,
    targetFile: targetNDJsonUrl,
    orderBy: 'id ASC',
  })
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(true)
  expect(await hashFile(targetNDJsonUrl)).toBe(testNDJsonHash)
})

it('Should load and dump ArrayCache', async () => {
  const cache = new ArrayCache<TestData>(testDataSort, [])
  expect(cache.data.length).toBe(0)

  const knex = Knex({
    client: 'postgresql',
    connection: postgresConnection,
    pool: knexPoolConfig,
  } as any)
  const loaderSource = new KnexLoaderSource(knex)
  const watcherSource = new PostgresTriggerWatcher(postgresConnection, UpdateType.Key)
  await cache.connect(
    {
      table: 'db_watch_test',
      keyFields: ['date', 'guid'],
      versionField: 'last_updated',
    },
    loaderSource,
    watcherSource
  )
  await cache.watcher?.init()
  expect(cache.data.length).toBe(10000)
  await watcherSource.close()
  await knex.destroy()
})

async function hashFile(path: string) {
  return readableToString(
    (await fileSystem.openReadableFile(path)).pipe(hasha.stream(hashOptions)).finish()
  )
}

function readableToString(stream: Readable): Promise<string> {
  const chunks: Buffer[] = []
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk: any) => chunks.push(Buffer.from(chunk)))
    stream.on('error', (err: Error) => reject(err))
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })
}
