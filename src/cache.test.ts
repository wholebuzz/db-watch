import { LocalFileSystem } from '@wholebuzz/fs'
import fs from 'fs'
import hasha from 'hasha'
import Knex from 'knex'
import rimraf from 'rimraf'
import { Readable } from 'stream'
import { promisify } from 'util'
import {
  DatabaseCopySourceType,
  DatabaseCopyTargetType,
  dbcp,
  knexPoolConfig,
} from 'dbcp'

const zlib = require('zlib')

const fileSystem = new LocalFileSystem()
const hashOptions = { algorithm: 'md5' }
const rmrf = promisify(rimraf)
const targetNDJsonUrl = '/tmp/target.jsonl.gz'
const testSchemaTableName = 'db_watch_test'
const testSchemaUrl = './test/schema.sql'
const testNDJsonUrl = './test/test.jsonl.gz'
const testNDJsonHash = '9c51a21c2d8a717f3def11864b62378e'

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

it('Should hash test data as string', async () => {
  expect(
    hasha(
      await readableToString(fs.createReadStream(testNDJsonUrl).pipe(zlib.createGunzip())),
      hashOptions
    )
  ).toBe(testNDJsonHash)
  expect(
    hasha(
      await readableToString((await fileSystem.openReadableFile(testNDJsonUrl)).finish()),
      hashOptions
    )
  ).toBe(testNDJsonHash)
})


it('Should restore to and dump from Postgres to ND-JSON', async () => {
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
