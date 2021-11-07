import { DatabaseCopySourceType, DatabaseCopyTargetType, knexPoolConfig } from 'dbcp'
import Knex from 'knex'
import {
  createNotifyRowFieldsFunction,
  createNotifyTrigger,
  dropNotifyFunction,
  dropTrigger,
} from './postgres/sql'

export const testSchemaTableName = 'db_watch_test'
export const testSchemaUrl = './test/schema.sql'

export interface DatabaseTarget {
  targetType: DatabaseCopyTargetType
  targetHost: string
  targetPort: number
  targetUser: string
  targetPassword: string
  targetName: string
  targetTable: string
}

export interface DatabaseConnection extends Record<string, any> {
  database: string
  user: string
  password: string
  host: string
  port: number
}

export const mysqlConnection: DatabaseConnection = {
  database: process.env.MYSQL_DB_NAME ?? '',
  user: process.env.MYSQL_DB_USER ?? '',
  password: process.env.MYSQL_DB_PASS ?? '',
  host: process.env.MYSQL_DB_HOST ?? '',
  port: parseInt(process.env.MYSQL_DB_PORT ?? '', 10),
  charset: 'utf8mb4',
}

export const mysqlSource = {
  sourceType: DatabaseCopySourceType.mysql,
  sourceHost: mysqlConnection.host,
  sourcePort: mysqlConnection.port,
  sourceUser: mysqlConnection.user,
  sourcePassword: mysqlConnection.password,
  sourceName: mysqlConnection.database,
  sourceTable: testSchemaTableName,
}

export const mysqlTarget: DatabaseTarget = {
  targetType: DatabaseCopyTargetType.mysql,
  targetHost: mysqlConnection.host,
  targetPort: mysqlConnection.port,
  targetUser: mysqlConnection.user,
  targetPassword: mysqlConnection.password,
  targetName: mysqlConnection.database,
  targetTable: testSchemaTableName,
}

export const postgresConnection = {
  database: process.env.POSTGRES_DB_NAME ?? '',
  user: process.env.POSTGRES_DB_USER ?? '',
  password: process.env.POSTGRES_DB_PASS ?? '',
  host: process.env.POSTGRES_DB_HOST ?? '',
  port: parseInt(process.env.POSTGRES_DB_PORT ?? '', 10),
}

export const postgresSource = {
  sourceType: DatabaseCopySourceType.postgresql,
  sourceHost: postgresConnection.host,
  sourcePort: postgresConnection.port,
  sourceUser: postgresConnection.user,
  sourcePassword: postgresConnection.password,
  sourceName: postgresConnection.database,
  sourceTable: testSchemaTableName,
}

export const postgresTarget: DatabaseTarget = {
  targetType: DatabaseCopyTargetType.postgresql,
  targetHost: postgresConnection.host,
  targetPort: postgresConnection.port,
  targetUser: postgresConnection.user,
  targetPassword: postgresConnection.password,
  targetName: postgresConnection.database,
  targetTable: testSchemaTableName,
}

export async function createTestDataTable(connection: DatabaseConnection, target: DatabaseTarget) {
  const knex = Knex({
    client: target.targetType,
    connection,
    pool: knexPoolConfig,
  } as any)
  const tableExists = await knex.schema.hasTable(testSchemaTableName)
  if (tableExists) {
    await knex.raw(dropTrigger(testSchemaTableName, 'updated'))
    await knex.raw(dropNotifyFunction(testSchemaTableName, 'updated'))
    await knex.schema.dropTable(testSchemaTableName)
  }
  await knex.schema.createTable(testSchemaTableName, (table) => {
    table.integer('id').primary()
    table.text('guid').notNullable().index()
    table.dateTime('date').defaultTo(knex.fn.now()).index()
    table.text('link').index()
    table.text('feed').index()
    table.text('type')
    table.jsonb('tags')
    table.jsonb('props')
    table.binary('features')
    table.boolean('archiving')
    table.timestamps(false, true)
  })
  // Maximum PostgreSQL NOTIFY payload is 8192 bytes.
  await knex.raw(
    createNotifyRowFieldsFunction(
      testSchemaTableName,
      'updated',
      "'date', orig.date, 'guid', orig.guid, 'props', json_build_object(" +
        "'guid', orig.props->>'guid', 'like', orig.props->>'like', 'repost', orig.props->>'repost')"
    )
  )
  await knex.raw(createNotifyTrigger(testSchemaTableName, 'updated'))
  await knex.destroy()
}
