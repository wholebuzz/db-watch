import * as crypto from 'crypto'
import { ReadableStreamTree } from 'tree-stream'

export enum UpdateOp {
  Insert = 'INSERT',
  Update = 'UPDATE',
  Delete = 'DELETE',
}

export interface UpdatedRow {
  op: UpdateOp
  table?: string
  key: Record<string, any>
  row?: Record<string, any>
  updated?: string[]
}

export interface Shard {
  index: number
  modulus: number
}

export interface DatabaseTable {
  table: string
  channel?: string
  keyFields: string[]
  shardField?: string
  versionField: string
}

export interface DatabaseTableParser<Item> {
  parseRow: (row: Record<string, any>) => Item
  parseKey: (key: Record<string, any>) => Partial<Item>
  parseUpdate: (update: UpdatedRow) => Partial<Item>
}

export interface DatabaseLoadOptions {
  shard?: Shard
  shardInverse?: boolean
  version?: Date
}

export class DatabaseLoaderSourceOptions {
  knexOwner?: boolean
}

export abstract class DatabaseLoaderSource {
  abstract close(): Promise<void>
  abstract fetch(table: DatabaseTable, update: UpdatedRow): Promise<Record<string, any>>
  abstract load(table: DatabaseTable, options: DatabaseLoadOptions): ReadableStreamTree
}

export const shardIndex = (text: string, modulus: number) =>
  parseInt(md5(text ?? '').slice(-4), 16) % modulus

export const shardMatchText = (text: string, shard: Shard) =>
  shardIndex(text, shard.modulus) === shard.index

export const md5 = (x: string) => crypto.createHash('md5').update(x).digest('hex')
