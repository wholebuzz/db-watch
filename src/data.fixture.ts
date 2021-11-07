import { writeJSONLines } from '@wholebuzz/fs/lib/json'
import { LocalFileSystem } from '@wholebuzz/fs/lib/local'
import { dbcp } from 'dbcp'
import hasha from 'hasha'
import rimraf from 'rimraf'
import { Readable } from 'stream'
import { promisify } from 'util'
import { createTestDataTable, DatabaseConnection, DatabaseTarget } from './db.fixture'
import { UpdatedRow } from './load'

export const targetNDJsonUrl = '/tmp/target.jsonl.gz'
export const testNDJsonUrl = './test/test.jsonl.gz'
export const testNDJsonHash = '9c51a21c2d8a717f3def11864b62378e'

export const fileSystem = new LocalFileSystem()
export const hashOptions = { algorithm: 'md5' }
export const rmrf = promisify(rimraf)

export interface TestData {
  id: number
  date: Date
  guid: string
  link: string
  feed: string
  props: Record<string, any>
  tags: Record<string, any>
}

export const cloneTestData = (x: TestData): TestData => ({
  id: x.id,
  date: x.date,
  guid: x.guid,
  link: x.link,
  feed: x.feed,
  props: x.props,
  tags: x.tags,
})

export const parseTestData = (row: Record<string, any>): TestData => ({
  ...row,
  guid: row.guid,
  date: new Date(row.date),
  link: row.link,
  feed: row.feed,
  id: row.id,
  props: {
    ...row.props,
  },
  tags: {
    ...row.tags,
  },
})

export const parseTestDataUpdate = (update: UpdatedRow): TestData => ({
  ...update.row,
  guid: update.key.guid,
  date: new Date(update.key.date),
  link: update.row?.link,
  feed: update.row?.feed,
  id: update.row?.id,
  props: {
    ...update.row?.props,
    guid: update.key.props.guid,
    like: update.key.props.like,
    repost: update.key.props.repost,
  },
  tags: {
    ...update.row?.tags,
  },
})

export const testDataIdSort = (a: TestData, b: TestData) => a.id - b.id

export const testDataSort = (a: TestData, b: TestData) => {
  const dateDiff = b.date.getTime() - a.date.getTime()
  if (dateDiff) return dateDiff
  if (a.guid < b.guid) return -1
  else if (b.guid < a.guid) return 1
  else return 0
}

export async function verifyTestData(data: TestData[]) {
  await rmrf(targetNDJsonUrl)
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(false)
  await writeJSONLines(fileSystem, targetNDJsonUrl, data.map(cloneTestData).sort(testDataIdSort))
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(true)
  expect(await hashFile(targetNDJsonUrl)).toBe(testNDJsonHash)
}

export async function loadTestData(
  connection: DatabaseConnection,
  target: DatabaseTarget,
  options?: { beforeN?: number; afterN?: number }
) {
  if (!options?.afterN) await createTestDataTable(connection, target)
  await dbcp({
    fileSystem,
    ...target,
    sourceFile: testNDJsonUrl,
    transformJson: options?.beforeN
      ? beforeNTransform(options.beforeN)
      : options?.afterN
      ? afterNTransform(options.afterN)
      : undefined,
  })
}

export function beforeNTransform(n: number) {
  let count = 0
  return (x: unknown) => {
    count++
    return count <= n ? x : null
  }
}

export function afterNTransform(n: number) {
  let count = 0
  return (x: unknown) => {
    count++
    return count > n ? x : null
  }
}

export async function hashFile(path: string) {
  return readableToString(
    (await fileSystem.openReadableFile(path)).pipe(hasha.stream(hashOptions)).finish()
  )
}

export function readableToString(stream: Readable): Promise<string> {
  const chunks: Buffer[] = []
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk: any) => chunks.push(Buffer.from(chunk)))
    stream.on('error', (err: Error) => reject(err))
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })
}
