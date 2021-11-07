import { dbcp } from 'dbcp'
import { newCache } from './cache.fixture'
import {
  cloneTestData,
  fileSystem,
  hashFile,
  loadTestData,
  rmrf,
  targetNDJsonUrl,
  TestData,
  testNDJsonHash,
  verifyTestData,
} from './data.fixture'
import { postgresConnection, postgresSource, postgresTarget } from './db.fixture'

it('Should load test data to PostgreSQL', async () => {
  // Load test data
  await loadTestData(postgresConnection, postgresTarget)

  // Dump and verify PostgreSQL
  await rmrf(targetNDJsonUrl)
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(false)
  await dbcp({
    fileSystem,
    ...postgresSource,
    targetFile: targetNDJsonUrl,
    orderBy: 'id ASC',
    transformJson: (x) => cloneTestData(x as TestData),
  })
  expect(await fileSystem.fileExists(targetNDJsonUrl)).toBe(true)
  expect(await hashFile(targetNDJsonUrl)).toBe(testNDJsonHash)
})

it('Should load and watch ArrayCache', async () => {
  // Load test data
  await loadTestData(postgresConnection, postgresTarget, { beforeN: 5000 })

  // Load half test data
  const { cache, loaderSource, watcherSource } = await newCache()
  expect(cache.data.length).toBe(0)
  await cache.watcher?.init()
  expect(cache.data.length).toBe(5000)

  // Watch second half load
  await loadTestData(postgresConnection, postgresTarget, { afterN: 5000 })
  await waitForValue(() => cache.data.length, 10000)
  expect(cache.data.length).toBe(10000)
  await watcherSource.close()
  await loaderSource.close()

  // Dump and verify Cache
  await verifyTestData(cache.data)
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function waitForValue<X>(getValue: () => X, waitFor: X) {
  for (let i = 0; i < 30; i++) {
    if (getValue() === waitFor) return
    await sleep(1000)
  }
}
