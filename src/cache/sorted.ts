import { DatabaseLoaderSource, DatabaseTable } from '../load'
import {
  DatabaseWatcher,
  DatabaseWatcherSource,
  idempotentSinkFromMemorySink,
  MemoryDatabaseSink,
} from '../watch'

export const sorted = require('sorted-array-functions')

export class ArrayCache<Item> implements MemoryDatabaseSink<Item> {
  watcher?: DatabaseWatcher<Item>

  constructor(public compare: (a: Item, b: Item) => number, public data: Item[] = []) {}

  async connect(
    table: DatabaseTable,
    source: DatabaseLoaderSource,
    watcherSource: DatabaseWatcherSource
  ) {
    this.watcher = new DatabaseWatcher<Item>(
      source,
      watcherSource,
      table,
      idempotentSinkFromMemorySink(table, {
        find: (x) => this.find(x),
        insert: (x) => this.insert(x),
        remove: (x) => this.remove(x),
      }),
      { concurrency: 1 }
    )
  }

  find(key: Record<string, any>): Item | null {
    const index = this.findIndex(key)
    return index >= 0 ? this.data[index] : null
  }

  findIndex(key: Record<string, any>): number {
    return sorted.eq(this.data, key, this.compare)
  }

  greaterIndex(key: Record<string, any>): number {
    return Math.max(0, sorted.gt(this.data, key, this.compare))
  }

  lesserIndex(key: Record<string, any>): number {
    return sorted.lt(this.data, key, this.compare)
  }

  insert(addItem: Item) {
    return sorted.add(this.data, addItem, this.compare)
  }

  remove(key: Partial<Item>) {
    return sorted.remove(key) ? (key as Item) : null
  }
}
