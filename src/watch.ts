import pSettle from 'p-settle'
import { dump } from './dump'
import {
  DatabaseLoaderSource,
  DatabaseTable,
  DatabaseTableParser,
  Shard,
  shardMatchText,
  UpdatedRow,
  UpdateOp,
} from './load'

export enum WatchMethod {
  Log,
  Poll,
  Trigger,
}

export enum AntiShardBehavior {
  None,
  Interleaved,
  ShardFirst,
  ShardLast,
}

export enum UpdateType {
  Delta,
  Full,
  Key,
}

export interface IdempotentDatabaseSink<Item> extends DatabaseTableParser<Item> {
  load: (item: Item) => void
  upsert: (key: Partial<Item>, value: Partial<Item> | null) => Item | null
}

export interface MemoryDatabaseSink<Item> extends Partial<DatabaseTableParser<Item>> {
  insert: (item: Item) => Item
  find: (key: Partial<Item>) => Item | null
  remove: (key: Partial<Item>) => Item | null
  update?: (current: Item, update: Partial<Item>) => void
  updated?: (current: Item | null, update: Partial<Item> | null) => void
}

export interface DatabaseWatcherSink<Item> extends IdempotentDatabaseSink<Item> {
  filterUpdate?: (item: UpdatedRow) => boolean
}

export abstract class DatabaseWatcherSource {
  constructor(public watchMethod: WatchMethod, public updateType: UpdateType) {}
  abstract watch(table: string, callback: (payload: UpdatedRow) => void): Promise<void>
}

export interface DatabaseWatcherOptions {
  antiShardBehavior?: AntiShardBehavior
  concurrency: number
  debug?: boolean
  shard?: Shard
  shardField?: string
  version?: Date
}

export class DatabaseLoader<Item> {
  updateQueue: UpdatedRow[] = []
  loading = false
  loaded = false
  version?: Date

  constructor(
    public source: DatabaseLoaderSource,
    public table: DatabaseTable,
    public sink: IdempotentDatabaseSink<Item>
  ) {}

  startLoading() {
    this.loading = true
  }

  doneLoading() {
    const updates = this.updateQueue
    this.updateQueue = []
    this.loading = false
    this.loaded = true
    return updates
  }

  delayLoading(updateFn: (update: UpdatedRow) => void) {
    return (update: UpdatedRow) => {
      if (this.loading) this.updateQueue.push(update)
      else updateFn(update)
    }
  }

  delayLoadingWithFetchQueue(dequeue: () => void) {
    return (update: UpdatedRow) => {
      this.updateQueue.push(update)
      dequeue()
    }
  }
}

export class DatabaseWatcher<Item> extends DatabaseLoader<Item> {
  constructor(
    public source: DatabaseLoaderSource,
    public watcher: DatabaseWatcherSource,
    public table: DatabaseTable,
    public sink: DatabaseWatcherSink<Item>,
    public options: DatabaseWatcherOptions
  ) {
    super(source, table, sink)
  }

  async watch() {
    await this.watcher.watch(
      this.table.table,
      this.watcher.updateType === UpdateType.Key
        ? this.delayLoadingWithFetchQueue(() => this.fetchAndUpdate())
        : this.delayLoading((x) => this.handleUpdatedRow(x))
    )
  }

  async init() {
    // const start = Date.now()
    this.startLoading()
    await this.watch()
    await this.initialLoad()
  }

  async initialLoad() {
    await dump(
      this.source.load(this.table, {
        shard: this.options.shard,
        version: this.options.version,
      }),
      this.sink.load
    )
    for (const delayedUpdate of this.doneLoading()) this.handleUpdatedRow(delayedUpdate)
  }

  async fetchAndUpdate() {
    if (this.loading || !this.updateQueue.length) return
    this.loading = true
    while (this.updateQueue.length) {
      const updates = this.updateQueue
      if (this.options.debug) console.log(`fetchAndUpdate: queue=${updates.length}`)
      this.updateQueue = []
      const ready = await pSettle(
        updates
          .filter(this.sink?.filterUpdate ?? ((x) => x))
          .map(
            (update) => () =>
              update.op === UpdateOp.Delete
                ? Promise.resolve(this.sink.parseUpdate(update))
                : this.source.fetch(this.table, update)
          ),
        { concurrency: this.options.concurrency }
      )
      ready.forEach((x, i) =>
        this.handleUpdatedRow({
          op: updates[i].op,
          key: updates[i].key,
          row: x.isFulfilled ? x.value : undefined,
        })
      )
    }
    this.loading = false
  }

  handleUpdatedRow(update: UpdatedRow) {
    if (!update.row) return null
    if (
      this.options.shard &&
      !shardMatchText(update.key[this.table.shardField ?? ''] ?? '', this.options.shard)
    ) {
      return null
    }
    if (this.sink.filterUpdate && !this.sink.filterUpdate(update)) return null
    const key = this.sink.parseRow(update.key)
    const row = this.sink.parseUpdate(update)

    switch (update.op) {
      case UpdateOp.Insert:
      case UpdateOp.Update:
        return this.sink.upsert(key, row)

      case UpdateOp.Delete:
        return this.sink.upsert(key, null)
    }
  }
}

export function idempotentSinkFromMemorySink<Item>(
  table: DatabaseTable,
  sink: MemoryDatabaseSink<Item>,
  watcherOptions?: Partial<DatabaseWatcherSink<Item>>
): DatabaseWatcherSink<Item> {
  const update = sink.update ?? assignProps
  return {
    ...watcherOptions,
    upsert: (key: Partial<Item>, row: Partial<Item> | null) => {
      const item = sink.find(key)
      if (sink.updated) sink.updated(item, row)
      if (!row) return sink.remove(key)
      if (item) {
        const rowkeys = Object.keys(row)
        const rekey = rowkeys.some((k) => table.keyFields.includes(k))
        if (rekey) {
          const removedItem = sink.remove(key)
          return sink.insert({ ...(removedItem ?? item), ...row })
        } else {
          update(item, row)
          return item
        }
      } else {
        return sink.insert(row as Item)
      }
    },
    load: sink.insert,
    parseKey: watcherOptions?.parseKey ?? sink.parseKey ?? ((x) => x as Partial<Item>),
    parseRow: watcherOptions?.parseRow ?? sink.parseRow ?? ((x) => x as Item),
    parseUpdate: (x: UpdatedRow) => x.row as Item,
  }
}

export function assignProps(record: Record<string, any>, itemRecord: Record<string, any>) {
  Object.keys(itemRecord).forEach((k) => {
    const propk = itemRecord[k]
    if (propk instanceof Object) {
      const outk = record[k] ?? (record[k] = {})
      Object.keys(propk).forEach((l) => (outk[l] = propk[l]))
    } else {
      record[k] = propk
    }
  })
}
