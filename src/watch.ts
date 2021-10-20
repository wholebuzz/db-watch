// import pSettle from 'p-settle'

export interface DatabaseListener {
  listen(channel: string, callback: (payload: UpdatedRow) => void): Promise<void>
}

export interface DatabaseSink<Item> {
  keyFields: string[]
  insert: (item: Item) => Item | null
  find: (update: UpdatedRow) => Item | null
  remove: (update: UpdatedRow) => Item | null
  update?: (record: Item, itemRecord: Record<string, any>) => void
  updated?: (current: Item, row: Record<string, any>) => void
  validate?: (row: Record<string, any>) => Item
}

export interface UpdatedRow {
  op: UpdateOp
  table?: string
  key: Record<string, any>
  row?: Record<string, any>
  updated?: string[]
}

export enum UpdateOp {
  Insert = 'INSERT',
  Update = 'UPDATE',
  Delete = 'DELETE',
}

/*
export class DatabaseLoader<X> {
  loading = false
  loaded = false
  updateQueue: UpdatedRow[] = []

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
      if (this.loading) this.updateQeue.push(update)
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

export class DatabaseWatcher<X> extends DatabaseLoader<X> {
  constructor(public listener: DatabaseListener, public sink: DatabaseSink<X>) {}

  async listen(channel: string) {
    this.listener.listen(
           'event_updated',
                   this.delayLoadingWithFetchQueue(() => this.fetchAndUpdateEvents())
                         ),

  async fetchAndUpdate(
    filterFn: (update: UpdatedRow) => boolean,
    parseFn: (update: UpdatedRow) => X,
    fetchFn: (update: UpdatedRow) => Promise<X>,
    updateFn: (update: UpdatedRow) => void,
    concurrency = 2,
    debug = false
  ) {
    if (this.loading || !this.updateQueue.length) return
    this.loading = true
    while (this.updateQueue.length) {
      const updates = this.updateQueue
      if (debug) console.log(`fetchAndUpdate: queue=${updates.length}`)
      this.updateQueue = []
      const ready = await pSettle(
        updates
          .filter(filterFn)
          .map((x) => () => (x.op === 'DELETE' ? Promise.resolve(parseFn(x)) : fetchFn(x))),
        { concurrency }
      )
      ready.forEach((x, i) =>
        updateFn({
          op: updates[i].op,
          key: updates[i].key,
          row: x.isFulfilled ? x.value : undefined,
        })
      )
    }
    this.loading = false
  }
}

export function applyUpdatedRow<Item>(sink: DatabaseSink<Item>, update: UpdatedRow) {
  if (!update.row) return null
  switch (update.op) {
    case UpdateOp.Insert:
      return sink.insert(update.row as Item)

    case UpdateOp.Update:
      const item = sink.find(update.key)
      if (item === undefined) {
        return null
      } else if (isUpdate) {
        const keys = Object.keys(update.row)
        const rekey = keyFields.some((k) => keys.includes(k))
        if (rekey) {
          const item = remove(key)
          return insert({ ...item, ...update.row })
        } else {
          const currentItem = sink.getFn(key)
          const record = currentItem as Record<string, any>
          if (updated) updated(currentItem, update.row)
          sink.update(record, update.row)
						return currentItem
        }
      }
      break

    case UpdateOp.Remove:
      remove(key)
  }
  return true
}

export function handleUpdatedRowItem<Item, Sink>(
  op: string,
  item: X,
  sink: DatabaseSink<Item, Key>,
  filter?: (x: X) => boolean,
) {
  const itemRecord = item as Record<string, any>
  const isUpdate = op === 'UPDATE'
  if (isUpdate || op === 'DELETE') {
    const index = findIndex(item)
    if (index < 0) {
      if (!filter || filter(item)) return null
    } else if (isUpdate) {
      const currentItem = sink.getFn(key)
      const record = currentItem as Record<string, any>
      let rekey = false
      keyFields.forEach(
        (k) => (rekey = rekey || record[k] < itemRecord[k] || itemRecord[k] < record[k])
      )
      if (rekey) {
        remove(index)
        assignUpdate(record, itemRecord)
        insert(currentItem)
      } else {
        if (updated) updated(currentItem, itemRecord)
        assignUpdate(record, itemRecord)
      }
      return currentItem
    } else return remove(index)
  } else if (op === 'INSERT') {
    if (!filter || filter(item)) {
      insert(item)
			return item
    } else {
      return null
    }
  } else {
    return null
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
*/
