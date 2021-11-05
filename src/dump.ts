import { Transform } from 'stream'
import through2 from 'through2'
import { pumpReadable, ReadableStreamTree } from 'tree-stream'

export const sorted = require('sorted-array-functions')

export function newArraySink<X>(output: X[], transform?: (x: X) => X) {
  return (item: X) => {
    output?.push(transform ? transform(item) : item)
  }
}

export function newSortedArraySink<X>(
  output: X[],
  compare: (a: X, b: X) => number,
  transform?: (x: X) => X,
  mergeLeft?: (existing: X, addItem: X) => void
) {
  return (item: X) => {
    let added = false
    const addItem = transform ? transform(item) : item
    if (mergeLeft) {
      const existing = sorted.eq(output, item, compare)
      added = existing >= 0
      if (added) mergeLeft(output![existing], addItem)
    }
    if (!added) sorted.add(output, addItem, compare)
  }
}

export function dump<X>(stream: ReadableStreamTree, sink: (x: X) => void): Promise<void> {
  return dumpStream(
    stream,
    through2.obj((data: X, _enc, callback) => {
      sink(data)
      callback()
    })
  )
}

export function dumpStream(stream: ReadableStreamTree, output: Transform) {
  stream = stream.pipe(output)
  return pumpReadable(stream, undefined)
}
