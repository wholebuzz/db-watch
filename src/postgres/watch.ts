import { ClientConfig } from 'pg'
import createPostgresSubscriber, { Subscriber } from 'pg-listen'
import { UpdatedRow } from '../load'
import { DatabaseWatcherSource, UpdateType, WatchMethod } from '../watch'

export class PostgresTriggerWatcher extends DatabaseWatcherSource {
  subscriber: Subscriber | undefined

  constructor(public config: ClientConfig, updateType: UpdateType) {
    super(WatchMethod.Trigger, updateType)
  }

  async close() {
    if (!this.subscriber) return
    await this.subscriber.close()
    this.subscriber = undefined
  }

  async watch(table: string, callback: (payload: UpdatedRow) => void) {
    const channel = `${table}_updated`
    const subscriber = await this.getSubscriber()
    subscriber.notifications.on(channel, callback)
    await subscriber.listenTo(channel)
  }

  async getSubscriber() {
    if (!this.subscriber) {
      this.subscriber = createPostgresSubscriber(this.config)
      this.subscriber.events.on('error', (error) => {
        throw error
      })
      await this.subscriber.connect()
    }
    return this.subscriber
  }
}