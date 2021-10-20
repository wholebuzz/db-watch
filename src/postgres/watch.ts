import { ClientConfig } from 'pg'
import createPostgresSubscriber, { Subscriber } from 'pg-listen'
import { DatabaseListener, UpdatedRow } from '../watch'

export class PostgresListener implements DatabaseListener {
  subscriber: Subscriber | undefined

  constructor(public config: ClientConfig) {}

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

  async listen(channel: string, callback: (payload: UpdatedRow) => void) {
    const subscriber = await this.getSubscriber()
    subscriber.notifications.on(channel, callback)
    await subscriber.listenTo(channel)
  }
}

