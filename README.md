# db-watch [![image](https://img.shields.io/npm/v/db-watch)](https://www.npmjs.com/package/db-watch) [![test](https://github.com/wholebuzz/db-watch/actions/workflows/test.yaml/badge.svg)](https://github.com/wholebuzz/db-watch/actions/workflows/test.yaml)

Various implementation of database replication, including creating Postgres triggers and monitoring with notify listeners.

## Example

```typescript
// Add to migrations
for (const query of setupQueries) await knex.raw(query)

// After CREATE TABLE users
await knex.raw(createNotifyRowFunction('users', 'updated', "'username', orig.username"))
await knex.raw(createNotifyTrigger('users', 'updated'))

// Maximum PostgreSQL NOTIFY payload is 8192 bytes.
await knex.raw(createNotifyRowFieldsFunction('event', 'updated', "'date', orig.date, 'guid', orig.guid))
await knex.raw(createNotifyTrigger('event', 'updated'))
```
