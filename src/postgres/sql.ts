export const setupQueries = [
  'CREATE EXTENSION hstore',

  `CREATE OR REPLACE FUNCTION jsonb_to_hstore(jsonb)
     RETURNS hstore AS
   $func$
     SELECT COALESCE(hstore(array_agg(key), array_agg(value)), hstore(''))
     FROM jsonb_each_text($1)
   $func$ LANGUAGE sql IMMUTABLE STRICT`,

  `CREATE OR REPLACE FUNCTION shard(text, int)
     RETURNS int AS
   $func$
     SELECT mod(('x' || right(md5($1), 4))::bit(16)::int, $2)
   $func$ LANGUAGE sql IMMUTABLE STRICT`,

  `CREATE OR REPLACE FUNCTION link_from_url(text)
     RETURNS text AS
   $func$
     SELECT SUBSTR($1, STRPOS($1, '://') + 3)
   $func$ LANGUAGE sql IMMUTABLE STRICT`,
]

/**
 * SQL to create a function to notify a channel of row changes.
 * @param table The table whose trigger will call this function.
 * @param operation The operation on table that triggers calls to this function.
 * @optional payload The data to notify with. May refer to [[variables]].
 * @optional preamble Any variable initialization or preprocessing necessary.
 * @optional variables The variables to declare for use in [[preamble]] and [[payload]].
 */
export function createNotifyFunction(
  table: string,
  operation: string,
  payload?: string,
  preamble?: string,
  variables?: string
) {
  return `
    CREATE OR REPLACE FUNCTION notify_${table}_${operation}()
      RETURNS trigger AS $$
      ${variables ? 'DECLARE' : ''}
        ${variables || ''}
      BEGIN
        ${preamble || ''}
        ${payload ? 'PERFORM pg_notify(' : 'NOTIFY '}
          ${payload ? "'" : ''}${table}_${operation}${payload ? "'" : ''}
        ${payload ? ', ' + payload + ')' : ''};
        RETURN NULL;
      END;
    $$ language 'plpgsql';
  `
}

/**
 * SQL to create a function to notify a channel with row deltas.
 * @param table The table whose trigger will call this function.
 * @param operation The operation on table that triggers calls to this function.
 * @optional key Key fields sent with every row delta.
 */
export function createNotifyRowFunction(table: string, operation: string, key = "'id', orig.id") {
  return createNotifyFunction(
    table,
    operation,
    'notification::text',
    `
      IF (TG_OP = 'DELETE') THEN
        orig = OLD;
        data = to_jsonb(OLD);
      ELSIF (TG_OP = 'UPDATE') THEN
        orig = OLD;
        data = hstore_to_jsonb(hstore(NEW) - hstore(OLD));
        rowkey = to_jsonb(NEW);
        notification = to_jsonb(OLD);
        FOR v IN SELECT * FROM jsonb_each(data) LOOP
          IF jsonb_typeof(rowkey->v.key) = 'object'
          THEN
            IF jsonb_typeof(notification->v.key) = 'object'
            THEN
              SELECT jsonb_object_agg(key, value) INTO w FROM jsonb_each(rowkey->v.key)
              WHERE key = any(akeys(jsonb_to_hstore(rowkey->v.key) - jsonb_to_hstore(notification->v.key)));
              data = jsonb_set(data, ('{'||v.key||'}')::text[], w);
            ELSE
              data = jsonb_set(data, ('{'||v.key||'}')::text[], rowkey->v.key);
            END IF;
          END IF;
        END LOOP;
      ELSE
        orig = NEW;
        data = to_jsonb(NEW);
      END IF;
      rowkey = jsonb_build_object(${key});
      notification = jsonb_build_object('table', TG_TABLE_NAME, 'key', rowkey, 'op', TG_OP, 'row', data);
    `,
    `
      rowkey jsonb;
      data jsonb;
      orig record;
      v record;
      w jsonb;
      notification jsonb;
    `
  )
}

/**
 * SQL to create a function to notify a channel with [[fields]].
 * @param table The table whose trigger will call this function.
 * @param operation The operation on table that triggers calls to this function.
 * @optional fields Fields to send with every update.
 */
export function createNotifyRowFieldsFunction(
  table: string,
  operation: string,
  fields = "'id', data.id"
) {
  return createNotifyFunction(
    table,
    operation,
    'notification::text',
    `
      IF (TG_OP = 'DELETE') THEN
        orig = OLD;
        data = OLD;
        updated = ARRAY[]::text[];
      ELSIF (TG_OP = 'UPDATE') THEN
        orig = OLD;
        data = NEW;
        updated = akeys(hstore(NEW) - hstore(OLD));
        key = to_jsonb(NEW);
        notification = to_jsonb(OLD);
        FOREACH v IN ARRAY updated LOOP
          IF jsonb_typeof(key->v) = 'object'
          THEN
            updated = array_remove(updated, v);
            FOR u IN SELECT * FROM akeys(jsonb_to_hstore(key->v) - jsonb_to_hstore(notification->v)) LOOP
              updated = array_append(updated, regexp_replace(v || '.' || u, '[{}]', '', 'g'));
            END LOOP;
          END IF;
        END LOOP;
      ELSE
        orig = NEW;
        data = NEW;
        updated = ARRAY[]::text[];
      END IF;
      notification = jsonb_build_object('table', TG_TABLE_NAME, 'op', TG_OP, 'key', jsonb_build_object(${fields}), 'updated', array_to_json(updated)::jsonb);
    `,
    `
      data record;
      orig record;
      v text;
      u text;
      key jsonb;
      updated text[];
      notification jsonb;
    `
  )
}

/**
 * Drops the function associated with [[operation]] on [[table]].
 * @param table The table associated with the function to drop.
 * @param operation The operation associated with the function to drop.
 */
export function dropNotifyFunction(table: string, operation: string) {
  return `DROP FUNCTION notify_${table}_${operation}`
}

/**
 * Creates a trigger on [[table]] calling the function associated with [[operation]].
 * @param table The table to create a trigger on.
 * @param operation An operation name describing the criteria e.g. 'update'.
 * @optional critiera Invocation criteria for the trigger.
 */
export function createNotifyTrigger(
  table: string,
  operation: string,
  criteria = 'AFTER INSERT OR UPDATE OR DELETE'
) {
  return `
    CREATE TRIGGER ${table}_${operation}
    ${criteria} ON ${table}
    FOR EACH ROW
    EXECUTE PROCEDURE notify_${table}_${operation}();
  `
}

/**
 * Drops the trigger on [[table]] associated with [[operation]].
 * @param table The table to drop the trigger on.
 * @param operation The operation name of the trigger to drop.
 */
export function dropTrigger(table: string, operation: string) {
  return `DROP TRIGGER ${table}_${operation} ON ${table};`
}
