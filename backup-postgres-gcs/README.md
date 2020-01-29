# Hasura Postgres Backup Scripts

Specify the env vars to run

```bash
export GOOGLE_CREDENTIALS_JSON=[google credential json]
export GCLOUD_PROJECT_NAME=[gcloud project id]
export GCLOUD_STORAGE_BUCKET=gs://[bucket name]
export GCLOUD_STORAGE_FOLDER=[folder prefix]
export PSQL_URI=
export PSQL_USER=
export PSQL_PASSWORD=
```

## Restore

1. Start a new postgres/patroni
2. Run the db init script (create hasura user and grant permission etc.)
3. Restore by running

```bash
# restore the data
gzip -d -c database_dump.gz | pg_restore --dbname=postgres -h localhost -U postgres -p 5432 --verbose

# restore the hasura schema
gzip -d -c hdb_schema_dump.gz | pg_restore --dbname=postgres -h localhost -U postgres -p 5432 --verbose

# restore the hasura data
gzip -d -c hdb_data_dump.gz | pg_restore --dbname=postgres -h localhost -U postgres -p 5432 --verbose
```