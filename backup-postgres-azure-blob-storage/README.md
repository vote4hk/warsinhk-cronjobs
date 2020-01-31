# Hasura Postgres Backup Scripts

Specify the env vars to run

```bash
# AZURE_STORAGE_KEY=$(az storage account keys list -n $AZURE_STORAGE_ACCOUNT | jq -rM '.[0].value')
export AZURE_STORAGE_ACCOUNT=[storage account name]
export AZURE_STORAGE_KEY=[key to access storage account]
export AZURE_CONTAINER_NAME=[bucket name]
export AZURE_CONTAINER_KEY_PREFIX=[object prefix]
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
