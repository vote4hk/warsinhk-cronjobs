#!/bin/bash
set -e

export DATE=$(date '+%Y%m%d')
export TIME=$(date '+%H%M%S')

if [ -z "$AZURE_STORAGE_ACCOUNT" ]; then
  echo "Please specify AZURE_STORAGE_ACCOUNT"
  exit 1
fi

if [ -z "$AZURE_STORAGE_KEY" ]; then
  echo "Please specify AZURE_STORAGE_KEY"
  exit 1
fi

if [ -z "$AZURE_CONTAINER_NAME" ]; then
  echo "Please specify AZURE_CONTAINER_NAME"
  exit 1
fi

if [ -z "$AZURE_CONTAINER_KEY_PREFIX" ]; then
  echo "Please specify AZURE_CONTAINER_KEY_PREFIX"
  exit 1
fi

if [ -z "$PSQL_URI" ]; then
  echo "Please specify PSQL_URI"
  exit 1
fi

if [ -z "$PSQL_USER" ] || [ -z "$PSQL_PASSWORD" ]; then
  echo "Please specify a username or password"
  exit 1
fi

export PGPASSWORD="$PSQL_PASSWORD"

# -F c -> custom file format
# -b blob data
# -v verbose
# -T ignore tables
# -c clean
# -n specify schema
# -s schema only
# -a data only
pg_dump -h $PSQL_URI -p 5432 -U postgres -F c -b -v -d postgres -T postgres_log* -T failed_authentication_* -T pg_stat_statements -n public | gzip -9 >database_dump.gz
pg_dump -h $PSQL_URI -p 5432 -U postgres -F c -b -v -s -d postgres -c -n hdb_catalog | gzip -9 >hdb_schema_dump.gz
pg_dump -h $PSQL_URI -p 5432 -U postgres -F c -b -v -a -d postgres -n hdb_catalog -T hdb_catalog.event_invocation_logs -T hdb_catalog.event_log | gzip -9 >hdb_data_dump.gz

# upload the files to azure storage container
az storage blob upload --container-name $AZURE_CONTAINER_NAME --file database_dump.gz   --name $AZURE_CONTAINER_KEY_PREFIX/$DATE/database_dump_$TIME.gz
az storage blob upload --container-name $AZURE_CONTAINER_NAME --file hdb_schema_dump.gz --name $AZURE_CONTAINER_KEY_PREFIX/$DATE/hdb_schema_dump_$TIME.gz
az storage blob upload --container-name $AZURE_CONTAINER_NAME --file hdb_data_dump.gz   --name $AZURE_CONTAINER_KEY_PREFIX/$DATE/hdb_data_dump_$TIME.gz
