#!/bin/bash
set -e

export DATE=$(date '+%Y%m%d')
export TIME=$(date '+%H%M%S')

if [ -z "$GOOGLE_CREDENTIALS_JSON" ]; then
  echo "Please specify GOOGLE_CREDENTIALS_JSON"
  exit 1
fi

if [ -z "$GCLOUD_PROJECT_NAME" ]; then
  echo "Please specify the project name"
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

echo $GOOGLE_CREDENTIALS_JSON >credential.json

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

# Do the gcloud auth
gcloud auth activate-service-account --key-file credential.json
gcloud config set project $GCLOUD_PROJECT_NAME

# upload the files to gcloud
gsutil cp database_dump.gz $GCLOUD_STORAGE_BUCKET/$GCLOUD_STORAGE_FOLDER/$DATE/database_dump_$TIME.gz
gsutil cp hdb_schema_dump.gz $GCLOUD_STORAGE_BUCKET/$GCLOUD_STORAGE_FOLDER/$DATE/hdb_schema_dump_$TIME.gz
gsutil cp hdb_data_dump.gz $GCLOUD_STORAGE_BUCKET/$GCLOUD_STORAGE_FOLDER/$DATE/hdb_data_dump_$TIME.gz
