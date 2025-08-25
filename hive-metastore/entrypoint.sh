#!/bin/bash
set -e

# echo ">>> Checking Hive schema in Postgres"
# if schematool -info -dbType postgres \
#     -userName appuser -passWord 1234 \
#     -url jdbc:postgresql://postgres:5432/metastore | grep -q "Schema version:"; then
#   echo ">>> Hive schema already initialized, skipping"
# else
#   echo ">>> Initializing Hive schema in Postgres"
#   schematool -initSchema -dbType postgres \
#     -userName appuser -passWord 1234 \
#     -url jdbc:postgresql://postgres:5432/metastore
# fi

echo ">>> Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
