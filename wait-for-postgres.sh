#!/bin/bash
# wait-for-postgres.sh

# what is this command doing?
set -e

host="$1"

# Note that we had to install the postgres-client in the docker image
until PGPASSWORD="test" psql -h "$host" -U "postgres" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"

