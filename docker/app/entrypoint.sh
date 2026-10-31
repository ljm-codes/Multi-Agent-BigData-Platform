#!/bin/sh
set -e

mkdir -p /root/.m2/repository /app/tmp_code /app/log /app/cache /app/tmp
exec "$@"
