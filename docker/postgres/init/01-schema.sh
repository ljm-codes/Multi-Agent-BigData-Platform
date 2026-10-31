#!/bin/sh
set -e

# 跳过 create database（容器已按 POSTGRES_DB 建库），并丢掉文件末尾未完成的建表语句
sed -e '/^[[:space:]]*create database /Id' \
    -e '/^-- 以下 SQL 均为 未开发/,$d' \
    /sql/postgresql.sql \
    | psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"
