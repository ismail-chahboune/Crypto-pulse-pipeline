# Runs automatically on first Postgres container start (via
# docker-entrypoint-initdb.d). Creates a separate database/user for the
# analytics warehouse so it stays isolated from Airflow's own metadata DB,
# while still only requiring a single Postgres container.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER crypto_user WITH PASSWORD 'crypto_pass';
    CREATE DATABASE crypto_warehouse OWNER crypto_user;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "crypto_warehouse" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION crypto_user;
    CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION crypto_user;
    CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION crypto_user;
EOSQL
