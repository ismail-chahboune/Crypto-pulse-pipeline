"""
Crypto Market Pipeline
=======================

Daily pipeline that:
  1. Creates the raw landing table if it doesn't exist.
  2. Pulls a market-data snapshot for the top N coins from the CoinGecko
     public API (no key required) and loads it into the warehouse.
  3. Runs the dbt project to build staging views and analytical marts
     (daily summaries, moving averages, rolling volatility).
  4. Runs dbt tests to validate the transformed data.

This DAG is intentionally built with plain PostgresHook + BashOperator
calls rather than a third-party dbt provider, to keep the dependency
footprint small and the logic easy to read end-to-end.
"""
from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append("/opt/airflow/include/scripts")
from fetch_market_data import fetch_top_coins_snapshot  # noqa: E402

DBT_PROJECT_DIR = "/opt/airflow/dbt/crypto_analytics"
POSTGRES_CONN_ID = "crypto_warehouse"

CREATE_RAW_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.crypto_price_snapshots (
        id SERIAL PRIMARY KEY,
        coin_id VARCHAR(100) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        name VARCHAR(100) NOT NULL,
        current_price NUMERIC,
        market_cap NUMERIC,
        market_cap_rank INTEGER,
        total_volume NUMERIC,
        price_change_pct_24h NUMERIC,
        snapshot_date DATE NOT NULL,
        fetched_at TIMESTAMP NOT NULL,
        UNIQUE (coin_id, snapshot_date)
    );
"""

UPSERT_SQL = """
    INSERT INTO raw.crypto_price_snapshots
        (coin_id, symbol, name, current_price, market_cap, market_cap_rank,
         total_volume, price_change_pct_24h, snapshot_date, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coin_id, snapshot_date) DO UPDATE SET
        current_price = EXCLUDED.current_price,
        market_cap = EXCLUDED.market_cap,
        market_cap_rank = EXCLUDED.market_cap_rank,
        total_volume = EXCLUDED.total_volume,
        price_change_pct_24h = EXCLUDED.price_change_pct_24h,
        fetched_at = EXCLUDED.fetched_at;
"""


@dag(
    dag_id="crypto_market_pipeline",
    description="Daily ingestion + dbt transformation of top-coin market data",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["crypto", "dbt", "portfolio"],
)
def crypto_market_pipeline():

    @task
    def create_raw_table() -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(CREATE_RAW_TABLE_SQL)

    @task
    def extract_and_load(top_n: int = 25) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        records = fetch_top_coins_snapshot(top_n=top_n)
        for record in records:
            hook.run(UPSERT_SQL, parameters=record)
        return len(records)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    create_raw_table() >> extract_and_load() >> dbt_run >> dbt_test


crypto_market_pipeline()
