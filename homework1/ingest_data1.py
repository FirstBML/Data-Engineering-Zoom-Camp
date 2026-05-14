#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# -------------------------
# CONFIG
# -------------------------
ZONE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"


# -------------------------
# DATABASE ENGINE
# -------------------------
def get_engine(db_url: str):
    return create_engine(db_url)


def build_green_taxi_url(year: int, month: int) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"


def build_db_url(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str) -> str:
    return f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"


# -------------------------
# LOAD DATA
# -------------------------
def load_green_taxi_data(url: str) -> pd.DataFrame:
    return pd.read_parquet(url, engine="fastparquet")


def load_zone_lookup(url: str) -> pd.DataFrame:
    return pd.read_csv(url)


# -------------------------
# INGEST FUNCTIONS
# -------------------------
def ingest_green_taxi(df: pd.DataFrame, engine, table_name: str, chunk_size: int = 100000):
    print("Starting green taxi ingestion...")

    for start in tqdm(range(0, len(df), chunk_size)):
        end = start + chunk_size
        df_chunk = df.iloc[start:end]

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace" if start == 0 else "append",
            index=False
        )

    print("Green taxi ingestion completed")


def ingest_zone_lookup(df: pd.DataFrame, engine, table_name: str):
    print("Starting zone lookup ingestion...")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print("Zone lookup ingestion completed")


# -------------------------
# MAIN PIPELINE
# -------------------------
@click.command()
@click.option("--pg-user", default="firstroot", show_default=True, help="Postgres username")
@click.option("--pg-pass", default="firstroot", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5432, show_default=True, type=int, help="Postgres port")
@click.option("--pg-db", default="first_ny_taxi", show_default=True, help="Postgres database name")
@click.option("--year", required=True, type=int, help="Year of green taxi dataset to ingest")
@click.option("--month", required=True, type=int, help="Month of green taxi dataset to ingest")
@click.option("--chunk-size", default=10000, show_default=True, type=int, help="Number of rows per chunk for ingestion")
@click.option("--green-table", default="green_taxi_data", show_default=True, help="Destination table name for green taxi data")
@click.option("--zone-table", default="taxi_zone_lookup", show_default=True, help="Destination table name for taxi zone lookup data")
def main(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, year: int, month: int, chunk_size: int, green_table: str, zone_table: str):
    db_url = build_db_url(pg_user, pg_pass, pg_host, pg_port, pg_db)
    engine = get_engine(db_url)

    green_taxi_url = build_green_taxi_url(year, month)

    # Load data
    df_green = load_green_taxi_data(green_taxi_url)
    df_zone = load_zone_lookup(ZONE_URL)

    # (Optional debugging schema)
    print(pd.io.sql.get_schema(df_green, name=green_table, con=engine))
    print(pd.io.sql.get_schema(df_zone, name=zone_table, con=engine))

    # Ingest
    ingest_green_taxi(df_green, engine, green_table, chunk_size=chunk_size)
    ingest_zone_lookup(df_zone, engine, zone_table)


# -------------------------
# ENTRY POINT
# -------------------------
if __name__ == "__main__":
    main()