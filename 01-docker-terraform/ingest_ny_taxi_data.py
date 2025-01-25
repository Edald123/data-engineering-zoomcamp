import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = "output.parquet"

    # Download the data
    os.system(f"wget {url} -O {parquet_name}")

    # Load the data
    trips = pq.read_table(parquet_name)
    trips = trips.to_pandas()

    # Get the first 100 trips
    trips_subset = trips.iloc[:100]

    # Create a connection to the database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Get the schema of the table
    print(pd.io.sql.get_schema(trips_subset, name=table_name, con=engine))

    # Create the table in the database, we will not insert any data yet
    trips_subset.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # Batch the data and insert it into the database
    batch_size = 100000
    for start in range(0, len(trips), batch_size):
        t_start = time()
        end = start + batch_size
        batch = trips.iloc[start:end]
        batch.to_sql(name=table_name, con=engine, if_exists="append")
        t_end = time()
        print(
            f"Processed rows {start} to {end - 1}, it took {t_end - t_start:.3f} seconds"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest data from the New York Taxi dataset"
    )

    parser.add_argument("--user", help="User to connect to the database")
    parser.add_argument("--password", help="Password to connect to the database")
    parser.add_argument("--host", help="Host of the database")
    parser.add_argument("--port", help="Port of the database")
    parser.add_argument("--db", help="Name of the database")
    parser.add_argument("--table_name", help="Name of the table to write the data to")
    parser.add_argument("--url", help="URL of the parquet file to ingest")

    args = parser.parse_args()
    main(args)
