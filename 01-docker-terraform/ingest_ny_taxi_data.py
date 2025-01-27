import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import argparse
import os


def look_up_table(engine):
    table_name = "zones"
    csv_file = "output.csv"
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Download the data
    os.system(f"wget {url} -O {csv_file}")

    # Load the data
    zones = pd.read_csv(csv_file)
    zones.to_sql(name=table_name, con=engine, if_exists="replace")

    # Remove the csv file
    os.system(f"rm {csv_file}")


def load_data(file_path):
    if file_path.endswith('.parquet'):
        print("Detected Parquet file. Loading...")
        data = pq.read_table(file_path).to_pandas()
    elif file_path.endswith('.csv'):
        print("Detected CSV file. Loading...")
        data = pd.read_csv(file_path)
    elif file_path.endswith('.csv.gz'):
        print("Detected Gzipped CSV file. Loading...")
        data = pd.read_csv(file_path, compression='gzip')
    else:
        raise ValueError("Unsupported file format. Please provide a .parquet, .csv, or .csv.gz file.")

    return data


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Determine the file name based on the URL
    file_name = url.split('/')[-1]

    # Download the data
    os.system(f"wget {url} -O {file_name}")

    # Load the data dynamically based on the file type
    trips = load_data(file_name)

    # Get the first 100 trips
    trips_subset = trips.iloc[:100]

    # Create a connection to the database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Create the lookup table
    look_up_table(engine)

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

    # Remove the downloaded file
    os.system(f"rm {file_name}")

    print("Data ingestion complete")


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
    parser.add_argument("--url", help="URL of the data file to ingest (CSV or Parquet)")

    args = parser.parse_args()
    main(args)
