FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app

COPY ingest_ny_taxi_data.py ingest_ny_taxi_data.py

ENTRYPOINT [ "python", "ingest_ny_taxi_data.py" ]