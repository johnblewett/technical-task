import json

import boto3
import pandas as pd
import sqlalchemy


DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "lcfcstatsbomb"
DB_HOST = "statsbomb-db.cjiagommwe01.eu-north-1.rds.amazonaws.com"
DB_PORT = "5432"


def prepare_data():
    """Prepare data to be uploaded to RDS"""

    s3_client = boto3.client("s3")

    def get_new_files(s3_client):
        """Get a list of newly added files to S3"""

        print("Getting new files")
        response = s3_client.get_object(Bucket="statsbomb-raw-data", Key="update_log.txt")
        data = response["Body"].read().decode("utf-8")
        return data.split(", ")

    def extract_data(file):
        """Retrieve the data from a file"""

        response = s3_client.get_object(Bucket="statsbomb-raw-data", Key=file)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data

    def filter_func(data, types):
        """Returns a subset of the data"""
        return [record for record in data if record["type"]["id"] in types]

    files = get_new_files(s3_client)

    data = []
    for file in files:
        data = data + extract_data(file)

    # Removed freeze frames because they're a nightmare to deal with - would probably store them as a json within their column
    shots = filter_func(data, [16])
    shots = pd.json_normalize(shots)
    shots.drop("shot.freeze_frame", axis=1, inplace=True)

    passes = filter_func(data, [30])
    passes = pd.json_normalize(passes)

    tackles = filter_func(data, [3, 4, 33])  # Dispossessed, Duel, 50/50
    tackles = pd.json_normalize(tackles)
    tackles.columns = [col.replace("50_50", "fifty_fifty") for col in tackles.columns]

    fouls = filter_func(data, [21, 22])  # Foul Committed, Foul Won
    fouls = pd.json_normalize(fouls)

    print("Created shots, passes, tackles and fouls tables\n")
    return shots, passes, tackles, fouls


db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_rds_connection(db_url):
    """Creates an SQLAlchemy connection to an Amazon RDS PostgreSQL database"""

    try:
        engine = sqlalchemy.create_engine(db_url, pool_size=10, max_overflow=20)
        connection = engine.connect()
        print("Connected to PostgreSQL sucessfully\n")
        return engine, connection

    except Exception as e:
        print(f"Error connecting to RDS: {e}")
        return None


def push_dataframe_to_rds(df, table_name, engine, connection):
    """Pushes a Pandas DataFrame to an Amazon RDS PostgreSQL database"""

    dtype_mapping = {"object": "text", "int64": "integer", "float64": "real", "bool": "boolean"}

    cols = [col.replace(".", "_") for col in df.columns]
    df.columns = cols

    column_types = []
    for column, dtype in df.dtypes.items():
        sql_type = dtype_mapping[str(dtype)]
        column_types.append(f"{column} {sql_type}")
    column_types = ", ".join(column_types)

    try:

        # Create the necessary table if it doesn't already exist
        print(f"Checking if {table_name} already exists")
        create_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({column_types})"""
        connection.execute(sqlalchemy.text(create_query))
        connection.commit()

        # Add columns to the RDS PostgreSQL database if they don't already exist
        print("Checking all necessary columns are present in RDS")
        inspector = sqlalchemy.inspect(engine)
        existing_columns = [col["name"] for col in inspector.get_columns(table_name)]

        for column, column_type in df.dtypes.items():
            if column not in existing_columns:
                sql_type = dtype_mapping[str(column_type)]
                alter_query = f"""ALTER TABLE {table_name} ADD COLUMN {column} {sql_type}"""
                connection.execute(sqlalchemy.text(alter_query))
                connection.commit()

        # Add columns to DataFrame if they exist in RDS
        print("Checking all necessary columns are present in DataFrame")
        df = df.reindex(df.columns.union(existing_columns, sort=False), axis=1, fill_value=None)

        # Deletes any IDs which are already in the RDS database
        print("Deleting rows where ID is in update table")
        ids = df["id"].to_list()
        ids = ", ".join([f"'{id}'" for id in ids])
        delete_query = f"""DELETE FROM {table_name} WHERE id IN ({ids})"""
        connection.execute(sqlalchemy.text(delete_query))
        connection.commit()

        # Push Pandas DataFrame to RDS
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Data successfully pushed to table {table_name}\n")

    except Exception as e:
        print(f"RDS update failed: {e}")


shots, passes, tackles, fouls = prepare_data()
engine, connection = get_rds_connection(db_url)

if connection:
    if not shots.empty:
        push_dataframe_to_rds(shots, "shots", engine, connection)
    if not passes.empty:
        push_dataframe_to_rds(passes, "passes", engine, connection)
    if not tackles.empty:
        push_dataframe_to_rds(tackles, "tackles", engine, connection)
    if not fouls.empty:
        push_dataframe_to_rds(fouls, "fouls", engine, connection)
