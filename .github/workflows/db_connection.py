import psycopg2
from psycopg2 import OperationalError
import os


def create_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None
