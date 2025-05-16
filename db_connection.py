
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()  # to load variables from .env

def get_connection():
    conn_str = (
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"PORT={os.getenv('DB_PORT')};"
        f"DATABASE={os.getenv('DB_DATABASE')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print("Error connecting to database:", e)
        return None