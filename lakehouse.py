import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables (or from somewhere else)
load_dotenv('./utils/.env')

client_id = os.environ.get('CLIENT_ID', '')
client_secret = os.environ.get('CLIENT_SECRET', '')

def connect_to_server(server: str, database: str, client_id: str, client_secret: str) -> pyodbc.Connection:
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={client_id};PWD={client_secret}"
    )
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except pyodbc.Error as e:
        print("Error while connecting to SQL Server:", e)
        raise


if __name__ == '__main__':

    # Parameters
    server = "your_sql_server.database.windows.net"
    database = "your_database_name"

    # Connect to SQL Server
    connection = connect_to_server(server, database, client_id, client_secret)

    # Use the connection
    cursor = connection.cursor()
    cursor.execute("SELECT TOP 10 * FROM your_table_name")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Close the connection
    cursor.close()
    connection.close()