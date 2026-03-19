import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class Database:

    def __init__(self, server: str, database: str, client_id: str, client_secret: str):
        self.server = server
        self.database = database
        self.client_id = client_id
        self.client_secret = client_secret
        self.data_dir = f'./data/lakehouse/{database}'

    
    def __create_sqlalchemy_engine(self):
        """
        Creates a SQLAlchemy engine for a SQL Server database using Active Directory Service Principal authentication.
        """
        connection_string = (
            f"mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={self.client_id};"
            f"PWD={self.client_secret}"
        )
        
        try:
            # Create the SQLAlchemy engine
            engine = create_engine(connection_string)
            return engine
        except SQLAlchemyError as e:
            print("Error while creating the SQLAlchemy engine:", e)
            raise


    def execute_query(self, query: str):

        # Create the SQLAlchemy engine
        engine = self.__create_sqlalchemy_engine()

        try:
            # Use a 'with' block to ensure the connection is closed
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

                current_timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')

                os.makedirs(f'{self.data_dir}/{self.database}', exist_ok=True)
                df.to_csv(f'{self.data_dir}/{self.database}/query_result_{current_timestamp}.csv', index=False, encoding='utf-8-sig')
                list_of_dicts = df.to_dict(orient='records')

                return {'message': 'Success', 'content': {'rows': list_of_dicts}}

        except (SQLAlchemyError, ConnectionError) as e:
            return {'message': 'Error', 'content': str(e)}
