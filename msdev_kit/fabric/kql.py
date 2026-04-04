import re
import pandas as pd
from pandas.core.frame import DataFrame
from .utilities import create_directory
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoMultiApiError


class KQLDatabase:

    def __init__(self, kusto_uri: str, database_name: str, client_id: str, client_secret: str, tenant_id: str):
        """
        Initialize variables.
        """
        self.kusto_uri = kusto_uri
        self.database_name = database_name

        # Create a connection string for authentication
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            kusto_uri, client_id, client_secret, tenant_id
        )

        # Create a Kusto client
        self.client = KustoClient(kcsb)
        self.data_dir = './data/monitoring'

        create_directory(self.data_dir)


    def query_kql_database(self, kql_query: str, sort_by: str = None) -> DataFrame:
        """
        Connects to a Kusto (KQL) database and executes a query.

        Args:
            kql_query (str): The KQL query to execute.

        Returns:
            pandas.DataFrame: A DataFrame containing the query results, or None if an error occurs.
        """
        try:
            # Execute the query
            response = self.client.execute(self.database_name, kql_query)
            print(response)

            # Convert the response to a pandas DataFrame
            last_parameters_string = kql_query.rsplit('| project', maxsplit=1)[1].strip()
            project_string = last_parameters_string.split('|', maxsplit=1)[0].strip().split(',')
            columns = [re.sub(r'\s+', '', s) for s in project_string]
            df = pd.DataFrame(response.primary_results[0], columns=columns, dtype=str)
            if sort_by:
                df.sort_values(by=sort_by, inplace=True, ascending=False)

            return df

        except KustoServiceError as error:
            if 'E_QUERY_RESULT_SET_TOO_LARGE' in str(error):
                print('ERROR: Query set too large, try adding some more filters!')
            else:
                print(f"An error occurred: {error}")
            return None
        except KustoMultiApiError as error:
            print(f"An error occurred: {error}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None