from azure.identity import ClientSecretCredential, InteractiveBrowserCredential, TokenCachePersistenceOptions

class Auth:

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize variables.

        Args:
            tenant_id (str, optional): tenant ID.
            client_id (str, optional): client ID (app registration).
            client_secret (str, optional): client secret/credentials (app registration).
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self, service: str = 'pbi') -> str:
        """
        Generates the bearer token to be used on Power BI or Fabric REST API requests.

        Authenticates using a service principal (app account).

        Args:
            service (str, optional): which service to get token to: pbi or fabric.

        Returns:
            str: token for authorization.
        """
        if service == 'pbi':
            scope = 'https://analysis.windows.net/powerbi/api/.default'
        elif service == 'fabric':
            scope = 'https://api.fabric.microsoft.com/.default'
        else:
            raise ValueError("Invalid service specified. Choose 'pbi' or 'fabric'.")

        auth = ClientSecretCredential(
                    authority = 'https://login.microsoftonline.com/',
                    tenant_id = self.tenant_id,
                    client_id = self.client_id,
                    client_secret = self.client_secret)

        response = auth.get_token(scope)
        access_token = response.token

        return access_token

    def get_token_for_user(self, service: str = 'pbi') -> str:
        """
        Generates the bearer token to be used on Microsoft REST API requests.

        Authenticates interactively (user account).

        Args:
            service (str, optional): which service to get token to: pbi, fabric or azure.

        Returns:
            str: token for authorization.
        """
        if service == 'pbi':
            scope = 'https://analysis.windows.net/powerbi/api/.default'
        elif service == 'fabric':
            scope = 'https://api.fabric.microsoft.com/.default'
        elif service == 'azure':
            scope = 'https://management.azure.com/.default'
        else:
            raise ValueError("Invalid service specified. Choose 'pbi', 'fabric' or'azure'.")

        auth = InteractiveBrowserCredential(cache_persistence_options=TokenCachePersistenceOptions())

        response = auth.get_token(scope)
        access_token = response.token

        return access_token

