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
        self.scope = 'https://analysis.windows.net/powerbi/api/.default'
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret


    def get_token(self) -> str:
        """
        Generates the bearer token to be used on Power BI REST API requests.

        Authenticates using a service principal (app account).

        Returns:
            str: token for authorization.
        """

        auth = ClientSecretCredential(
                    authority = 'https://login.microsoftonline.com/',
                    tenant_id = self.tenant_id,
                    client_id = self.client_id,
                    client_secret = self.client_secret)

        response = auth.get_token(self.scope)
        access_token = response.token

        return access_token


    def get_token_for_user(self) -> str:
        """
        Generates the bearer token to be used on Power BI REST API requests.

        Authenticates interactively (user account).

        Returns:
            str: token for authorization.
        """

        auth = InteractiveBrowserCredential(cache_persistence_options=TokenCachePersistenceOptions())

        response = auth.get_token(self.scope)
        access_token = response.token

        return access_token
