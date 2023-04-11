import requests
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
        self.SharePointPrincipal = '00000003-0000-0ff1-ce00-000000000000'


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
    

    def get_token_for_sharepoint(self, host_name) -> str:
        """Creates authenticated SharePoint context via certificate credentials

        """
        """
        Generates the bearer token to be used on SharePoint REST API requests.

        Creates authenticated SharePoint context via certificate credentials

        Args:

            host_name (str): host name in the format 'example.sharepoint.com', like 'contoso.sharepoint.com'.

        Returns:
            str: token for authorization.
        """
        resource = f'{self.SharePointPrincipal}/{host_name}@{self.tenant_id}'
        principal_id = f'{self.client_id}@{self.tenant_id}'
        url = f'https://accounts.accesscontrol.windows.net/{self.tenant_id}/tokens/OAuth/2'

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'grant_type': 'client_credentials',
            'client_id': principal_id,
            'client_secret': self.client_secret,
            'scope': resource,
            'resource': resource
        }
        
        response = requests.post(url=url, headers=headers, data=data)
        response.raise_for_status()

        access_token = response.json().get('access_token', '')

        return access_token