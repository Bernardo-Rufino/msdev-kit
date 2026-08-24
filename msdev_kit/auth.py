from azure.identity import (
    ClientSecretCredential,
    InteractiveBrowserCredential,
    TokenCachePersistenceOptions,
)

_SCOPES = {
    "pbi": "https://analysis.windows.net/powerbi/api/.default",
    "fabric": "https://api.fabric.microsoft.com/.default",
    "azure": "https://management.azure.com/.default",
    "graph": "https://graph.microsoft.com/.default",
}


class Auth:
    def __init__(
        self, tenant_id: str = "", client_id: str = "", client_secret: str = ""
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._credential = (
            ClientSecretCredential(
                authority="https://login.microsoftonline.com/",
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
            if all((tenant_id, client_id, client_secret))
            else InteractiveBrowserCredential(
                cache_persistence_options=TokenCachePersistenceOptions()
            )
        )

    def get_token(self, service: str = "pbi") -> str:
        scope = _SCOPES.get(service)
        if not scope:
            raise ValueError(
                f"Invalid service specified. Choose one of: {', '.join(_SCOPES)}"
            )
        return self._credential.get_token(scope).token

    def get_token_for_user(
        self, service: str = "pbi", allow_unencrypted_storage: bool = False
    ) -> str:
        """Acquire a fresh interactive user token for a supported service.

        Args:
            service (str, optional): Target service name. Defaults to 'pbi'.
            allow_unencrypted_storage (bool, optional): Allow Azure Identity to save
                the token cache without OS encryption. Use only when secure cache
                storage is unavailable. Defaults to False.

        Returns:
            str: OAuth access token for the selected service.

        Raises:
            ValueError: If service is not supported.
        """
        scope = _SCOPES.get(service)
        if not scope:
            raise ValueError(
                f"Invalid service specified. Choose one of: {', '.join(_SCOPES)}"
            )
        credential = InteractiveBrowserCredential(
            cache_persistence_options=TokenCachePersistenceOptions(
                allow_unencrypted_storage=allow_unencrypted_storage
            )
        )
        return credential.get_token(scope).token
