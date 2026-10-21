import hashlib
import os
from pathlib import Path
import tempfile

from azure.identity import (
    AuthenticationRecord,
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
        self._user_cache_dir = Path.home() / ".msdev-kit" / "auth"
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
        self,
        service: str = "pbi",
        allow_unencrypted_storage: bool = False,
        *,
        account: str | None = None,
    ) -> str:
        """Acquire a user token, optionally reusing a tenant-scoped account record.

        Args:
            service (str, optional): Target service name. Defaults to 'pbi'.
            allow_unencrypted_storage (bool, optional): Allow Azure Identity to save
                the token cache without OS encryption. Use only when secure cache
                storage is unavailable. Defaults to False.
            account (str | None, optional): The exact username to cache and reuse.
                Requires tenant_id. If omitted, retains fresh interactive behavior.

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
        if account is None:
            options = {
                "cache_persistence_options": TokenCachePersistenceOptions(
                    allow_unencrypted_storage=allow_unencrypted_storage
                )
            }
            if self.tenant_id:
                options["tenant_id"] = self.tenant_id
            credential = InteractiveBrowserCredential(**options)
            return credential.get_token(scope).token

        if not isinstance(account, str) or not account.strip():
            raise ValueError("account must be a non-empty username")
        if not self.tenant_id:
            raise ValueError("tenant_id is required for a cached user session")

        account = account.strip()
        cache_key = hashlib.sha256(
            f"{self.tenant_id.casefold()}\0{account.casefold()}".encode("utf-8")
        ).hexdigest()
        if self._user_cache_dir.is_symlink():
            raise ValueError(
                "user authentication cache directory must not be a symlink"
            )
        record_path = self._user_cache_dir / f"{cache_key}.json"
        cache_options = TokenCachePersistenceOptions(
            name=f"msdev-kit-user-{cache_key}",
            allow_unencrypted_storage=allow_unencrypted_storage,
        )
        options = {
            "cache_persistence_options": cache_options,
            "tenant_id": self.tenant_id,
        }

        if record_path.exists():
            if record_path.is_symlink():
                raise ValueError("cached authentication record must not be a symlink")
            record = AuthenticationRecord.deserialize(
                record_path.read_text(encoding="utf-8")
            )
            if record.username.casefold() != account.casefold():
                raise ValueError(
                    "cached authentication record belongs to another account"
                )
            options["authentication_record"] = record
        else:
            options["login_hint"] = account

        credential = InteractiveBrowserCredential(**options)
        if "authentication_record" not in options:
            record = credential.authenticate(scopes=[scope])
            if record.username.casefold() != account.casefold():
                raise ValueError(
                    "authenticated account does not match requested account"
                )
            self._write_user_record(record_path, record)
        return credential.get_token(scope).token

    def _write_user_record(
        self, record_path: Path, record: AuthenticationRecord
    ) -> None:
        cache_dir = record_path.parent
        cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if cache_dir.is_symlink():
            raise ValueError(
                "user authentication cache directory must not be a symlink"
            )
        os.chmod(cache_dir, 0o700)
        descriptor, temporary_path = tempfile.mkstemp(dir=cache_dir)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                file.write(record.serialize())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, record_path)
        finally:
            Path(temporary_path).unlink(missing_ok=True)
